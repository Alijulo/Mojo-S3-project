// store/cluster.rs
use super::{ObjectStore, PutOptions, GetOptions, GetResult};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::Body;
use std::{
    collections::HashSet,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::{mpsc, RwLock}, task::JoinHandle};
use tracing::{info, warn};
use anyhow::Result as AnyhowResult;  // Alias to avoid confusion

// ──────────────────────────────────────────────────────
// Node state with health + latency EMA
// ──────────────────────────────────────────────────────
#[derive(Clone, Debug)]
struct Node {
    id: String,
    url: String,
    weight: u16,
    healthy: bool,
    last_check: Instant,
    failures: u32,
    latency_ms_ema: f64, // exponential moving average (lower is better)
}

impl Node {
    fn new(id: String, url: String, weight: u16) -> Self {
        Self {
            id,
            url,
            weight,
            healthy: true,
            last_check: Instant::now(),
            failures: 0,
            latency_ms_ema: 5000.0, // initialize conservatively
        }
    }

    fn update_latency(&mut self, rtt_ms: u64, alpha: f64) {
        let sample = rtt_ms as f64;
        self.latency_ms_ema = alpha * sample + (1.0 - alpha) * self.latency_ms_ema;
    }
}

// ──────────────────────────────────────────────────────
// Consistent hashing ring (virtual nodes + latency-biased)
// ──────────────────────────────────────────────────────
#[derive(Clone)]
struct HashRing {
    vnodes: Vec<(u64, usize)>, // (hash, node_index)
    nodes: Vec<Node>,
}

impl HashRing {
    fn new() -> Self {
        Self { vnodes: vec![], nodes: vec![] }
    }

    fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }

    fn rebuild(&mut self) {
        // Sort healthy nodes by latency EMA (fastest first) to bias placement order
        self.nodes.sort_by(|a, b| a.latency_ms_ema.partial_cmp(&b.latency_ms_ema).unwrap_or(std::cmp::Ordering::Equal));

        self.vnodes.clear();
        for (idx, node) in self.nodes.iter().enumerate() {
            if !node.healthy { continue; }
            for v in 0..node.weight {
                let token = format!("{}#{}", node.id, v);
                self.vnodes.push((hash64(&token), idx));
            }
        }
        self.vnodes.sort_by_key(|(h, _)| *h);
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty() || self.vnodes.is_empty()
    }

    fn replicas_for(&self, key: &str, n: usize) -> Vec<Node> {
        if self.is_empty() { return vec![]; }
        let h = hash64(key);
        let start = match self.vnodes.binary_search_by_key(&h, |(hh, _)| *hh) {
            Ok(p) => p,
            Err(p) => p,
        };

        let mut out = Vec::with_capacity(n);
        let mut seen: HashSet<usize> = HashSet::new();
        let mut i = 0;

        while out.len() < n && i < self.vnodes.len() * 2 {
            let (_, idx) = self.vnodes[(start + i) % self.vnodes.len()];
            if seen.insert(idx) {
                out.push(self.nodes[idx].clone());
            }
            i += 1;
            if seen.len() >= self.nodes.len() { break; }
        }

        // Final latency sort to prefer fastest within the chosen set
        out.sort_by(|a, b| a.latency_ms_ema.partial_cmp(&b.latency_ms_ema).unwrap_or(std::cmp::Ordering::Equal));
        out
    }
}

fn hash64(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

// ──────────────────────────────────────────────────────
#[derive(Clone)]
pub struct ClusterStore {
    ring: Arc<RwLock<HashRing>>,
    client: reqwest::Client,

    replication: usize,    // e.g., 3
    write_quorum: usize,   // e.g., 2
    read_quorum: usize,    // e.g., 1 or 2

    timeout: Duration,     // per request timeout
    health_interval: Duration,
    failure_threshold: u32,
    ema_alpha: f64,        // smoothing factor for latency EMA
}

impl ClusterStore {
    pub async fn new(node_urls: Vec<String>) -> Result<Self> {
        if node_urls.is_empty() {
            return Err(anyhow!("ClusterStore requires at least one node"));
        }

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Duration::from_secs(30))
            .build()?;

        let mut ring = HashRing::new();
        for (i, url) in node_urls.into_iter().enumerate() {
            // Weight 16 virtual nodes per real node (smooth distribution)
            ring.add_node(Node::new(format!("n{}", i), url, 16));
        }
        ring.rebuild();

        let store = Self {
            ring: Arc::new(RwLock::new(ring)),
            client,
            replication: 3,
            write_quorum: 2,
            read_quorum: 1,
            timeout: Duration::from_secs(30),
            health_interval: Duration::from_secs(10),
            failure_threshold: 3,
            ema_alpha: 0.2, // 20% new sample, 80% history
        };

        store.spawn_health_checker();
        Ok(store)
    }

    fn key_token(bucket: &str, key: &str) -> String {
        format!("{bucket}/{key}")
    }

    fn node_put_url(&self, node: &Node, bucket: &str, key: &str) -> String {
        format!("{}/api/v1/object/{bucket}/{key}", node.url)
    }

    fn node_get_url(&self, node: &Node, bucket: &str, key: &str) -> String {
        format!("{}/api/v1/object/{bucket}/{key}", node.url)
    }

    fn node_list_url(&self, node: &Node, bucket: &str, prefix: &str) -> String {
        format!("{}/api/v1/bucket/{bucket}?prefix={prefix}", node.url)
    }

    fn node_health_url(&self, node: &Node) -> String {
        format!("{}/health", node.url)
    }

    fn spawn_health_checker(&self) {
        let ring = self.ring.clone();
        let client = self.client.clone();
        let interval = self.health_interval;
        let threshold = self.failure_threshold;
        let alpha = self.ema_alpha;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let mut ring_guard = ring.write().await;
                for node in ring_guard.nodes.iter_mut() {
                    let url = format!("{}/health", node.url);
                    let start = Instant::now();
                    let resp = client.get(&url)
                        .timeout(Duration::from_secs(3))
                        .send()
                        .await;

                    match resp {
                        Ok(r) if r.status().is_success() => {
                            let rtt = start.elapsed().as_millis() as u64;
                            node.healthy = true;
                            node.failures = 0;
                            node.last_check = Instant::now();
                            node.update_latency(rtt, alpha);
                        }
                        _ => {
                            node.failures += 1;
                            node.last_check = Instant::now();
                            if node.failures >= threshold {
                                if node.healthy {
                                    warn!("Node {} marked unhealthy", node.id);
                                }
                                node.healthy = false;
                                node.latency_ms_ema = f64::INFINITY;
                            }
                        }
                    }
                }

                ring_guard.rebuild();
            }
        });
    }
}

#[async_trait]
impl ObjectStore for ClusterStore {
    // ──────────────────────────────────────────────────
    // Streaming tee PUT with quorum and backpressure
    // ──────────────────────────────────────────────────
    async fn put_stream(
        &self,
        bucket: &str,
        key: &str,
        _opts: PutOptions,
        mut body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    ) -> Result<()> {
        let token = Self::key_token(bucket, key);
        let targets = {
            let ring = self.ring.read().await;
            ring.replicas_for(&token, self.replication)
        };
        if targets.is_empty() {
            return Err(anyhow!("No healthy replicas available"));
        }

        // One channel per replica (bounded for backpressure)
        let mut senders = Vec::<mpsc::Sender<Option<Bytes>>>::with_capacity(targets.len());
        let mut receivers = Vec::<mpsc::Receiver<Option<Bytes>>>::with_capacity(targets.len());
        for _ in 0..targets.len() {
            let (tx, rx) = mpsc::channel::<Option<Bytes>>(16);
            senders.push(tx);
            receivers.push(rx);
        }

        // Spawn per-replica streaming tasks
        let mut tasks: Vec<JoinHandle<Result<reqwest::Response>>> = Vec::with_capacity(targets.len());
        for (i, node) in targets.iter().enumerate() {
            let mut rx = receivers.remove(0);
            let url = self.node_put_url(node, bucket, key);
            let client = self.client.clone();
            let timeout = self.timeout;

            // let stream = async_stream::stream! {
            //     while let Some(item) = rx.recv().await {
            //         match item {
            //             Some(bytes) => yield bytes,
            //             None => break,
            //         }
            //     }
            // };



            let stream = async_stream::stream! {
                while let Some(item) = rx.recv().await {
                    match item {
                        Some(bytes) => yield Ok(bytes) as AnyhowResult<Bytes>,  // Explicit cast for clarity
                        None => break,
                    }
                }
            };
            let body = Body::wrap_stream(stream);

            tasks.push(tokio::spawn(async move {
                let resp = client
                    .put(&url)
                    .body(body)
                    .timeout(timeout)
                    .send()
                    .await;
                match resp {
                    Ok(r) => Ok(r),
                    Err(e) => Err(anyhow!(e)),
                }
            }));
        }

        // Tee input to all replicas (bounded send to enforce backpressure)
        while let Some(chunk) = body.next().await {
            let bytes = chunk?;
            for tx in &senders {
                if let Err(_e) = tx.send(Some(bytes.clone())).await {
                    // Replica channel closed; continue feeding others
                    warn!("Replica channel closed during PUT; continuing");
                }
            }
        }
        // Signal EOF
        for tx in &senders {
            let _ = tx.send(None).await;
        }

        // Quorum: wait for enough successes
        let mut successes = 0usize;
        let mut errors = Vec::new();
        for t in tasks {
            match t.await {
                Ok(Ok(resp)) if resp.status().is_success() => {
                    successes += 1;
                    if successes >= self.write_quorum {
                        break;
                    }
                }
                Ok(Ok(resp)) => errors.push(anyhow!("PUT status {}", resp.status())),
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(anyhow!("join error: {}", e)),
            }
        }

        if successes < self.write_quorum {
            return Err(anyhow!(
                "Write quorum not met: {successes}/{}; errors: {errors:?}",
                self.write_quorum
            ));
        }

        info!("Cluster PUT {bucket}/{key} quorum={successes}/{}", self.write_quorum);
        Ok(())
    }

    // ──────────────────────────────────────────────────
    // GET with latency-biased placement and quorum
    // ──────────────────────────────────────────────────
    async fn get_stream(
        &self,
        bucket: &str,
        key: &str,
        _opts: GetOptions,
    ) -> Result<GetResult> {
        let token = Self::key_token(bucket, key);
        let targets = {
            let ring = self.ring.read().await;
            ring.replicas_for(&token, self.replication)
        };
        if targets.is_empty() {
            return Err(anyhow!("No healthy replicas available"));
        }

        let mut successes = 0usize;
        let mut first_ok: Option<reqwest::Response> = None;

        for node in targets {
            let url = self.node_get_url(&node, bucket, key);
            match self.client.get(&url).timeout(self.timeout).send().await {
                Ok(resp) if resp.status().is_success() => {
                    successes += 1;
                    if first_ok.is_none() {
                        first_ok = Some(resp);
                    }
                    if successes >= self.read_quorum {
                        break;
                    }
                }
                Ok(resp) => warn!("GET {} -> {}", url, resp.status()),
                Err(e) => warn!("GET {} error: {}", url, e),
            }
        }

        let mut resp = first_ok.ok_or_else(|| anyhow!("Read quorum not met"))?;

        let content_length = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("\"unknown\"")
            .to_string();

        let stream = async_stream::try_stream! {
            while let Some(chunk) = resp.chunk().await? {
                yield chunk;
            }
        };

        Ok(GetResult {
            content_length,
            content_type,
            etag,
            version_id: None,
            body: Box::pin(stream),
        })
    }

    // ──────────────────────────────────────────────────
    // LIST via first healthy replica in placement
    // ──────────────────────────────────────────────────
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let token = Self::key_token(bucket, prefix);
        let targets = {
            let ring = self.ring.read().await;
            ring.replicas_for(&token, 1)
        };
        if targets.is_empty() {
            return Ok(vec![]);
        }
        let node = &targets[0];
        let url = self.node_list_url(node, bucket, prefix);

        let resp = self.client.get(&url).timeout(self.timeout).send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(anyhow!("LIST {} -> {}", url, status));
        }
        let keys: Vec<String> = resp.json().await?;
        Ok(keys)
    }

    // ──────────────────────────────────────────────────
    // DELETE with quorum across replicas
    // ──────────────────────────────────────────────────
    async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let token = Self::key_token(bucket, key);
        let targets = {
            let ring = self.ring.read().await;
            ring.replicas_for(&token, self.replication)
        };
        if targets.is_empty() {
            return Err(anyhow!("No healthy replicas available"));
        }

        let mut successes = 0usize;
        let mut errors = Vec::new();
        for node in targets {
            let url = self.node_put_url(&node, bucket, key);
            match self.client.delete(&url).timeout(self.timeout).send().await {
                Ok(r) if r.status().is_success() => {
                    successes += 1;
                    if successes >= self.write_quorum {
                        break;
                    }
                }
                Ok(r) => errors.push(anyhow!("DELETE {} -> {}", url, r.status())),
                Err(e) => errors.push(anyhow!("DELETE {} error: {}", url, e)),
            }
        }

        if successes < self.write_quorum {
            return Err(anyhow!("Delete quorum not met: {successes}/{}", self.write_quorum));
        }
        Ok(())
    }

    // ──────────────────────────────────────────────────
    // EXISTS: true if any replica reports success
    // ──────────────────────────────────────────────────
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let token = Self::key_token(bucket, key);
        let targets = {
            let ring = self.ring.read().await;
            ring.replicas_for(&token, self.replication)
        };
        for node in targets {
            let url = self.node_get_url(&node, bucket, key);
            if let Ok(resp) = self.client.head(&url).timeout(self.timeout).send().await {
                if resp.status().is_success() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
