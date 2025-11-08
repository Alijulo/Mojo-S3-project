import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import {
  listBuckets,
  createBucket as apiCreateBucket,
  deleteBucket as apiDeleteBucket,
  listObjects,
  getBucketMetadata,
  BucketMetadata,
  S3Bucket,
} from "../api";
import { Eye, Trash2, ArrowUp, ArrowDown } from "lucide-react";

interface BucketWithStats extends S3Bucket {
  objectCount: number;
  totalSize: number;
  permission: string;
  metadata: BucketMetadata | null;
  selected: boolean;
}

/* ────────────────────── Helpers ────────────────────── */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(1)} GB`;
}
function getPermissionLabel(perm: string): string {
  switch (perm) {
    case "ReadWrite": return "Read + Write";
    case "ReadOnly": return "Read Only";
    default: return "No Access";
  }
}
function getPermissionColor(perm: string): string {
  switch (perm) {
    case "ReadWrite": return "bg-green-100 text-green-800";
    case "ReadOnly": return "bg-blue-100 text-blue-800";
    default: return "bg-red-100 text-red-800";
  }
}
function extractXmlErrorMessage(xmlText: string): string {
  try {
    const parser = new DOMParser();
    const xml = parser.parseFromString(xmlText, "application/xml");
    return xml.querySelector("Message")?.textContent?.trim() || "Unknown error";
  } catch {
    return "Unknown error";
  }
}

/* ────────────────────── Component ────────────────────── */
export default function Buckets() {
  const [buckets, setBuckets] = useState<BucketWithStats[]>([]);
  const [filteredBuckets, setFilteredBuckets] = useState<BucketWithStats[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [newBucketName, setNewBucketName] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [selectAll, setSelectAll] = useState(false);

  /* ───── Load all data ───── */
  const loadBuckets = async () => {
    setError("");
    try {
      const loaded = await listBuckets();

      const withStats = await Promise.all(
        loaded.map(async (b) => {
          let objectCount = 0;
          let totalSize = 0;
          let metadata: BucketMetadata | null = null;

          try {
            const objects = await listObjects(b.Name);
            objectCount = objects.length;
            totalSize = objects.reduce((s, o) => s + (o.Size || 0), 0);
          } catch (e) {
            console.warn(`listObjects ${b.Name}`, e);
          }

          try {
            metadata = await getBucketMetadata(b.Name);
          } catch (e) {
            console.warn(`metadata ${b.Name}`, e);
          }

          return {
            ...b,
            objectCount,
            totalSize,
            permission: metadata?.permission ?? "Unknown",
            metadata,
            selected: false,
          };
        })
      );

      const sorted = withStats.sort(
        (a, b) =>
          new Date(b.CreationDate).getTime() - new Date(a.CreationDate).getTime()
      );

      setBuckets(sorted);
      setFilteredBuckets(sorted);
    } catch (e) {
      console.error(e);
      setError("Failed to load buckets. Check backend connection.");
    }
  };

  useEffect(() => { loadBuckets(); }, []);

  useEffect(() => {
    const filtered = buckets.filter((b) =>
      b.Name.toLowerCase().includes(searchTerm.toLowerCase())
    );
    setFilteredBuckets(filtered);
  }, [searchTerm, buckets]);

  /* ───── Selection ───── */
  const handleSelectAll = () => {
    const newVal = !selectAll;
    setSelectAll(newVal);
    setFilteredBuckets((p) => p.map((b) => ({ ...b, selected: newVal })));
  };
  const handleSelectBucket = (name: string) => {
    setFilteredBuckets((p) =>
      p.map((b) => (b.Name === name ? { ...b, selected: !b.selected } : b))
    );
  };

  /* ───── Bulk delete ───── */
  const handleBulkDelete = async () => {
    const sel = filteredBuckets.filter((b) => b.selected);
    if (!sel.length) return;
    if (!window.confirm(`Delete ${sel.length} bucket(s)?`)) return;

    setError("");
    try {
      await Promise.all(sel.map((b) => apiDeleteBucket(b.Name)));
      setBuckets((p) => p.filter((b) => !sel.some((s) => s.Name === b.Name)));
      setSelectAll(false);
    } catch (e) {
      console.error(e);
      setError("Failed to delete selected buckets.");
    }
  };

  /* ───── Create bucket ───── */
  const handleCreateBucket = async (e: React.FormEvent) => {
    e.preventDefault();
    const name = newBucketName.trim();
    if (!name) return;

    setError("");
    try {
      setLoading(true);
      await apiCreateBucket(name);
      setNewBucketName("");
      await loadBuckets();
    } catch (e) {
      let msg = "Failed to create bucket.";
      if (isAxiosError(e) && e.response?.data) {
        msg = extractXmlErrorMessage(e.response.data as string);
      }
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  /* ───── Sort ───── */
  const toggleSort = () => {
    const newOrder = sortOrder === "asc" ? "desc" : "asc";
    setSortOrder(newOrder);
    const sorted = [...filteredBuckets].sort((a, b) => {
      const diff = new Date(a.CreationDate).getTime() - new Date(b.CreationDate).getTime();
      return newOrder === "asc" ? diff : -diff;
    });
    setFilteredBuckets(sorted);
  };

  /* ───── Render ───── */
  return (
    <div className="space-y-4 max-w-7xl mx-auto px-2 sm:px-4">
      <h1 className="text-2xl sm:text-3xl font-bold text-blue-700">Buckets</h1>

      {/* ── Toolbar ── */}
      <div className="flex flex-col sm:flex-row items-center justify-between gap-3">
        {/* Search */}
        <input
          type="text"
          placeholder="Search buckets..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full sm:w-64 border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />

        {/* Create form */}
        <form onSubmit={handleCreateBucket} className="flex gap-2 w-full sm:w-auto">
          <input
            type="text"
            value={newBucketName}
            onChange={(e) => setNewBucketName(e.target.value)}
            placeholder="Bucket name..."
            className="flex-1 sm:w-48 border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
          />
          <button
            type="submit"
            disabled={loading || !newBucketName.trim()}
            className="bg-green-600 hover:bg-green-700 text-white px-4 py-1.5 rounded-md text-sm transition disabled:opacity-50"
          >
            {loading ? "…" : "Create"}
          </button>
        </form>

        {/* Bulk delete */}
        {filteredBuckets.some((b) => b.selected) && (
          <button
            onClick={handleBulkDelete}
            className="text-red-600 hover:text-red-800 p-2"
            title="Delete selected buckets"
          >
            <Trash2 size={20} />
          </button>
        )}
      </div>

      {/* ── Error ── */}
      {error && (
        <p className="text-red-600 bg-red-50 p-2 rounded text-sm">{error}</p>
      )}

      {/* ── Table (sticky header, scrollable body) ── */}
      <div className="overflow-auto max-h-[calc(100vh-260px)] border rounded-lg shadow">
        <table className="min-w-full bg-white text-center">
          <thead className="bg-gray-100 text-gray-700 text-xs uppercase sticky top-0 z-10">
            <tr>
              <th className="p-2 w-10">
                <input
                  type="checkbox"
                  checked={selectAll}
                  onChange={handleSelectAll}
                  className="w-4 h-4 rounded-full border-2 border-gray-400 checked:bg-indigo-600 checked:border-indigo-600 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-1 cursor-pointer"
                />
              </th>
              <th className="p-2 text-left">Bucket Name</th>
              <th className="p-2">Objects</th>
              <th className="p-2">Size</th>
              <th className="p-2">Access</th>
              <th
                className="p-2 cursor-pointer hover:bg-gray-200 transition"
                onClick={toggleSort}
              >
                <div className="flex items-center justify-center gap-1">
                  <span>Created</span>
                  {sortOrder === "asc" ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
                </div>
              </th>
              <th className="p-2">Actions</th>
            </tr>
          </thead>

          <tbody className="text-sm">
            {filteredBuckets.length === 0 ? (
              <tr>
                <td colSpan={7} className="py-8 text-gray-500 italic">
                  No buckets found.
                </td>
              </tr>
            ) : (
              filteredBuckets.map((b) => (
                <tr
                  key={b.Name}
                  className={`border-t hover:bg-gray-50 transition ${b.selected ? "bg-indigo-50" : ""}`}
                >
                  <td className="p-2">
                    <input
                      type="checkbox"
                      checked={b.selected}
                      onChange={() => handleSelectBucket(b.Name)}
                      className="w-4 h-4 rounded-full border-2 border-gray-400 checked:bg-indigo-600 checked:border-indigo-600 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-1 cursor-pointer"
                    />
                  </td>

                  <td className="p-2 text-left">
                    <Link
                      to={`/buckets/${b.Name}`}
                      className="text-blue-700 font-medium hover:underline truncate block max-w-[180px]"
                      title={b.Name}
                    >
                      {b.Name}
                    </Link>
                  </td>

                  <td className="p-2">{b.objectCount}</td>
                  <td className="p-2">{formatBytes(b.totalSize)}</td>

                  <td className="p-2">
                    <span
                      className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${getPermissionColor(
                        b.permission
                      )}`}
                    >
                      {getPermissionLabel(b.permission)}
                    </span>
                  </td>

                  <td className="p-2">
                    {new Date(b.CreationDate).toLocaleDateString()}
                  </td>

                  <td className="p-2">
                    <Link
                      to={`/buckets/${b.Name}`}
                      className="text-blue-600 hover:text-blue-800 relative group"
                      aria-label={`View objects in ${b.Name}`}
                    >
                      <Eye size={18} />
                      <span className="absolute bottom-5 left-1/2 -translate-x-1/2 bg-gray-800 text-white text-xs px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition pointer-events-none whitespace-nowrap">
                        View objects
                      </span>
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-gray-500 italic mt-2">
        Note: Buckets must be empty before deletion.
      </p>
    </div>
  );
}