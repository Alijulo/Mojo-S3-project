// api.ts
import axios, { AxiosResponse, isAxiosError } from "axios";
import { Buffer } from "buffer";

// Safe polyfill — TypeScript now knows about globalThis.Buffer
if (typeof globalThis.Buffer === "undefined") {
  (globalThis as any).Buffer = Buffer;
}
if (typeof globalThis.btoa === "undefined") {
  globalThis.btoa = (str: string) => Buffer.from(str, "binary").toString("base64");
}

// --- 1. S3 Type Definitions (Based on your Rust structs) ---

export interface S3Bucket {
  Name: string;
  CreationDate: string; // ISO 8601 string
}

export interface S3Object {
  Key: string;
  LastModified: string; // ISO 8601 string
  Size: number; // Size in bytes
}

// --- 2. XML Parsing Utility ---

const parseS3Xml = (xmlString: string) => {
  const parser = new DOMParser();
  const xmlDoc = parser.parseFromString(xmlString, "application/xml");

  // Check for S3 Error response
  if (xmlDoc.getElementsByTagName("Error").length > 0) {
    const code = xmlDoc.getElementsByTagName("Code")[0]?.textContent || "Unknown";
    const message = xmlDoc.getElementsByTagName("Message")[0]?.textContent || "An unknown S3 error occurred.";
    const status = xmlDoc.getElementsByTagName("Status")[0]?.textContent || "500";
    throw new Error(`S3 XML Error (${status} - ${code}): ${message}`);
  }

  // Parse ListAllMyBucketsResult
  if (xmlDoc.getElementsByTagName("ListAllMyBucketsResult").length > 0) {
    const bucketNodes = xmlDoc.getElementsByTagName("Bucket");
    const buckets: S3Bucket[] = Array.from(bucketNodes).map(node => ({
      Name: node.getElementsByTagName("Name")[0]?.textContent || "unknown",
      CreationDate: node.getElementsByTagName("CreationDate")[0]?.textContent || new Date().toISOString(),
    }));
    return buckets;
  }

  // Parse ListBucketResult
  if (xmlDoc.getElementsByTagName("ListBucketResult").length > 0) {
    const objectNodes = xmlDoc.getElementsByTagName("Contents");
    const objects: S3Object[] = Array.from(objectNodes).map(node => ({
      Key: node.getElementsByTagName("Key")[0]?.textContent || "unknown",
      LastModified: node.getElementsByTagName("LastModified")[0]?.textContent || new Date().toISOString(),
      Size: parseInt(node.getElementsByTagName("Size")[0]?.textContent || "0", 10),
    }));
    return objects;
  }

  return { success: true };
};

// --- 3. Base Configuration and Axios Instances ---

const API_URL_ROOT = import.meta.env.VITE_API_URL;
if (!API_URL_ROOT) throw new Error("VITE_API_URL is not defined in .env");

const BASE_URL = `${API_URL_ROOT}/api/v1`;

// 1. Login API — NO JWT interceptor
const loginApi = axios.create({
  baseURL: BASE_URL,
  headers: { Accept: "application/xml" },
});

// 2. S3 API — WITH JWT interceptor
export const s3Api = axios.create({
  baseURL: BASE_URL,
});

// Add JWT to all S3 API requests (except login)
s3Api.interceptors.request.use((config) => {
  const token = localStorage.getItem("authToken");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Global 401/403 handler
s3Api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401 || error.response?.status === 403) {
      localStorage.removeItem("authToken");
      window.location.href = "/login";
    }
    return Promise.reject(error);
  }
);

// --- 4. LOGIN FUNCTION (FIXED) ---
export async function login(username: string, password: string): Promise<string> {
  const authHeader = `Basic ${btoa(`${username}:${password}`)}`;

  const response = await loginApi.post(
    "/login",
    null, // No body
    {
      headers: {
        Authorization: authHeader,
      },
      responseType: "text",
    }
  );

  // Parse XML: <AuthResponseXml><Token>jwt...</Token></AuthResponseXml>
  const parser = new DOMParser();
  const xmlDoc = parser.parseFromString(response.data, "application/xml");
  const tokenElement = xmlDoc.getElementsByTagName("Token")[0];
  const token = tokenElement?.textContent;

  if (!token) {
    throw new Error("Login failed: no token in response");
  }

  localStorage.setItem("authToken", token);
  return token;
}

// --- 5. S3 Operation Functions (unchanged) ---

export async function listBuckets(): Promise<S3Bucket[]> {
  const response: AxiosResponse<string> = await s3Api.get(`/buckets`, {
    responseType: 'text',
  });
  return parseS3Xml(response.data) as S3Bucket[];
}

// export async function createBucket(bucketName: string): Promise<void> {
//   await s3Api.put(`/bucket/${bucketName}`);
// }

export async function createBucket(bucketName: string, subpath?: string): Promise<void> {
  const url = subpath
    ? `/bucket/${bucketName}/${subpath}`
    : `/bucket/${bucketName}`;

  await s3Api.put(url);
}


export async function deleteBucket(bucketName: string): Promise<void> {
  await s3Api.delete(`/bucket/${bucketName}`);
}

export async function listObjects(bucketName: string): Promise<S3Object[]> {
  const response: AxiosResponse<string> = await s3Api.get(`/bucket/${bucketName}`, {
    responseType: 'text',
  });
  return parseS3Xml(response.data) as S3Object[];
}

export async function putObject(bucketName: string, key: string, file: File): Promise<void> {
  await s3Api.put(`/object/${bucketName}/${key}`, file, {
    headers: {
      "Content-Type": file.type || "application/octet-stream"
    },
  });
}

export async function deleteObject(bucketName: string, key: string): Promise<void> {
  await s3Api.delete(`/object/${bucketName}/${key}`);
}

export function getObjectUrl(bucketName: string, key: string): string {
  return `${BASE_URL}/object/${bucketName}/${key}`;
}