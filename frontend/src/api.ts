import axios, { AxiosResponse, isAxiosError } from "axios";

// --- 1. S3 Type Definitions (Based on your Rust structs) ---

// Corresponds to ListAllMyBucketsResult -> Bucket
export interface S3Bucket {
    Name: string;
    CreationDate: string; // ISO 8601 string
}

// Corresponds to ListBucketResult -> Contents
export interface S3Object {
    Key: string;
    LastModified: string; // ISO 8601 string
    Size: number; // Size in bytes
}

// --- 2. XML Parsing Utility ---

/**
 * A basic function to parse XML string response into a usable object structure.
 * This is a highly simplified version using DOMParser. 
 * For robust S3 compliance, consider a library like 'fast-xml-parser'.
 */
const parseS3Xml = (xmlString: string) => {
    const parser = new DOMParser();
    const xmlDoc = parser.parseFromString(xmlString, "application/xml");
    
    // Check for an S3 Error response (e.g., AppError in Rust)
    if (xmlDoc.getElementsByTagName("Error").length > 0) {
        const code = xmlDoc.getElementsByTagName("Code")[0]?.textContent || "Unknown";
        const message = xmlDoc.getElementsByTagName("Message")[0]?.textContent || "An unknown S3 error occurred.";
        const status = xmlDoc.getElementsByTagName("Status")[0]?.textContent || "500";
        // Throw an error that includes the server-provided message
        throw new Error(`S3 XML Error (${status} - ${code}): ${message}`);
    }

    // Parse ListAllMyBucketsResult (GET /buckets)
    if (xmlDoc.getElementsByTagName("ListAllMyBucketsResult").length > 0) {
        const bucketNodes = xmlDoc.getElementsByTagName("Bucket");
        const buckets: S3Bucket[] = Array.from(bucketNodes).map(node => ({
            Name: node.getElementsByTagName("Name")[0]?.textContent || "unknown",
            CreationDate: node.getElementsByTagName("CreationDate")[0]?.textContent || new Date().toISOString(),
        }));
        return buckets;
    }

    // Parse ListBucketResult (GET /{bucket})
    if (xmlDoc.getElementsByTagName("ListBucketResult").length > 0) {
        const objectNodes = xmlDoc.getElementsByTagName("Contents");
        // Note: S3 ListObjects can return 'CommonPrefixes' for folders, but your Rust code only returns 'Contents' (files).
        const objects: S3Object[] = Array.from(objectNodes).map(node => ({
            Key: node.getElementsByTagName("Key")[0]?.textContent || "unknown",
            LastModified: node.getElementsByTagName("LastModified")[0]?.textContent || new Date().toISOString(),
            Size: parseInt(node.getElementsByTagName("Size")[0]?.textContent || "0", 10),
        }));
        return objects;
    }
    
    // Default success response for PUT/DELETE operations
    return { success: true };
};


// --- 3. Base Configuration and Axios Instance ---

const API_URL_ROOT = import.meta.env.VITE_API_URL;
if (!API_URL_ROOT) throw new Error("❌ VITE_API_URL is not defined in .env");

// ✅ Correct BASE_URL to include the /api/v1 prefix used by the Rust server
const BASE_URL = `${API_URL_ROOT}/api/v1`; 

const s3Api = axios.create({
    baseURL: BASE_URL,
});

// --- 4. S3 Operation Functions ---

/**
 * GET /api/v1/buckets -> List all buckets (Returns XML)
 */
export async function listBuckets(): Promise<S3Bucket[]> {
    const response: AxiosResponse<string> = await s3Api.get(`/buckets`, {
        responseType: 'text', 
    });
    return parseS3Xml(response.data) as S3Bucket[];
}

/**
 * PUT /api/v1/bucket/{bucket} -> Create a bucket (Expects 200 OK)
 */
export async function createBucket(bucketName: string): Promise<void> {
    await s3Api.put(`/bucket/${bucketName}`); 
}

/**
 * DELETE /api/v1/bucket/{bucket} -> Delete an empty bucket (Expects 204 No Content)
 */
export async function deleteBucket(bucketName: string): Promise<void> {
    await s3Api.delete(`/bucket/${bucketName}`);
}


/**
 * GET /api/v1/bucket/{bucket} -> List objects in a bucket (Returns XML)
 */
export async function listObjects(bucketName: string): Promise<S3Object[]> {
    const response: AxiosResponse<string> = await s3Api.get(`/bucket/${bucketName}`, {
        responseType: 'text', 
    });
    return parseS3Xml(response.data) as S3Object[];
}

/**
 * PUT /api/v1/bucket/{bucket}/{key} -> Upload an object
 */
export async function putObject(bucketName: string, key: string, file: File): Promise<void> {
    // Send the raw File object as the body
    await s3Api.put(`/bucket/${bucketName}/${key}`, file, {
        headers: {
            // Set content type based on the file type
            "Content-Type": file.type || "application/octet-stream"
        },
    });
}

// export async function putObject(bucketName: string, key: string, file: File) {
//   const encodedKey = encodeURIComponent(key);

//   try {
//     const response = await s3Api.put(`/bucket/${bucketName}/${encodedKey}`, file, {
//       headers: {
//         "Content-Type": file.type || "application/octet-stream",
//       },
//     });

//     console.log("✅ Upload API response:", response.status, response.data);

//     // If no body, still return a message
//     return {
//       status: response.status,
//       message: response.data || "File uploaded successfully",
//     };
//   } catch (error: any) {
//     console.error("❌ Upload error:", error);
//     throw error;
//   }
// }

/**
 * DELETE /api/v1/bucket/{bucket}/{key} -> Delete an object (Expects 204 No Content)
 */
export async function deleteObject(bucketName: string, key: string): Promise<void> {
    await s3Api.delete(`/bucket/${bucketName}/${key}`);
}



/**
 * GET /api/v1/bucket/{bucket}/{key} -> Generate object URL for direct download
 */
export function getObjectUrl(bucketName: string, key: string): string {
    return `${BASE_URL}/bucket/${bucketName}/${key}`;
}