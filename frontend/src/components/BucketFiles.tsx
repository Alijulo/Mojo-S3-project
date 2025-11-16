import { useEffect, useState, useRef } from "react";
import { useParams } from "react-router-dom";
import { isAxiosError } from "axios";
import { listObjects, putObject, deleteObject, getObjectUrl, S3Object } from "../api";

export default function BucketFiles() {
  const { bucket } = useParams<{ bucket: string }>();
  const [files, setFiles] = useState<S3Object[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  // --- 1. Load Files (GET /api/v1/bucket/{bucket}) ---
  const loadFiles = async () => {
    if (!bucket) return;
    setMessage((prev) => (prev.startsWith("❌") ? "" : prev));
    try {
      const loadedObjects = await listObjects(bucket);
      setFiles(loadedObjects.sort((a, b) => a.Key.localeCompare(b.Key)));
    } catch (err) {
      console.error("Failed to load files:", err);
      setFiles([]);
      if (isAxiosError(err) && err.response?.status === 404) {
        setMessage(`⚠️ Bucket '${bucket}' not found.`);
      } else {
        setMessage("❌ Failed to load files. Check backend connection.");
      }
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSelectedFile(e.target.files ? e.target.files[0] : null);
  };

  // --- 2. Upload File (PUT /api/v1/bucket/{bucket}/{key}) ---
  const handleUpload = async (e: React.MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    if (!selectedFile || !bucket) return;

    try {
      setUploading(true);
      setMessage("");
      const encodedKey = encodeURIComponent(selectedFile.name);
      await putObject(bucket, encodedKey, selectedFile);
      setMessage(`✅ Uploaded successfully: ${selectedFile.name}`);
      setSelectedFile(null);

      if (fileInputRef.current) fileInputRef.current.value = "";
      await loadFiles();

      setTimeout(() => setMessage(""), 5000);
    } catch (err) {
      console.error("Upload failed:", err);
      let errorMessage = "❌ Upload failed. Try again.";
      if (isAxiosError(err) && err.response) {
        errorMessage = `❌ Upload failed (${err.response.status}): ${err.response.data || "Server error"}`;
      }
      setMessage(errorMessage);
      setTimeout(() => setMessage(""), 5000);
    } finally {
      setUploading(false);
    }
  };

  // --- 3. Delete File (DELETE /api/v1/bucket/{bucket}/{key}) ---
  const handleDeleteFile = async (key: string) => {
    if (!bucket) return;
    if (!window.confirm(`Delete file "${key}"? This cannot be undone.`)) return;

    try {
      const encodedKey = encodeURIComponent(key);
      await deleteObject(bucket, encodedKey);
      setMessage(`🗑️ File '${key}' deleted.`);
      await loadFiles();
      setTimeout(() => setMessage(""), 3000);
    } catch (err) {
      console.error("Failed to delete file:", err);
      let errorMessage = "❌ Failed to delete file.";
      if (isAxiosError(err) && err.response) {
        errorMessage = `❌ Delete failed (${err.response.status}): ${err.response.data || "Server error"}`;
      }
      setMessage(errorMessage);
    }
  };

  useEffect(() => {
    if (bucket) loadFiles();
  }, [bucket]);

  return (
    <div className="space-y-8">
      <h2 className="text-2xl font-bold text-indigo-700">📂 Bucket: {bucket}</h2>

      {/* Upload Section */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">⬆️ Upload Single File</h3>
        {message && <p className="mb-2 text-sm font-medium text-gray-700">{message}</p>}
        <input
          type="file"
          multiple={false}
          ref={fileInputRef}
          onChange={handleFileChange}
          className="border border-gray-300 rounded p-2 w-full mb-4"
        />
        <button
          onClick={handleUpload}
          disabled={!selectedFile || uploading}
          className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded transition disabled:opacity-50"
        >
          {uploading ? "Uploading..." : "Upload"}
        </button>
      </div>

      {/* Files List */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">Files</h3>
        {files.length === 0 ? (
          <p className="text-gray-500 italic">No files in this bucket.</p>
        ) : (
          <ul className="divide-y divide-gray-200">
            {files.map((file) => (
              <li key={file.Key} className="flex justify-between items-center py-3">
                <div>
                  <a
                    href={getObjectUrl(bucket!, file.Key)}
                    className="text-indigo-600 hover:underline font-medium"
                    target="_blank"
                    rel="noreferrer"
                  >
                    {file.Key}
                  </a>
                  <p className="text-gray-500 text-xs mt-0.5">
                    Size: {(file.Size / 1024).toFixed(2)} KB | Modified:{" "}
                    {new Date(file.LastModified).toLocaleDateString()}
                  </p>
                </div>
                <button
                  onClick={() => handleDeleteFile(file.Key)}
                  className="text-red-600 hover:text-red-800 text-sm font-medium transition"
                >
                  Delete
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
