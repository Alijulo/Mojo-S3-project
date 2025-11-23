import { useEffect, useState, useRef } from "react";
import { useParams } from "react-router-dom";
import { isAxiosError } from "axios";
import { listObjects, putObject, deleteObject, downloadObject, S3Object } from "../api";

export default function BucketFiles() {
  const { bucket } = useParams<{ bucket: string }>();
  const [files, setFiles] = useState<S3Object[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  // --- 1. Load Files ---
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

  // --- 2. Upload File ---
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

  // --- 3. Delete File ---
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

  // --- 4. View File (open in new tab) ---
  const handleViewFile = async (file: S3Object) => {
    if (!bucket) return;
    try {
      const blob = await downloadObject(bucket, file.Key);
      const url = URL.createObjectURL(blob);
      window.open(url, "_blank"); // open in new tab
    } catch (err) {
      console.error("View failed:", err);
      setMessage("❌ Could not open file.");
    }
  };

  // --- 5. Download File (save to disk) ---
  const handleDownloadFile = async (file: S3Object) => {
    if (!bucket) return;
    try {
      const blob = await downloadObject(bucket, file.Key);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.Key; // suggest filename
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Download failed:", err);
      setMessage("❌ Download failed.");
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
                <div className="flex items-center space-x-4">
                  <span className="text-indigo-600 font-medium">{file.Key}</span>
                  <button
                    onClick={() => handleViewFile(file)}
                    className="text-blue-600 hover:text-blue-800 text-sm font-medium transition"
                  >
                    View
                  </button>
                  <button
                    onClick={() => handleDownloadFile(file)}
                    className="text-green-600 hover:text-green-800 text-sm font-medium transition"
                  >
                    Download
                  </button>
                </div>
                <div className="text-right">
                  <p className="text-gray-500 text-xs">
                    Size: {(file.Size / 1024).toFixed(2)} KB | Modified:{" "}
                    {new Date(file.LastModified).toLocaleDateString()}
                  </p>
                  <button
                    onClick={() => handleDeleteFile(file.Key)}
                    className="text-red-600 hover:text-red-800 text-sm font-medium transition mt-1"
                  >
                    Delete
                  </button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
