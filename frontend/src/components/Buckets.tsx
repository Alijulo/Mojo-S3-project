import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import {
  listBuckets,
  createBucket as apiCreateBucket,
  deleteBucket as apiDeleteBucket,
  S3Bucket,
} from "../api";

export default function Buckets() {
  const [buckets, setBuckets] = useState<S3Bucket[]>([]);
  const [newBucketName, setNewBucketName] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // --- 1. Load Buckets (GET /api/v1/buckets) ---
  const loadBuckets = async () => {
    setError("");
    try {
      const loadedBuckets = await listBuckets();
      setBuckets(loadedBuckets.sort((a, b) => a.Name.localeCompare(b.Name)));
    } catch (err) {
      console.error("Failed to load buckets:", err);
      setError("❌ Failed to load buckets. Check backend connection and VITE_API_URL.");
    }
  };

  useEffect(() => {
    loadBuckets();
  }, []);

  // --- 2. Create Bucket (PUT /api/v1/bucket/{name}) ---
  const handleCreateBucket = async (e: React.FormEvent) => {
    e.preventDefault();
    const bucketName = newBucketName.trim();
    if (!bucketName) return;

    setError("");

    try {
      setLoading(true);
      await apiCreateBucket(bucketName);
      setNewBucketName("");
      loadBuckets();
    } catch (error) {
      console.error("Failed to create bucket:", error);

      let errorMessage = "❌ Failed to create bucket.";
      if (isAxiosError(error) && error.response) {
        errorMessage = `❌ Server Error (${error.response.status}): ${error.response.data || "Could not connect."}`;
      }
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // --- 3. Delete Bucket (DELETE /api/v1/bucket/{name}) ---
  const handleDeleteBucket = async (name: string) => {
    if (!window.confirm(`Are you sure you want to delete bucket '${name}'? This cannot be undone.`)) {
      return;
    }

    setError("");
    try {
      await apiDeleteBucket(name);
      loadBuckets();
    } catch (error) {
      console.error("Failed to delete bucket:", error);

      let errorMessage = "❌ Failed to delete bucket.";
      if (isAxiosError(error) && error.response) {
        if (error.response.status === 409) {
          errorMessage = "❌ Error: Bucket must be empty before deletion. (409 Conflict)";
        } else {
          errorMessage = `❌ Server Error (${error.response.status}): ${error.response.data || "Could not connect."}`;
        }
      }
      setError(errorMessage);
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold text-blue-700">🪣 Buckets</h1>

      <form onSubmit={handleCreateBucket} className="flex gap-2">
        <input
          type="text"
          value={newBucketName}
          onChange={(e) => setNewBucketName(e.target.value)}
          placeholder="Enter bucket name (e.g., project-data)..."
          className="border border-gray-300 rounded p-2 flex-grow focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
        <button
          type="submit"
          disabled={loading || !newBucketName.trim()}
          className="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded transition disabled:opacity-50"
        >
          {loading ? "Creating..." : "Create"}
        </button>
      </form>

      {error && <p className="text-red-600 font-medium bg-red-50 p-3 rounded">{error}</p>}

      <ul className="space-y-3 pt-2">
        {buckets.length === 0 ? (
          <p className="text-gray-500 italic">No buckets yet.</p>
        ) : (
          buckets.map((b) => (
            <li
              key={b.Name}
              className="flex justify-between items-center bg-white p-4 rounded-lg shadow border border-gray-100"
            >
              <div>
                <Link
                  to={`/buckets/${b.Name}`}
                  className="text-blue-700 font-medium hover:underline text-lg"
                >
                  {b.Name}
                </Link>
                <p className="text-xs text-gray-500 mt-0.5">
                  Created: {new Date(b.CreationDate).toLocaleDateString()}
                </p>
              </div>
              <button
                onClick={() => handleDeleteBucket(b.Name)}
                className="text-red-600 hover:text-red-800 text-sm font-medium transition"
              >
                Delete
              </button>
            </li>
          ))
        )}
      </ul>

      <p className="text-sm text-gray-500 italic">
        Note: Buckets must be empty before deletion.
      </p>
    </div>
  );
}
