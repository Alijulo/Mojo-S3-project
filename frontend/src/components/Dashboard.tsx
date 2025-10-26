import { useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import { createBucket } from "../api";

export default function Dashboard() {
  const [bucketName, setBucketName] = useState("");
  const [message, setMessage] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setMessage("");

    if (!bucketName.trim()) {
      setMessage("⚠️ Please enter a bucket name.");
      return;
    }

    try {
      await createBucket(bucketName.trim());
      setMessage(`✅ Bucket '${bucketName}' created successfully (or already exists).`);
      setBucketName("");
    } catch (error) {
      console.error("Failed to create bucket", error);
      let errorMessage = "❌ Failed to create bucket. Check backend logs.";

      if (isAxiosError(error) && error.response) {
        errorMessage = `❌ Server Error (${error.response.status}): ${
          error.response.data || "Could not connect."
        }`;
      }

      setMessage(errorMessage);
    }
  };
  return (
    <main className="fixed inset-0 overflow-hidden flex flex-col items-center justify-center bg-gray-50 p-8">
      <div className="max-w-5xl w-full space-y-8">
        {/* Create Bucket Section */}
        <section className="bg-white rounded-lg shadow w-full p-6">
          <h2 className="text-xl font-semibold mb-4 text-indigo-700">
            🪣 Create New Bucket
          </h2>
          <form onSubmit={handleSubmit} className="space-y-4">
            <input
              type="text"
              name="bucket"
              value={bucketName}
              onChange={(e) => setBucketName(e.target.value)}
              placeholder="Enter bucket name (e.g., my-new-storage)"
              className="border border-gray-300 rounded p-2 w-full text-base focus:outline-none focus:ring-2 focus:ring-indigo-500"
              required
            />
            <button
              type="submit"
              className="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded text-base transition"
            >
              Create Bucket
            </button>
          </form>

          {message && (
            <p className="mt-3 text-sm font-medium text-gray-700">{message}</p>
          )}
        </section>

        {/* Quick Links */}
        <section className="bg-white rounded-lg shadow w-full p-6">
          <h2 className="text-xl font-semibold mb-4 text-indigo-700">
            🔗 Quick Access
          </h2>
          <div className="grid grid-cols-2 gap-6">
            <Link
              to="/recentfiles"
              className="block text-center bg-indigo-100 hover:bg-indigo-200 p-4 rounded-lg border border-indigo-300 text-base font-medium text-indigo-800 transition"
            >
              📁 View Recent Files
            </Link>
            <Link
              to="/buckets"
              className="block text-center bg-indigo-100 hover:bg-indigo-200 p-4 rounded-lg border border-indigo-300 text-base font-medium text-indigo-800 transition"
            >
              🪣 Manage Buckets
            </Link>
          </div>
        </section>
      </div>
    </main>
  );
}
