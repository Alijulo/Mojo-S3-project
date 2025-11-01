import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import {
  listBuckets,
  createBucket as apiCreateBucket,
  deleteBucket as apiDeleteBucket,
  listObjects,
  S3Bucket,
} from "../api";
import { Eye, ArrowUpDown, ArrowUp, ArrowDown } from "lucide-react";

interface BucketWithStats extends S3Bucket {
  objectCount: number;
  totalSize: number;
  access: string;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(1)} GB`;
}

function extractXmlErrorMessage(xmlText: string): string {
  try {
    const parser = new DOMParser();
    const xml = parser.parseFromString(xmlText, "application/xml");
    const msg = xml.querySelector("Message")?.textContent?.trim();
    return msg || "Unknown server error";
  } catch {
    return "Unknown server error";
  }
}

export default function Buckets() {
  const [buckets, setBuckets] = useState<BucketWithStats[]>([]);
  const [filteredBuckets, setFilteredBuckets] = useState<BucketWithStats[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [newBucketName, setNewBucketName] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");


  
  const loadBuckets = async () => {
    setError("");
    try {
      const loadedBuckets = await listBuckets();

      const bucketsWithStats = await Promise.all(
        loadedBuckets.map(async (b) => {
          try {
            const objects = await listObjects(b.Name);
            const totalSize = objects.reduce((sum, o) => sum + (o.Size || 0), 0);
            return {
              ...b,
              objectCount: objects.length,
              totalSize,
              access: "Private",
            };
          } catch {
            return { ...b, objectCount: 0, totalSize: 0, access: "Unknown" };
          }
        })
      );

      const sortedBuckets = bucketsWithStats.sort(
        (a, b) =>
          new Date(b.CreationDate).getTime() - new Date(a.CreationDate).getTime()
      );

      setBuckets(sortedBuckets);
      setFilteredBuckets(sortedBuckets);
    } catch (err) {
      console.error("Failed to load buckets:", err);
      setError("❌ Failed to load buckets. Check backend connection and VITE_API_URL.");
    }
  };

  useEffect(() => {
    loadBuckets();
  }, []);

  useEffect(() => {
    if (!searchTerm.trim()) {
      setFilteredBuckets(buckets);
    } else {
      setFilteredBuckets(
        buckets.filter((b) =>
          b.Name.toLowerCase().includes(searchTerm.toLowerCase())
        )
      );
    }
  }, [searchTerm, buckets]);

 

  const handleCreateBucket = async (e: React.FormEvent) => {
    e.preventDefault();
    const bucketName = newBucketName.trim();
    if (!bucketName) return;

    setError("");
    try {
      setLoading(true);
      await apiCreateBucket(bucketName);
      setNewBucketName("");
      await loadBuckets();
    } catch (error) {
      console.error("Failed to create bucket:", error);

      let errorMessage = "❌ Failed to create bucket.";

      if (isAxiosError(error) && error.response) {
        let serverMessage = "";

        if (typeof error.response.data === "string") {
          serverMessage = extractXmlErrorMessage(error.response.data);
        }

        errorMessage = `❌ ${serverMessage || "Server error"}`;
      }

      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };


  const handleDeleteBucket = async (name: string) => {
    if (!window.confirm(`Are you sure you want to delete bucket '${name}'?`))
      return;
    setError("");
    try {
      await apiDeleteBucket(name);
      setBuckets((prev) => prev.filter((b) => b.Name !== name));
    } catch (error) {
      console.error("Failed to delete bucket:", error);
      let errorMessage = "❌ Failed to delete bucket.";
      if (isAxiosError(error) && error.response) {
        if (error.response.status === 409) {
          errorMessage =
            "❌ Error: Bucket must be empty before deletion. (409 Conflict)";
        } else {
          errorMessage = `❌ Server Error (${error.response.status}): ${
            error.response.data || "Could not connect."
          }`;
        }
      }
      setError(errorMessage);
    }
  };

  const toggleSort = () => {
    const newOrder = sortOrder === "asc" ? "desc" : "asc";
    setSortOrder(newOrder);
    const sorted = [...filteredBuckets].sort((a, b) => {
      const diff =
        new Date(a.CreationDate).getTime() - new Date(b.CreationDate).getTime();
      return newOrder === "asc" ? diff : -diff;
    });
    setFilteredBuckets(sorted);
  };

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold text-blue-700">🪣 Buckets</h1>

      {/* Search + Create Section */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        {/* Search Box */}
        <input
          type="text"
          placeholder="🔍 Search buckets..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="border border-gray-300 rounded p-2 w-full sm:w-[300px] md:w-[350px] focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />

        {/* Create Bucket Form */}
        <form
          onSubmit={handleCreateBucket}
          className="flex gap-2 items-center ml-auto md:w-[380px]"
        >
          <input
            type="text"
            value={newBucketName}
            onChange={(e) => setNewBucketName(e.target.value)}
            placeholder="Bucket name..."
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
      </div>

      {error && (
        <p className="text-red-600 font-medium bg-red-50 p-3 rounded">{error}</p>
      )}

      {/* Buckets Table */}
      <div className="overflow-x-auto shadow rounded-lg border border-gray-100">
        <table className="min-w-full bg-white border-collapse text-center">
          <thead className="bg-gray-100 text-gray-700 text-sm uppercase">
            <tr>
              <th className="p-3 min-w-[200px] text-left pl-4">Bucket Name</th>
              <th className="p-3 min-w-[100px]">Objects</th>
              <th className="p-3 min-w-[120px]">Size</th>
              <th className="p-3 min-w-[120px]">Access</th>
              <th
                className="p-3 min-w-[140px] cursor-pointer select-none hover:bg-gray-200 transition"
                onClick={toggleSort}
              >
                <div className="flex items-center justify-center gap-2">
                  <span>Created On</span>
                  {sortOrder === "asc" ? (
                    <ArrowUp size={16} />
                  ) : (
                    <ArrowDown size={16} />
                  )}
                </div>
              </th>
              <th className="p-3 min-w-[100px]">Actions</th>
            </tr>
          </thead>
          <tbody>
            {filteredBuckets.length === 0 ? (
              <tr>
                <td colSpan={6} className="text-center text-gray-500 py-4 italic">
                  No buckets found.
                </td>
              </tr>
            ) : (
              filteredBuckets.map((b) => (
                <tr
                  key={b.Name}
                  className="border-t hover:bg-gray-50 transition text-center"
                >
                  <td className="p-3 text-left pl-4">
                    <Link
                      to={`/buckets/${b.Name}`}
                      className="text-blue-700 font-medium hover:underline block truncate max-w-[220px]"
                      title={b.Name}
                    >
                      {b.Name}
                    </Link>
                  </td>
                  <td className="p-3">{b.objectCount}</td>
                  <td className="p-3">{formatBytes(b.totalSize)}</td>
                  <td className="p-3">{b.access}</td>
                  <td className="p-3">
                    {new Date(b.CreationDate).toLocaleDateString()}
                  </td>
                  <td className="p-3">
                    <div className="flex items-center justify-center gap-5">
                      <Link
                        to={`/buckets/${b.Name}`}
                        className="text-blue-600 hover:text-blue-800 transition flex items-center justify-center relative group"
                        aria-label={`View ${b.Name}`}
                      >
                        <Eye size={18} />
                        <span className="absolute bottom-6 left-1/2 -translate-x-1/2 text-xs bg-gray-800 text-white px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition pointer-events-none whitespace-nowrap">
                          View Objects
                        </span>
                      </Link>
                      <button
                        onClick={() => handleDeleteBucket(b.Name)}
                        className="text-red-600 hover:text-red-800 transition"
                        aria-label={`Delete ${b.Name}`}
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <p className="text-sm text-gray-500 italic">
        Note: Buckets must be empty before deletion.
      </p>
    </div>
  );
}
