import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getObjectUrl } from "../api"; 

interface RecentFile {
  bucket: string;
  name: string;
  modified: string; // ISO date string
}

const MOCK_RECENT_FILES: RecentFile[] = [
  { bucket: "project-assets", name: "logo.png", modified: "2025-10-18T10:30:00.000Z" },
  { bucket: "user-data", name: "report-q3.pdf", modified: "2025-10-18T09:15:00.000Z" },
  { bucket: "project-assets", name: "style.css", modified: "2025-10-17T14:45:00.000Z" },
];

export default function RecentFiles() {
  const [files, setFiles] = useState<RecentFile[]>([]);

  const loadRecent = () => {
    console.log("Mocking recent files load as endpoint /api/v1/recentfiles is not available.");
    setFiles(MOCK_RECENT_FILES);
  };

  useEffect(() => {
    loadRecent();
  }, []);

  const filesByBucket: Record<string, RecentFile[]> = {};
  files.forEach((f) => {
    if (!filesByBucket[f.bucket]) filesByBucket[f.bucket] = [];
    filesByBucket[f.bucket].push(f);
  });

  return (
    <main className="max-w-5xl mx-auto py-8 px-6 space-y-8">
      <h2 className="text-3xl font-bold text-blue-700">🕒 Recent Files (Mocked)</h2>

      {files.length === 0 ? (
        <p className="text-gray-500 italic">No recent files.</p>
      ) : (
        Object.entries(filesByBucket).map(([bucket, bucketFiles]) => (
          <section
            key={bucket}
            className="bg-white rounded-lg shadow w-full p-6 space-y-4"
          >
            <h3 className="text-xl font-semibold text-indigo-700">{bucket}</h3>

            {bucketFiles.map((f) => (
              <div
                key={f.name}
                className="flex justify-between items-center bg-indigo-50 hover:bg-indigo-100 p-4 rounded-lg border border-indigo-200 transition"
              >
                <div className="flex flex-col">
                  <p className="text-base font-semibold text-blue-700">{f.name}</p>
                  <p className="text-sm text-gray-500">
                    Bucket:{" "}
                    <Link
                      to={`/buckets/${f.bucket}`}
                      className="text-indigo-600 hover:underline font-medium"
                    >
                      {f.bucket}
                    </Link>
                  </p>
                  <p className="text-xs text-gray-400">
                    Modified: {new Date(f.modified).toLocaleDateString()}
                  </p>
                </div>
                <a
                  href={getObjectUrl(f.bucket, f.name)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center justify-center w-10 h-10 bg-blue-500 hover:bg-blue-600 text-white rounded-full transition"
                  title="Download file"
                >
                  ⬇
                </a>
              </div>
            ))}
          </section>
        ))
      )}
    </main>
  );
}
