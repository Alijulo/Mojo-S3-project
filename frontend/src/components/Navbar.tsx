import { Link } from "react-router-dom";

export default function Navbar() {
  return (
    <nav className="bg-indigo-600 text-white py-4 px-6 flex justify-between items-center shadow">
      <h1 className="text-2xl font-bold">🪣 Mojo S3 Admin</h1>
      <div className="space-x-6">
        {/* ✅ Using Link for client-side routing */}
        <Link to="/" className="hover:underline">
          Dashboard
        </Link>
        <Link to="/buckets" className="hover:underline">
          Buckets
        </Link>
        <Link to="/recentfiles" className="hover:underline">
          Recent Files
        </Link>
      </div>
    </nav>
  );
}
