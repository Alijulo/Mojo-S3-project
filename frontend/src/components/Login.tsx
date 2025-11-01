// login.tsx
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { login } from "../api";

export default function Login() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);

    if (!username.trim() || !password.trim()) {
      setError("Please enter username and password");
      setLoading(false);
      return;
    }

    try {
      await login(username.trim(), password);
      navigate("/", { replace: true });
    } catch (err: any) {
      console.error("Login failed:", err);

      if (err.message.includes("missing token")) {
        setError("Server error: invalid response");
      } else if (err.response?.status === 403) {
        setError("Invalid username or password");
      } else if (err.response?.status === 500) {
        setError("Server error. Try again later.");
      } else {
        setError("Login failed. Check connection.");
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col items-center justify-start min-h-screen pt-32 bg-gray-50">
      <div className="w-96 p-8 bg-white rounded-2xl shadow-lg">
        <h2 className="text-2xl font-semibold mb-6 text-center text-indigo-700">
          Mojo S3 Admin
        </h2>

        {error && (
          <p className="text-red-600 text-sm mb-4 p-3 bg-red-50 rounded border border-red-200">
            {error}
          </p>
        )}

        <form onSubmit={handleLogin} className="space-y-5">
          <input
            type="text"
            placeholder="Username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="w-full border border-gray-300 px-4 py-3 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 transition"
            required
            disabled={loading}
          />
          <input
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full border border-gray-300 px-4 py-3 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 transition"
            required
            disabled={loading}
          />
          <button
            type="submit"
            disabled={loading}
            className="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-3 rounded-lg font-medium transition disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>

        {/* <p className="mt-6 text-xs text-gray-500 text-center">
          Default: <code className="bg-gray-100 px-1 rounded">admin</code> /{" "}
          <code className="bg-gray-100 px-1 rounded">My$ecureP@ssword!</code>
        </p> */}
      </div>
    </div>
  );
}