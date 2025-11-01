// login.tsx
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { login } from "../api";

export default function Login() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  // Clear error when user types
  useEffect(() => {
    if (username || password) {
      setError("");
    }
  }, [username, password]);

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();

    // DO NOT clear error here — let useEffect handle it
    // if (error) setError("");  ← REMOVE THIS LINE

    if (!username.trim() || !password.trim()) {
      setError("Please enter username and password");
      return;
    }

    setLoading(true);

    try {
      await login(username.trim(), password);
      navigate("/", { replace: true });
    } catch (err: any) {
      console.error("Login failed:", err);

      let message = "Login failed. Check connection.";

      // Prioritize response status
      if (err.response?.status === 403 || err.response?.status === 401) {
        message = "Invalid username or password";
      } else if (err.response?.status === 500) {
        message = "Server error. Try again later.";
      } else if (err.response?.status >= 400) {
        message = "Login failed. Please try again.";
      } else if (!err.response && err.request) {
        message = "Cannot reach server. Is backend running?";
      } else if (err.message?.includes("missing token")) {
        message = "Server error: invalid response";
      }

      setError(message);
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

        {/* Persistent Error Box */}
        {error && (
          <div className="mb-4 p-4 bg-red-50 border border-red-300 text-red-700 rounded-lg text-sm font-medium animate-fade-in">
            {error}
          </div>
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
            autoFocus
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
            disabled={loading || !username.trim() || !password.trim()}
            className="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-3 rounded-lg font-medium transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
          >
            {loading ? (
              <>
                <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Signing in...
              </>
            ) : (
              "Sign In"
            )}
          </button>
        </form>

        {/* Dev hint */}
        {/* {import.meta.env.DEV && (
          <p className="mt-6 text-xs text-gray-500 text-center">
            Default: <code className="bg-gray-100 px-1 rounded">admin</code> /{" "}
            <code className="bg-gray-100 px-1 rounded">My$ecureP@ssword!</code>
          </p>
        )} */}
      </div>
    </div>
  );
}