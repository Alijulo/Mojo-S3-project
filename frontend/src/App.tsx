import { Routes, Route, Navigate } from "react-router-dom";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import Dashboard from "./components/Dashboard";
import Buckets from "./components/Buckets";
import BucketFiles from "./components/BucketFiles";
// import RecentFiles from "./components/RecentFiles";
import Login from "./components/Login";

function ProtectedRoute({ children }: { children: JSX.Element }) {
  const token = localStorage.getItem("authToken");
  if (!token) {
    return <Navigate to="/login" replace />;
  }
  return children;
}

export default function App() {
  return (
    <div className="flex flex-col min-h-screen">
      <Navbar />
      {/* ADD pt-16 to prevent content from hiding under navbar */}
      <main className="flex-grow max-w-5xl mx-auto py-10 px-6 space-y-8 pt-16">
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <Dashboard />
              </ProtectedRoute>
            }
          />
          <Route
            path="/buckets"
            element={
              <ProtectedRoute>
                <Buckets />
              </ProtectedRoute>
            }
          />
          <Route
            path="/buckets/:bucket"
            element={
              <ProtectedRoute>
                <BucketFiles />
              </ProtectedRoute>
            }
          />
        </Routes>
      </main>
      <Footer />
    </div>
  );
}
