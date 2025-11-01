// App.tsx
import { Routes, Route, Navigate } from "react-router-dom";
import Footer from "./components/Footer";
import Dashboard from "./components/Dashboard";
import Buckets from "./components/Buckets";
import BucketFiles from "./components/BucketFiles";
import Login from "./components/Login";
import Layout from "./components/Layout";

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const token = localStorage.getItem("authToken");
  if (!token) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>; // ← Wrap in fragment
}

export default function App() {
  return (
    <div className="flex flex-col min-h-screen">
      <Routes>
        {/* Public */}
        <Route path="/login" element={<Login />} />

        {/* Protected — Wrap Layout + Routes */}
        <Route
          path="/*"
          element={
            <ProtectedRoute>
              <Layout />
            </ProtectedRoute>
          }
        >
          <Route path="" element={<Dashboard />} />
          <Route path="buckets" element={<Buckets />} />
          <Route path="buckets/:bucket" element={<BucketFiles />} />
        </Route>
      </Routes>

      <Footer />
    </div>
  );
}