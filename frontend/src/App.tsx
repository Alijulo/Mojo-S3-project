import { Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import Dashboard from "./components/Dashboard";
import Buckets from "./components/Buckets";
import BucketFiles from "./components/BucketFiles";
import RecentFiles from "./components/RecentFiles";

export default function App() {
  return (
    <div className="flex flex-col min-h-screen">
      <Navbar />
      <main className="flex-grow max-w-5xl mx-auto py-10 px-6 space-y-8">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/buckets" element={<Buckets />} />
          <Route path="/buckets/:bucket" element={<BucketFiles />} />
          <Route path="/recentfiles" element={<RecentFiles />} />
        </Routes>
      </main>
      <Footer />
    </div>
  );
}
