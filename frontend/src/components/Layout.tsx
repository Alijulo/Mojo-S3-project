// components/Layout.tsx
import { Outlet } from "react-router-dom";
import Navbar from "./Navbar";
import Sidebar from "./Sidebar";
import { useState } from "react";

export default function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(true);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar - Always visible */}
      <Sidebar isOpen={sidebarOpen} onToggle={() => setSidebarOpen(!sidebarOpen)} />

      {/* Main Content Area */}
      <div className={`flex-1 flex flex-col transition-all duration-300 ${sidebarOpen ? "ml-64" : "ml-16"}`}>
        {/* Fixed Navbar */}
        <Navbar />

        {/* Page Content - Changes per route */}
        <main className="flex-1 overflow-auto pt-16"> {/* pt-16 = h-16 navbar */}
          <div className="p-6">
            <Outlet /> {/* ← Your page (Dashboard, Buckets, etc.) */}
          </div>
        </main>
      </div>
    </div>
  );
}