// components/Dashboard.tsx
import { Link } from "react-router-dom";
import {Upload, Trash2, HardDrive,Plus,Menu,X } from "lucide-react";
import { createLucideIcon } from "lucide-react";
import { bucket } from "@lucide/lab";
import Sidebar from "./Sidebar";
import { useState } from "react";


const Bucket = createLucideIcon("Bucket", bucket);

export default function Dashboard() {
  const [sidebarOpen, setSidebarOpen] = useState(true);  // ← Controls sidebar
  const [showCreateModal, setShowCreateModal] = useState(false);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <Sidebar isOpen={sidebarOpen} onToggle={() => setSidebarOpen(!sidebarOpen)} />

      {/* Main Content — Push right when sidebar is open */}
      <div className={`flex-1 flex flex-col overflow-hidden transition-all duration-300 ${sidebarOpen ? "ml-64" : "ml-16"}`}>
        {/* Top Bar */}
        <header className="bg-white shadow-sm border-b border-gray-200 px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            {/* Toggle Button — Always visible */}
            <button
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="p-2 rounded-md hover:bg-gray-100 transition lg:hidden"
            >
              {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
            </button>
            <h1 className="text-xl font-semibold text-gray-800">Object Browser</h1>
          </div>
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 transition"
          >
            <Plus size={18} />
            Create Bucket
          </button>
        </header>

        {/* Body */}
        <main className="flex-1 overflow-auto p-6">
          <div className="max-w-7xl mx-auto space-y-8">
            {/* Welcome */}
            {/* <section className="text-center">
              <h2 className="text-2xl font-bold text-indigo-700">Buckets</h2>
              <p className="text-gray-600 mt-2">
                MinIO uses buckets to organize objects. A bucket is similar to a folder or directory in a filesystem, where each bucket can hold an arbitrary number of objects.
              </p>
              <p className="text-indigo-600 mt-4">
                To get started, <Link to="#" className="underline">Create a Bucket</Link>.
              </p>
            </section> */}

            {/* Stats */}
            <section className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
              <StatCard title="Total Buckets" value="12" icon={<Bucket className="h-6 w-6 text-indigo-600" />} bgColor="bg-indigo-50" borderColor="border-indigo-200" />
              <StatCard title="Total Objects" value="3,847" icon={<HardDrive className="h-6 w-6 text-green-600" />} bgColor="bg-green-50" borderColor="border-green-200" />
              <StatCard title="Storage Used" value="2.4 GB" icon={<Upload className="h-6 w-6 text-purple-600" />} bgColor="bg-purple-50" borderColor="border-purple-200" />
              <StatCard title="Recent Actions" value="5" icon={<Trash2 className="h-6 w-6 text-red-600" />} bgColor="bg-red-50" borderColor="border-red-200" />
            </section>

            {/* Quick Access */}
            <section className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4 text-indigo-700">Quick Access</h2>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                <QuickLink to="/buckets" label="Manage Buckets" description="Create, delete, and browse buckets" icon={<Bucket className="h-10 w-10 text-indigo-600" />} />
                <QuickLink to="/buckets" label="Upload Files" description="Add new objects to any bucket" icon={<Upload className="h-10 w-10 text-green-600" />} />
                <QuickLink to="/" label="View Activity" description="See recent uploads and deletions" icon={<HardDrive className="h-10 w-10 text-purple-600" />} />
              </div>
            </section>

          </div>
        </main>
      </div>

      {/* Modal */}
      {showCreateModal && <CreateBucketModal onClose={() => setShowCreateModal(false)} />}
    </div>
  );
}

// Reusable components
function StatCard({ title, value, icon, bgColor, borderColor }: { title: string; value: string; icon: React.ReactNode; bgColor: string; borderColor: string; }) {
  return (
    <div className={`p-5 rounded-lg border ${borderColor} ${bgColor} shadow-sm`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-800 mt-1">{value}</p>
        </div>
        <div className="p-3 bg-white rounded-full">{icon}</div>
      </div>
    </div>
  );
}


function QuickLink({ to, label, description, icon }: { to: string; label: string; description: string; icon: React.ReactNode; }) {
  return (
    <Link to={to} className="block p-5 bg-gray-50 hover:bg-gray-100 rounded-lg border border-gray-200 transition-all duration-200 hover:shadow-md text-left group">
      <div className="flex items-start gap-4">
        <div className="p-3 bg-white rounded-lg shadow-sm group-hover:scale-110 transition-transform">{icon}</div>
        <div>
          <h3 className="font-semibold text-gray-800 group-hover:text-indigo-700">{label}</h3>
          <p className="text-sm text-gray-600 mt-1">{description}</p>
        </div>
      </div>
    </Link>
  );
}

function CreateBucketModal({ onClose }: { onClose: () => void }) {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-md w-full p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Create Bucket</h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">×</button>
        </div>
        <p className="text-sm text-gray-600">Feature coming soon...</p>
        <div className="mt-6 flex justify-end">
          <button onClick={onClose} className="px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700">Close</button>
        </div>
      </div>
    </div>
  );
}