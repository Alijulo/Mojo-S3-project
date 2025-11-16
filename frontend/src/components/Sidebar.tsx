// components/Sidebar.tsx
import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  ChevronDown,
  ChevronRight,
  Home,
  Key,
  FileText,
  Shield,
  Users,
  Activity,
  Settings,
  HelpCircle,
  Server,
  createLucideIcon,
  Heart,
  Menu
} from "lucide-react";

import { bucket } from "@lucide/lab";

const Bucket = createLucideIcon("Bucket", bucket);
interface SidebarProps {
  isOpen: boolean;
  onToggle: () => void;
}

export default function Sidebar({ isOpen, onToggle }: SidebarProps) {
  const [userOpen, setUserOpen] = useState(true);
  const [adminOpen, setAdminOpen] = useState(true);
  const location = useLocation();

  const isActive = (path: string) => location.pathname === path;

  return (
    <aside
      className={`fixed left-0 h-screen bg-indigo-600 text-white transition-all duration-300 flex flex-col z-40 ${
        isOpen ? "w-64" : "w-16"
      }`}
      style={{ top: "64px" }}  // ← STARTS BELOW NAVBAR (h-16 = 64px)
    >
      {/* Header */}
      <div className="p-4 flex items-center justify-between border-b border-indigo-700">
        <div className={`flex items-center ${isOpen ? "block" : "hidden"}`}>
          {/* <Bucket className="h-8 w-8 mr-2" /> */}
          <h2 className="font-bold text-lg">S3 SERVER</h2>
        </div>
        <button
          onClick={onToggle}
          className="p-1 hover:bg-indigo-700 rounded transition"
          aria-label="Toggle sidebar"
        >
          <Menu size={20} />
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto mt-4 px-2">
        {/* USER SECTION */}
        <SectionHeader
          label="User"
          isOpen={userOpen}
          onToggle={() => setUserOpen(!userOpen)}
          isSidebarOpen={isOpen}
        />
        {userOpen && (
          <div className="space-y-1">
            <NavItem icon={<Bucket size={18} />} label="Buckets" path="/" active={isActive("/")} isSidebarOpen={isOpen} />
            <NavItem icon={<Key size={18} />} label="Access Keys" path="/access-keys" isSidebarOpen={isOpen} />
            <NavItem icon={<FileText size={18} />} label="Documentation" path="/docs" isSidebarOpen={isOpen} />
          </div>
        )}

        {/* ADMINISTRATOR SECTION */}
        <SectionHeader
          label="Administrator"
          isOpen={adminOpen}
          onToggle={() => setAdminOpen(!adminOpen)}
          isSidebarOpen={isOpen}
        />
        {adminOpen && (
          <div className="space-y-1">
            <NavItem icon={<Bucket size={18} />} label="Manage Buckets" path="/buckets" active={isActive("/buckets")} isSidebarOpen={isOpen} />
            <NavItem icon={<Shield size={18} />} label="Policies" path="/policies" isSidebarOpen={isOpen} />
            <NavItem icon={<Users size={18} />} label="Identity" path="/identity" isSidebarOpen={isOpen} />
            <NavItem icon={<Activity size={18} />} label="Monitoring" path="/monitoring" isSidebarOpen={isOpen} />
            <NavItem icon={<Server size={18} />} label="Events" path="/events" isSidebarOpen={isOpen} />
            <NavItem icon={<Settings size={18} />} label="Configuration" path="/config" isSidebarOpen={isOpen} />
          </div>
        )}

        {/* SUBNET SECTION */}
        <SectionHeader label="Subnet" isOpen={true} isSidebarOpen={isOpen} />
        <div className="space-y-1">
          <NavItem icon={<Heart size={18} />} label="Health" path="/health" isSidebarOpen={isOpen} />
        </div>
      </nav>

      {/* Footer */}
      <div className="p-3 border-t border-indigo-700 text-xs text-indigo-200">
        <span className={`${isOpen ? "block" : "hidden"}`}>AGPLv3 License</span>
      </div>
    </aside>
  );
}

// Reusable Section Header
function SectionHeader({
  label,
  isOpen,
  onToggle,
  isSidebarOpen,
}: {
  label: string;
  isOpen: boolean;
  onToggle?: () => void;
  isSidebarOpen: boolean;
}) {
  return (
    <div
      className={`flex items-center justify-between px-3 py-2 text-xs font-semibold text-indigo-200 uppercase tracking-wider ${
        onToggle ? "cursor-pointer hover:text-white" : ""
      }`}
      onClick={onToggle}
    >
      <span className={`${isSidebarOpen ? "block" : "hidden"}`}>{label}</span>
      {onToggle && isSidebarOpen && (
        <button className="p-1">
          {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>
      )}
    </div>
  );
}

// Reusable Nav Item
function NavItem({
  icon,
  label,
  path,
  active,
  isSidebarOpen,
}: {
  icon: React.ReactNode;
  label: string;
  path: string;
  active?: boolean;
  isSidebarOpen: boolean;
}) {
  return (
    <Link
      to={path}
      className={`flex items-center gap-3 px-3 py-2 rounded-md transition ${
        active ? "bg-indigo-700 text-white" : "hover:bg-indigo-700 text-indigo-100"
      }`}
    >
      {icon}
      <span className={`${isSidebarOpen ? "block" : "hidden"} text-sm`}>
        {label}
      </span>
    </Link>
  );
}
