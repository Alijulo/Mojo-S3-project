// components/Navbar.tsx
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useState } from "react";
import { LogOut, Home } from "lucide-react";
import { createLucideIcon } from "lucide-react";
import { bucket } from "@lucide/lab";

const Bucket = createLucideIcon("Bucket", bucket);

export default function Navbar() {
  const location = useLocation();
  const navigate = useNavigate();

  const handleLogout = () => {
    localStorage.removeItem("authToken");
    navigate("/login", { replace: true });
  };

  const isActive = (path: string) => location.pathname === path;

  return (
    <nav className="fixed top-0 left-0 right-0 bg-indigo-600 text-white shadow-lg z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <div className="flex items-center">
            <Bucket className="h-8 w-8 mr-2" />
            <h1 className="text-xl font-bold">Mojo S3 Admin</h1>
          </div>

          {/* Desktop Links */}
          <div className="hidden md:flex items-center space-x-1">
            <NavLink to="/" icon={<Home size={18} />} label="Dashboard" active={isActive("/")} />
            <NavLink to="/buckets" icon={<Bucket className="w-5 h-5" />} label="Buckets" active={isActive("/buckets")} />
            
            <button
              onClick={handleLogout}
              className="flex items-center gap-2 px-4 py-2 rounded-md hover:bg-indigo-700 transition-colors duration-200 text-sm font-medium"
            >
              <LogOut size={18} />
              <span>Logout</span>
            </button>
          </div>

          {/* Mobile Menu */}
          <div className="md:hidden">
            <MobileMenu onLogout={handleLogout} isActive={isActive} />
          </div>
        </div>
      </div>
    </nav>
  );
}

// Reusable Nav Link
function NavLink({ to, icon, label, active }: { to: string; icon: React.ReactNode; label: string; active: boolean }) {
  return (
    <Link
      to={to}
      className={`flex items-center gap-2 px-4 py-2 rounded-md transition-colors duration-200 text-sm font-medium ${
        active ? "bg-indigo-700 text-white" : "hover:bg-indigo-700 hover:text-white"
      }`}
    >
      {icon}
      <span>{label}</span>
    </Link>
  );
}

// Mobile Menu
function MobileMenu({ onLogout, isActive }: { onLogout: () => void; isActive: (path: string) => boolean }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="p-2 rounded-md hover:bg-indigo-700 transition"
      >
        <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={open ? "M6 18L18 6M6 6l12 12" : "M4 6h16M4 12h16M4 18h16"} />
        </svg>
      </button>

      {open && (
        <div className="absolute right-0 mt-2 w-48 bg-indigo-700 rounded-md shadow-lg py-1 z-50">
          <MobileNavLink to="/" label="Dashboard" icon={<Home size={16} />} active={isActive("/")} onClick={() => setOpen(false)} />
          <MobileNavLink to="/buckets" label="Buckets" icon={<Bucket className="w-4 h-4" />} active={isActive("/buckets")} onClick={() => setOpen(false)} />
          <div className="border-t border-indigo-600 my-1"></div>
          <button
            onClick={() => {
              setOpen(false);
              onLogout();
            }}
            className="w-full flex items-center gap-2 px-4 py-2 text-left text-sm hover:bg-indigo-800 transition"
          >
            <LogOut size={16} />
            <span>Logout</span>
          </button>
        </div>
      )}
    </div>
  );
}

function MobileNavLink({ to, label, icon, active, onClick }: { to: string; label: string; icon: React.ReactNode; active: boolean; onClick: () => void }) {
  return (
    <Link
      to={to}
      onClick={onClick}
      className={`flex items-center gap-2 px-4 py-2 text-sm ${active ? "bg-indigo-800" : "hover:bg-indigo-700"} transition`}
    >
      {icon}
      <span>{label}</span>
    </Link>
  );
}