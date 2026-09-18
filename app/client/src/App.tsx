import { createBrowserRouter, RouterProvider, NavLink, Outlet, useLocation } from 'react-router';
import { useEffect, useState } from 'react';
import { Badge, Button, Sheet, SheetContent, SheetHeader, SheetTitle, useIsMobile } from '@databricks/appkit-ui/react';
import { Menu, Users, Store } from 'lucide-react';
import { MemberSearch } from './pages/MemberSearch';
import { Member360 } from './pages/Member360';
import { Stores } from './pages/Stores';
import { useApi } from './lib/api';
import { ymd, int } from './lib/format';
import type { Meta } from './lib/types';

interface NavItem {
  to: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}

const NAV: NavItem[] = [
  { to: '/', label: 'Member Search', icon: Users },
  { to: '/stores', label: 'Store Overview', icon: Store },
];

function navLinkClass({ isActive }: { isActive: boolean }) {
  return `flex items-center gap-2.5 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
    isActive
      ? 'bg-primary text-primary-foreground'
      : 'text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-foreground'
  }`;
}

/** Simple arches-style wordmark (no real McDonald's assets). */
function ArchesMark() {
  return (
    <span
      aria-hidden
      className="inline-flex h-7 w-8 items-center justify-center rounded-md font-black text-lg leading-none"
      style={{ color: '#292929', background: 'var(--sidebar-primary)' }}
    >
      M
    </span>
  );
}

function NavLinks({ onClick }: { onClick?: () => void }) {
  return (
    <nav className="flex flex-col gap-1">
      {NAV.map(({ to, label, icon: Icon }) => (
        <NavLink key={to} to={to} end={to === '/'} className={navLinkClass} onClick={onClick}>
          <Icon className="h-4 w-4 shrink-0" />
          {label}
        </NavLink>
      ))}
    </nav>
  );
}

function Brand() {
  return (
    <div className="px-2 pb-5">
      <div className="flex items-center gap-2.5 text-sidebar-foreground font-semibold">
        <ArchesMark />
        <div className="leading-tight">
          <div className="text-sm">McDonald&apos;s Philippines</div>
          <div className="text-[0.72rem] text-sidebar-foreground/60 font-normal">Customer 360</div>
        </div>
      </div>
    </div>
  );
}

function FreshnessBadge() {
  const { data } = useApi<Meta>('/api/meta', 60000);
  if (!data) return null;
  return (
    <div className="mt-auto pt-4 text-xs text-sidebar-foreground/60 space-y-1.5">
      <Badge
        variant="outline"
        className="font-normal border-sidebar-border text-sidebar-foreground/80"
      >
        Lakebase · {data.backend}
      </Badge>
      <div>
        {data.members != null ? `${int(data.members)} members · ` : ''}
        {data.stores != null ? `${int(data.stores)} stores` : ''}
      </div>
      <div>{data.refreshed_at ? `synced ${ymd(data.refreshed_at)}` : 'no sync data'}</div>
    </div>
  );
}

function Layout() {
  const isMobile = useIsMobile();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const location = useLocation();
  const onMember = location.pathname.startsWith('/members/');
  const title = onMember
    ? 'Customer 360'
    : (NAV.find((n) => (n.to === '/' ? location.pathname === '/' : location.pathname.startsWith(n.to)))?.label ??
      'Customer 360');

  useEffect(() => {
    // Close the mobile nav when switching to a desktop viewport.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (!isMobile) setMobileNavOpen(false);
  }, [isMobile]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Desktop sidebar */}
      <aside className="hidden md:flex fixed inset-y-0 left-0 w-60 flex-col border-r border-sidebar-border bg-sidebar px-3 py-5">
        <Brand />
        <NavLinks />
        <FreshnessBadge />
      </aside>

      <div className="md:pl-60">
        <header className="border-b px-4 md:px-8 py-3 flex items-center gap-3 sticky top-0 bg-background/90 backdrop-blur z-10">
          <div className="md:hidden">
            <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
              <Button variant="ghost" size="icon" onClick={() => setMobileNavOpen(true)}>
                <Menu className="h-5 w-5" />
                <span className="sr-only">Open navigation</span>
              </Button>
              <SheetContent side="left">
                <SheetHeader>
                  <SheetTitle>McDonald&apos;s Philippines — Customer 360</SheetTitle>
                </SheetHeader>
                <div className="p-4">
                  <NavLinks onClick={() => setMobileNavOpen(false)} />
                </div>
              </SheetContent>
            </Sheet>
          </div>
          <div>
            <h1 className="text-xl font-semibold leading-tight">
              McDonald&apos;s Philippines — {title}
            </h1>
            <div className="text-xs text-muted-foreground">GADC · Workshop demo (synthetic data)</div>
          </div>
        </header>
        <main className="p-4 md:p-8 max-w-[1400px]">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <MemberSearch /> },
      { path: '/members/:id', element: <Member360 /> },
      { path: '/stores', element: <Stores /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
