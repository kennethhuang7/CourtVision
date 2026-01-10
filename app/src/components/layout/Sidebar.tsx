import { NavLink, useLocation } from 'react-router-dom';
import {
  LayoutDashboard,
  Users,
  BarChart3,
  Settings,
  LogOut,
  TrendingUp,
  Trophy,
  UserPlus,
  UsersRound,
  Globe,
  MessageSquare,
  Bell,
  BellOff,
  Activity,
  Target,
  Flame
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useAuth } from '@/contexts/AuthContext';
import { useUserProfile } from '@/hooks/useUserProfile';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { cn, getInitials } from '@/lib/utils';
import { useChatWindow } from '@/contexts/ChatWindowContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';

function ChatWindowToggle() {
  const { isVisible, toggle } = useChatWindow();

  return (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        "h-8 w-8 p-0 flex-shrink-0",
        isVisible && "bg-primary/10 text-primary hover:bg-primary/20"
      )}
      onClick={toggle}
      title={isVisible ? "Hide messages" : "Show messages"}
    >
      <MessageSquare className="h-4 w-4" />
    </Button>
  );
}

function DoNotDisturbToggle() {
  const { isEnabled: doNotDisturb, disable, enable } = useDoNotDisturb();

  const handleToggle = () => {
    if (doNotDisturb) {
      disable();
    } else {
      enable();
    }
  };

  return (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        "h-8 w-8 p-0 flex-shrink-0",
        doNotDisturb && "text-muted-foreground/50"
      )}
      onClick={handleToggle}
      title={doNotDisturb ? "Do Not Disturb - Click to enable notifications" : "Do Not Disturb - Click to disable notifications"}
    >
      {doNotDisturb ? <BellOff className="h-4 w-4" /> : <Bell className="h-4 w-4" />}
    </Button>
  );
}

const navigation = [
  { name: 'Predictions', href: '/dashboard', icon: LayoutDashboard },
  { name: 'Player Analysis', href: '/dashboard/player-analysis', icon: Users },
  { name: 'Pick Finder', href: '/dashboard/pick-finder', icon: Target },
  { name: 'Trends', href: '/dashboard/trends', icon: Flame },
  { name: 'My Picks', href: '/dashboard/saved-picks', icon: Trophy },
  { name: 'Community', href: '/dashboard/community', icon: Globe },
  { name: 'Messages', href: '/dashboard/messages', icon: MessageSquare },
  { name: 'My Friends', href: '/dashboard/friends', icon: UserPlus },
  { name: 'My Groups', href: '/dashboard/groups', icon: UsersRound },
  { name: 'Model Performance', href: '/dashboard/model-performance', icon: BarChart3 },
  { name: 'Analytics', href: '/dashboard/analytics', icon: Activity },
  { name: 'Settings', href: '/dashboard/settings', icon: Settings },
];

export function Sidebar() {
  const location = useLocation();
  const { logout, user } = useAuth();
  const { data: profile } = useUserProfile();

  
  const displayName = profile?.display_name || profile?.username || user?.username || 'User';
  const username = profile?.username || user?.username || 'user';
  const profilePictureUrl = profile?.profile_picture_url;

  return (
    <aside className="fixed left-0 top-10 z-40 h-[calc(100vh-2.5rem)] w-64 border-r border-border bg-sidebar overflow-hidden">
      <div className="flex h-full flex-col">
        <div className="flex min-h-[3.5rem] items-center gap-3 border-b border-sidebar-border px-6 py-2 shrink-0">
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg overflow-hidden">
            <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
          </div>
          <div className="overflow-hidden flex-1 min-w-0">
            <h1 className="text-lg font-bold text-foreground truncate leading-tight">CourtVision</h1>
            <p className="text-xs text-muted-foreground truncate leading-tight">NBA Player Predictions</p>
          </div>
        </div>

        <nav className="flex-1 space-y-1 px-3 py-4 overflow-y-auto">
          {navigation.map((item) => {
            const isActive = location.pathname === item.href ||
              (item.href !== '/dashboard' && location.pathname.startsWith(item.href));

            return (
              <NavLink
                key={item.name}
                to={item.href}
                className={() => cn(
                  'nav-link group',
                  isActive && 'active'
                )}
              >
                <item.icon className={cn(
                  'h-5 w-5 shrink-0 transition-colors',
                  isActive ? 'text-primary' : 'text-muted-foreground group-hover:text-foreground'
                )} />
                <span className={cn(
                  'font-medium truncate min-w-0',
                  isActive ? 'text-foreground' : 'text-muted-foreground group-hover:text-foreground'
                )}>
                  {item.name}
                </span>
                {isActive && (
                  <TrendingUp className="h-4 w-4 shrink-0 text-primary animate-fade-in ml-auto" />
                )}
              </NavLink>
            );
          })}
        </nav>

        <div className="border-t border-sidebar-border p-4 shrink-0">
          <div className="mb-3 flex items-center gap-2">
            <div className="flex items-center gap-2 min-w-0 flex-1">
              <Avatar className="h-9 w-9 shrink-0">
                {profilePictureUrl ? (
                  <AvatarImage src={profilePictureUrl} alt={displayName} />
                ) : null}
                <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                  {getInitials(displayName)}
                </AvatarFallback>
              </Avatar>
              <div className="flex-1 overflow-hidden min-w-0">
                <p className="truncate text-sm font-medium text-foreground leading-tight">
                  {displayName}
                </p>
                <p className="truncate text-xs text-muted-foreground leading-tight">
                  {username}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-1 shrink-0">
              <ChatWindowToggle />
              <DoNotDisturbToggle />
            </div>
          </div>
          <button
            onClick={logout}
            className="nav-link w-full text-destructive hover:bg-destructive/10 hover:text-destructive"
          >
            <LogOut className="h-5 w-5 shrink-0" />
            <span className="font-medium truncate min-w-0">Logout</span>
          </button>
        </div>
      </div>
    </aside>
  );
}
