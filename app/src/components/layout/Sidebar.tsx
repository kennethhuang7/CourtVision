import { NavLink, useLocation } from 'react-router-dom';
import { useState, useEffect } from 'react';
import {
  LayoutDashboard,
  Users,
  BarChart3,
  Settings,
  LogOut,
  Trophy,
  UserPlus,
  UsersRound,
  Globe,
  MessageSquare,
  Bell,
  BellOff,
  Activity,
  Target,
  Flame,
  HelpCircle,
  PanelLeftClose,
  ChevronRight
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useAuth } from '@/contexts/AuthContext';
import { useUserProfile } from '@/hooks/useUserProfile';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { cn, getInitials } from '@/lib/utils';
import { useChatWindow } from '@/contexts/ChatWindowContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { SettingsModal } from '@/components/settings/SettingsModal';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

function ChatWindowToggle({ isCollapsed }: { isCollapsed?: boolean }) {
  const { isVisible, toggle } = useChatWindow();

  const button = (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        "h-8 w-8 p-0 flex-shrink-0",
        isVisible && "bg-primary/10 text-primary hover:bg-primary/20"
      )}
      onClick={toggle}
    >
      <MessageSquare className="h-4 w-4" />
    </Button>
  );

  if (isCollapsed) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>{button}</TooltipTrigger>
        <TooltipContent side="right">{isVisible ? "Hide messages" : "Show messages"}</TooltipContent>
      </Tooltip>
    );
  }

  return button;
}

function DoNotDisturbToggle({ isCollapsed }: { isCollapsed?: boolean }) {
  const { isEnabled: doNotDisturb, disable, enable } = useDoNotDisturb();

  const handleToggle = () => {
    if (doNotDisturb) {
      disable();
    } else {
      enable();
    }
  };

  const button = (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        "h-8 w-8 p-0 flex-shrink-0",
        doNotDisturb && "text-muted-foreground/50"
      )}
      onClick={handleToggle}
    >
      {doNotDisturb ? <BellOff className="h-4 w-4" /> : <Bell className="h-4 w-4" />}
    </Button>
  );

  if (isCollapsed) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>{button}</TooltipTrigger>
        <TooltipContent side="right">
          {doNotDisturb ? "Enable notifications" : "Disable notifications"}
        </TooltipContent>
      </Tooltip>
    );
  }

  return button;
}

const mainNavigation = [
  { name: 'Predictions', href: '/dashboard', icon: LayoutDashboard },
  { name: 'Pick Finder', href: '/dashboard/pick-finder', icon: Target },
  { name: 'Player Analysis', href: '/dashboard/player-analysis', icon: Users },
  { name: 'My Picks', href: '/dashboard/saved-picks', icon: Trophy },
  { name: 'Trends', href: '/dashboard/trends', icon: Flame },
];

const socialNavigation = [
  { name: 'Community', href: '/dashboard/community', icon: Globe },
  { name: 'Messages', href: '/dashboard/messages', icon: MessageSquare },
  { name: 'My Friends', href: '/dashboard/friends', icon: UserPlus },
  { name: 'My Groups', href: '/dashboard/groups', icon: UsersRound },
];

const insightsNavigation = [
  { name: 'Model Performance', href: '/dashboard/model-performance', icon: BarChart3 },
  { name: 'Analytics', href: '/dashboard/analytics', icon: Activity },
  { name: 'How It Works', href: '/dashboard/how-it-works', icon: HelpCircle },
];

export function Sidebar() {
  const location = useLocation();
  const { logout, user } = useAuth();
  const { data: profile } = useUserProfile();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(() => {
    const saved = localStorage.getItem('sidebar-collapsed');
    return saved === 'true';
  });

  useEffect(() => {
    localStorage.setItem('sidebar-collapsed', String(isCollapsed));
    // Dispatch custom event for DashboardLayout to listen to
    window.dispatchEvent(new CustomEvent('sidebar-collapse', { detail: { isCollapsed } }));
  }, [isCollapsed]);

  const displayName = profile?.display_name || profile?.username || user?.username || 'User';
  const username = profile?.username || user?.username || 'user';
  const profilePictureUrl = profile?.profile_picture_url;

  const renderNavItems = (items: typeof mainNavigation) => {
    return items.map((item) => {
      const isActive = location.pathname === item.href ||
        (item.href !== '/dashboard' && location.pathname.startsWith(item.href));

      const navLink = (
        <NavLink
          key={item.name}
          to={item.href}
          className={() => cn(
            'group relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-all duration-200',
            'hover:bg-accent/50',
            isActive
              ? 'bg-primary/10 text-foreground'
              : 'text-muted-foreground hover:text-foreground',
            isCollapsed && 'justify-center px-2'
          )}
        >
          {/* Active indicator bar */}
          {isActive && (
            <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-6 bg-primary rounded-r-full" />
          )}
          <item.icon className={cn(
            'h-5 w-5 shrink-0 transition-all duration-200',
            isActive ? 'text-primary' : 'text-muted-foreground group-hover:text-foreground'
          )} />
          {!isCollapsed && (
            <span className={cn(
              'font-medium truncate min-w-0 transition-colors duration-200',
              isActive ? 'text-foreground' : 'text-muted-foreground group-hover:text-foreground'
            )}>
              {item.name}
            </span>
          )}
        </NavLink>
      );

      if (isCollapsed) {
        return (
          <Tooltip key={item.name}>
            <TooltipTrigger asChild>{navLink}</TooltipTrigger>
            <TooltipContent side="right" sideOffset={10}>
              {item.name}
            </TooltipContent>
          </Tooltip>
        );
      }

      return navLink;
    });
  };

  const settingsButton = (
    <Button
      variant="ghost"
      size="sm"
      className={cn(
        "h-8 w-8 p-0 flex-shrink-0",
        settingsOpen && "bg-primary/10 text-primary hover:bg-primary/20"
      )}
      onClick={() => setSettingsOpen(true)}
    >
      <Settings className="h-4 w-4" />
    </Button>
  );

  return (
    <aside className={cn(
      "fixed left-0 top-10 z-40 h-[calc(100vh-2.5rem)] border-r border-border/50 bg-sidebar/95 backdrop-blur-md overflow-hidden transition-all duration-300 ease-in-out",
      isCollapsed ? "w-[4.5rem]" : "w-64"
    )}>
      <div className="flex h-full flex-col">
        {/* Header */}
        <div className={cn(
          "flex items-center border-b border-sidebar-border/50 shrink-0 transition-all duration-300",
          isCollapsed ? "flex-col gap-2 px-3 py-3" : "px-4 py-2 gap-3 min-h-[3.5rem]"
        )}>
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg overflow-hidden shadow-lg shadow-primary/10">
            <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
          </div>
          {isCollapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0 text-muted-foreground hover:text-foreground hover:bg-accent/50"
                  onClick={() => setIsCollapsed(false)}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right">Expand sidebar</TooltipContent>
            </Tooltip>
          ) : (
            <>
              <div className="overflow-hidden flex-1 min-w-0">
                <h1 className="text-lg font-bold text-foreground truncate leading-tight">CourtVision</h1>
                <p className="text-xs text-muted-foreground/80 truncate leading-tight">NBA Player Predictions</p>
              </div>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-muted-foreground hover:text-foreground shrink-0"
                    onClick={() => setIsCollapsed(true)}
                  >
                    <PanelLeftClose className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="right">Collapse sidebar</TooltipContent>
              </Tooltip>
            </>
          )}
        </div>

        {/* Navigation */}
        <nav className={cn(
          "flex-1 py-4 overflow-y-auto transition-all duration-300",
          isCollapsed ? "px-2" : "px-3"
        )}>
          <div className="space-y-1 mb-3">
            {renderNavItems(mainNavigation)}
          </div>

          <div className={cn(
            "my-3 border-t border-sidebar-border/40",
            isCollapsed && "mx-2"
          )} />

          <div className="mb-3">
            {!isCollapsed && (
              <div className="px-3 py-2">
                <h3 className="text-[11px] font-semibold text-muted-foreground/60 uppercase tracking-wider">Social</h3>
              </div>
            )}
            <div className="space-y-1">
              {renderNavItems(socialNavigation)}
            </div>
          </div>

          <div className={cn(
            "my-3 border-t border-sidebar-border/40",
            isCollapsed && "mx-2"
          )} />

          <div>
            {!isCollapsed && (
              <div className="px-3 py-2">
                <h3 className="text-[11px] font-semibold text-muted-foreground/60 uppercase tracking-wider">Insights</h3>
              </div>
            )}
            <div className="space-y-1">
              {renderNavItems(insightsNavigation)}
            </div>
          </div>
        </nav>

        {/* Footer */}
        <div className={cn(
          "border-t border-sidebar-border/40 shrink-0 bg-gradient-to-t from-background/30 to-transparent transition-all duration-300",
          isCollapsed ? "p-2" : "p-3"
        )}>
          {/* User section */}
          {isCollapsed ? (
            <div className="flex flex-col items-center gap-2">
              <Tooltip>
                <TooltipTrigger asChild>
                  <Avatar className="h-9 w-9 shrink-0 ring-2 ring-primary/20 shadow-lg shadow-primary/10 cursor-pointer">
                    {profilePictureUrl ? (
                      <AvatarImage src={profilePictureUrl} alt={displayName} />
                    ) : null}
                    <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                      {getInitials(displayName)}
                    </AvatarFallback>
                  </Avatar>
                </TooltipTrigger>
                <TooltipContent side="right">
                  <div className="text-sm font-medium">{displayName}</div>
                  <div className="text-xs text-muted-foreground">@{username}</div>
                </TooltipContent>
              </Tooltip>
              <div className="flex flex-col gap-1">
                <ChatWindowToggle isCollapsed />
                <DoNotDisturbToggle isCollapsed />
                <Tooltip>
                  <TooltipTrigger asChild>{settingsButton}</TooltipTrigger>
                  <TooltipContent side="right">Settings</TooltipContent>
                </Tooltip>
              </div>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 w-8 p-0 text-destructive hover:bg-destructive/10 hover:text-destructive"
                    onClick={logout}
                  >
                    <LogOut className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="right">Logout</TooltipContent>
              </Tooltip>
            </div>
          ) : (
            <>
              <div className="mb-3 flex items-center gap-2">
                <div className="flex items-center gap-2 min-w-0 flex-1">
                  <Avatar className="h-9 w-9 shrink-0 ring-2 ring-primary/20 shadow-lg shadow-primary/10">
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
                    <p className="truncate text-xs text-muted-foreground/80 leading-tight">
                      @{username}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-1 shrink-0">
                  <ChatWindowToggle />
                  <DoNotDisturbToggle />
                  {settingsButton}
                </div>
              </div>
              <button
                onClick={logout}
                className="w-full flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm text-destructive hover:bg-destructive/10 transition-colors duration-200"
              >
                <LogOut className="h-5 w-5 shrink-0" />
                <span className="font-medium truncate min-w-0">Logout</span>
              </button>
            </>
          )}
        </div>
      </div>

      <SettingsModal open={settingsOpen} onClose={() => setSettingsOpen(false)} />
    </aside>
  );
}
