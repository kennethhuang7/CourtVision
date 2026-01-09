import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, BellOff, HelpCircle, Minus, Square, X, Settings, LogOut, Check, ChevronRight, Inbox, Trash2 } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { useUserProfile } from '@/hooks/useUserProfile';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials, cn } from '@/lib/utils';
import { useDoNotDisturb, type SuppressionDuration } from '@/contexts/DoNotDisturbContext';
import { useRecentNotifications, useUnreadNotificationCount, useMarkNotificationAsRead, useMarkAllNotificationsAsRead, useDeleteNotification, useDeleteAllNotifications } from '@/hooks/useNotifications';
import { formatDistanceToNow } from 'date-fns';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from '@/components/ui/dropdown-menu';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { toast } from 'sonner';

export function TitleBar() {
  const { user, logout } = useAuth();
  const { data: profile } = useUserProfile();
  const navigate = useNavigate();
  const [isMaximized, setIsMaximized] = useState(false);
  const [clearAllDialogOpen, setClearAllDialogOpen] = useState(false);

  
  const { data: recentNotifications = [] } = useRecentNotifications(10);
  const { data: unreadCount = 0 } = useUnreadNotificationCount();
  const markAsRead = useMarkNotificationAsRead();
  const markAllAsRead = useMarkAllNotificationsAsRead();
  const deleteNotification = useDeleteNotification();
  const deleteAllNotifications = useDeleteAllNotifications();

  
  const displayName = profile?.display_name || profile?.username || user?.username || 'User';
  const profilePictureUrl = profile?.profile_picture_url;

  useEffect(() => {
    
    if (window.electron?.isMaximized) {
      window.electron.isMaximized().then(setIsMaximized);
    }

    
    const cleanupMaximize = window.electron?.onMaximize?.(() => setIsMaximized(true));
    const cleanupRestore = window.electron?.onRestore?.(() => setIsMaximized(false));

    return () => {
      cleanupMaximize?.();
      cleanupRestore?.();
    };
  }, []);

  const handleMinimize = () => {
    if (window.electron?.minimize) {
      window.electron.minimize();
    }
  };

  const handleMaximize = () => {
    if (isMaximized) {
      if (window.electron?.restore) {
        window.electron.restore();
      }
    } else {
      if (window.electron?.maximize) {
        window.electron.maximize();
      }
    }
  };

  const handleClose = () => {
    if (window.electron?.close) {
      window.electron.close();
    }
  };

  
  const suppressionOptions: { minutes: SuppressionDuration; label: string }[] = [
    { minutes: 5, label: '5 minutes' },
    { minutes: 10, label: '10 minutes' },
    { minutes: 15, label: '15 minutes' },
    { minutes: 30, label: '30 minutes' },
    { minutes: 60, label: '1 hour' },
    { minutes: 120, label: '2 hours' },
    { minutes: 240, label: '4 hours' },
    { minutes: 480, label: '8 hours' },
    { minutes: 1440, label: '24 hours' },
  ];

  const {
    isEnabled: doNotDisturb,
    isPermanent,
    activeSuppressionDuration,
    enable,
    disable,
    suppressFor,
    cancelSuppression,
  } = useDoNotDisturb();

  const handlePermanentDND = () => {
    if (doNotDisturb && isPermanent) {
      
      disable();
    } else {
      
      enable();
    }
  };

  const handleSuppressFor = (minutes: SuppressionDuration) => {
    if (doNotDisturb && !isPermanent && activeSuppressionDuration === minutes) {
      
      cancelSuppression();
    } else {
      
      suppressFor(minutes);
    }
  };

  const handleHelp = () => {
    
  };

  const handleClearAll = async () => {
    try {
      await deleteAllNotifications.mutateAsync();
      toast.success('All notifications cleared');
      setClearAllDialogOpen(false);
    } catch (error) {
      toast.error('Failed to clear notifications');
    }
  };

  const handleDeleteNotification = async (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await deleteNotification.mutateAsync(id);
      toast.success('Notification deleted');
    } catch (error) {
      toast.error('Failed to delete notification');
    }
  };


  return (
    <div className="fixed top-0 left-0 right-0 z-50 flex h-10 items-center justify-between bg-background/95 backdrop-blur-sm border-b border-border/30 px-2 select-none" style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}>
      <div className="flex items-center gap-2 pl-2 overflow-hidden flex-1 min-w-0 mr-2">
        <span className="text-sm font-medium text-foreground/80 whitespace-nowrap truncate">CourtVision</span>
      </div>

      <div className="flex items-center gap-1" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button
              className={cn(
                "flex h-8 w-8 items-center justify-center rounded-full transition-colors",
                doNotDisturb 
                  ? "text-muted-foreground/50 hover:text-muted-foreground hover:bg-secondary/50" 
                  : "text-muted-foreground hover:text-foreground hover:bg-secondary/50"
              )}
              title={doNotDisturb ? "Do Not Disturb - Open options" : "Do Not Disturb - Open options"}
            >
              {doNotDisturb ? <BellOff className="h-4 w-4 shrink-0" /> : <Bell className="h-4 w-4 shrink-0" />}
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            align="end"
            className="w-48 bg-card border-border/50 shadow-xl"
            sideOffset={8}
          >
            <DropdownMenuItem
              onClick={handlePermanentDND}
              className="gap-2 cursor-pointer text-sm"
            >
              <span className="flex-1 min-w-0 text-sm truncate">Do Not Disturb</span>
              {doNotDisturb && (
                <Check className="h-3.5 w-3.5 shrink-0 text-primary" />
              )}
            </DropdownMenuItem>
            <DropdownMenuSeparator className="bg-border/50" />
            <DropdownMenuSub>
              <DropdownMenuSubTrigger className="text-sm">
                Suppress notifications
              </DropdownMenuSubTrigger>
              <DropdownMenuSubContent className="w-40">
                {suppressionOptions.map((option) => {
                  const isActive = !isPermanent && activeSuppressionDuration === option.minutes;
                  return (
                    <DropdownMenuItem
                      key={option.minutes}
                      onClick={() => handleSuppressFor(option.minutes)}
                      className="gap-2 cursor-pointer text-sm"
                    >
                      <span className="flex-1 min-w-0 text-sm truncate">{option.label}</span>
                      {isActive && (
                        <Check className="h-3.5 w-3.5 shrink-0 text-primary" />
                      )}
                    </DropdownMenuItem>
                  );
                })}
              </DropdownMenuSubContent>
            </DropdownMenuSub>
          </DropdownMenuContent>
        </DropdownMenu>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button
              className="relative flex h-8 w-8 items-center justify-center rounded-full text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
              title="Notifications"
            >
              <Inbox className="h-4 w-4 shrink-0" />
              {unreadCount > 0 && (
                <span className="absolute -top-1 -right-1 flex h-4 min-w-4 items-center justify-center rounded-full bg-destructive text-[10px] font-bold text-destructive-foreground px-1">
                  {unreadCount > 99 ? '99+' : unreadCount}
                </span>
              )}
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            align="end"
            className="w-96 max-h-[500px] overflow-y-auto bg-card border-border/50 shadow-xl"
            sideOffset={8}
          >
            <div className="flex items-center justify-between px-3 py-2 border-b border-border/50">
              <h3 className="text-sm font-semibold text-foreground">Notifications</h3>
              {recentNotifications.length > 0 && (
                <div className="flex items-center gap-2">
                  {unreadCount > 0 && (
                    <button
                      onClick={() => markAllAsRead.mutate()}
                      className="text-xs text-primary hover:text-primary/80 font-medium"
                    >
                      Mark all read
                    </button>
                  )}
                  <button
                    onClick={() => setClearAllDialogOpen(true)}
                    className="text-xs text-destructive hover:text-destructive/80 font-medium"
                  >
                    Clear all
                  </button>
                </div>
              )}
            </div>

            {recentNotifications.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                No notifications yet
              </div>
            ) : (
              <div className="py-1">
                {recentNotifications.map((notification) => (
                  <div
                    key={notification.id}
                    className={cn(
                      "relative group",
                      !notification.read && "bg-primary/5"
                    )}
                  >
                    <DropdownMenuItem
                      className="flex flex-col items-start gap-1 px-3 py-3 pr-10 cursor-pointer"
                      onClick={() => {
                        markAsRead.mutate(notification.id);
                        
                        if (notification.action_url) {
                          navigate(notification.action_url);
                        }
                      }}
                    >
                      <div className="flex items-start justify-between w-full gap-2">
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-foreground truncate">
                            {notification.title}
                          </p>
                          <p className="text-xs text-muted-foreground line-clamp-2 mt-0.5">
                            {notification.message}
                          </p>
                        </div>
                        {!notification.read && (
                          <div className="h-2 w-2 rounded-full bg-primary shrink-0 mt-1.5" />
                        )}
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {formatDistanceToNow(new Date(notification.created_at), { addSuffix: true })}
                      </p>
                    </DropdownMenuItem>
                    <button
                      onClick={(e) => handleDeleteNotification(notification.id, e)}
                      className="absolute top-1/2 -translate-y-1/2 right-2 h-6 w-6 flex items-center justify-center rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors opacity-0 group-hover:opacity-100"
                      title="Delete notification"
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                ))}
              </div>
            )}

            {recentNotifications.length > 0 && (
              <>
                <DropdownMenuSeparator className="bg-border/50" />
                <DropdownMenuItem
                  className="justify-center text-sm text-primary hover:text-primary/80 cursor-pointer py-2"
                  onClick={() => navigate('/dashboard/notifications')}
                >
                  View all notifications
                </DropdownMenuItem>
              </>
            )}
          </DropdownMenuContent>
        </DropdownMenu>

        <button
          onClick={handleHelp}
          className="flex h-8 w-8 items-center justify-center rounded-full text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          title="Help"
        >
          <HelpCircle className="h-4 w-4 shrink-0" />
        </button>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="ml-1 flex h-8 w-8 shrink-0 items-center justify-center rounded-full ring-2 ring-transparent hover:ring-primary/50 transition-all">
              <Avatar className="h-7 w-7 shrink-0">
                {profilePictureUrl ? (
                  <AvatarImage src={profilePictureUrl} alt={displayName} />
                ) : null}
                <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-xs font-medium text-white">
                  {getInitials(displayName)}
                </AvatarFallback>
              </Avatar>
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent 
            align="end" 
            className="w-48 bg-card border-border/50 shadow-xl text-sm"
            sideOffset={8}
          >
            <DropdownMenuItem
              className="gap-2 cursor-pointer text-sm"
              onClick={() => navigate('/dashboard/settings')}
            >
              <Settings className="h-4 w-4 shrink-0" />
              <span className="text-sm min-w-0 truncate">Settings</span>
            </DropdownMenuItem>
            <DropdownMenuItem className="gap-2 cursor-pointer text-sm">
              <HelpCircle className="h-4 w-4 shrink-0" />
              <span className="text-sm min-w-0 truncate">Help</span>
            </DropdownMenuItem>
            <DropdownMenuSeparator className="bg-border/50" />
            <DropdownMenuItem
              className="gap-2 cursor-pointer text-destructive focus:text-destructive text-sm"
              onClick={logout}
            >
              <LogOut className="h-4 w-4 shrink-0" />
              <span className="text-sm min-w-0 truncate">Log out</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

        <div className="mx-2 h-4 w-px bg-border/30" />

        <button
          onClick={handleMinimize}
          className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          title="Minimize"
        >
          <Minus className="h-4 w-4 shrink-0" />
        </button>

        <button
          onClick={handleMaximize}
          className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          title={isMaximized ? "Restore" : "Maximize"}
        >
          {isMaximized ? (
            <div className="relative h-3.5 w-3.5 shrink-0">
              <div className="absolute bottom-0 left-0 h-2.5 w-2.5 border border-current" />
              <div className="absolute top-0 right-0 h-2.5 w-2.5 border border-current bg-card" />
            </div>
          ) : (
            <Square className="h-3.5 w-3.5 shrink-0" />
          )}
        </button>

        <button
          onClick={handleClose}
          className="flex h-8 w-10 shrink-0 items-center justify-center text-muted-foreground hover:text-white hover:bg-destructive transition-colors"
          title="Close"
        >
          <X className="h-4 w-4 shrink-0" />
        </button>
      </div>

      <AlertDialog open={clearAllDialogOpen} onOpenChange={setClearAllDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Clear all notifications?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete all notifications. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleClearAll}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleteAllNotifications.isPending}
            >
              {deleteAllNotifications.isPending ? 'Clearing...' : 'Clear all'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
