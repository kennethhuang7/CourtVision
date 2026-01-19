import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, Trash2, CheckCheck, X, Inbox } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import {
  useAllNotifications,
  useMarkAllNotificationsAsRead,
  useMarkNotificationAsRead,
  useDeleteNotification,
  useDeleteAllNotifications
} from '@/hooks/useNotifications';
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
import { logger } from '@/lib/logger';

export default function Notifications() {
  const navigate = useNavigate();
  const [clearAllDialogOpen, setClearAllDialogOpen] = useState(false);

  const { data, isLoading, isError } = useAllNotifications(0, 100);
  const markAllAsRead = useMarkAllNotificationsAsRead();
  const markAsRead = useMarkNotificationAsRead();
  const deleteNotification = useDeleteNotification();
  const deleteAllNotifications = useDeleteAllNotifications();

  const notifications = data?.data || [];
  const unreadCount = notifications.filter(n => !n.read).length;

  const handleMarkAllAsRead = async () => {
    try {
      await markAllAsRead.mutateAsync();
      toast.success('All notifications marked as read');
    } catch (error) {
      toast.error('Failed to mark notifications as read');
      logger.error('Error marking all notifications as read', error as Error);
    }
  };

  const handleClearAll = async () => {
    try {
      await deleteAllNotifications.mutateAsync();
      toast.success('All notifications cleared');
      setClearAllDialogOpen(false);
    } catch (error) {
      toast.error('Failed to clear notifications');
      logger.error('Error clearing all notifications', error as Error);
    }
  };

  const handleDeleteNotification = async (id: string) => {
    try {
      await deleteNotification.mutateAsync(id);
      toast.success('Notification deleted');
    } catch (error) {
      toast.error('Failed to delete notification');
      logger.error('Error deleting notification', error as Error);
    }
  };

  const handleNotificationClick = (notification: typeof notifications[0]) => {
    
    if (!notification.read) {
      markAsRead.mutate(notification.id);
    }

    
    if (notification.action_url) {
      navigate(notification.action_url);
    }
  };

  
  if (isLoading) {
    return (
      <div className="space-y-6">
        <div>
          <div className="h-8 w-48 bg-muted animate-pulse rounded mb-2" />
          <div className="h-4 w-64 bg-muted animate-pulse rounded" />
        </div>

        <div className="flex flex-wrap gap-3">
          <div className="h-10 w-40 bg-muted animate-pulse rounded" />
          <div className="h-10 w-32 bg-muted animate-pulse rounded" />
        </div>

        <div className="space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="stat-card p-4">
              <div className="flex items-start gap-3">
                <div className="h-10 w-10 bg-muted animate-pulse rounded-full shrink-0" />
                <div className="flex-1 space-y-2">
                  <div className="h-5 w-3/4 bg-muted animate-pulse rounded" />
                  <div className="h-4 w-full bg-muted animate-pulse rounded" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded" />
                </div>
                <div className="h-8 w-8 bg-muted animate-pulse rounded" />
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  
  if (isError) {
    return (
      <div className="flex h-[calc(100vh-10rem)] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <Bell className="h-16 w-16 text-destructive" />
          <p className="text-destructive">Error loading notifications. Please try again.</p>
        </div>
      </div>
    );
  }

  
  if (notifications.length === 0) {
    return (
      <div className="flex h-[calc(100vh-10rem)] items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <Inbox className="h-16 w-16 text-muted-foreground" />
          <p className="text-muted-foreground">No notifications yet.</p>
          <p className="text-sm text-muted-foreground">You'll see notifications here when you have them.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      
      <div className="min-w-0">
        <h1 className="text-3xl font-bold leading-tight truncate">Notifications</h1>
        <p className="text-muted-foreground mt-1 leading-tight truncate">
          {unreadCount > 0
            ? `${unreadCount} unread notification${unreadCount === 1 ? '' : 's'}`
            : 'All caught up!'}
        </p>
      </div>

      
      <div className="flex flex-wrap gap-3">
        {unreadCount > 0 && (
          <Button
            onClick={handleMarkAllAsRead}
            disabled={markAllAsRead.isPending}
            variant="outline"
            className="gap-2"
          >
            <CheckCheck className="h-4 w-4" />
            {markAllAsRead.isPending ? 'Marking...' : 'Mark all as read'}
          </Button>
        )}
        <Button
          onClick={() => setClearAllDialogOpen(true)}
          disabled={deleteAllNotifications.isPending}
          variant="outline"
          className="gap-2 text-destructive hover:text-destructive hover:bg-destructive/10"
        >
          <Trash2 className="h-4 w-4" />
          {deleteAllNotifications.isPending ? 'Clearing...' : 'Clear all'}
        </Button>
      </div>

      
      <div className="space-y-3">
        {notifications.map((notification) => (
          <div
            key={notification.id}
            className={cn(
              'stat-card transition-all hover:shadow-md border-l-4',
              !notification.read ? 'border-l-primary bg-primary/5' : 'border-l-transparent'
            )}
          >
            <div className="flex items-start gap-3">
              
              <div className={cn(
                "flex h-10 w-10 shrink-0 items-center justify-center rounded-full",
                !notification.read ? "bg-primary/20" : "bg-muted"
              )}>
                <Bell className={cn(
                  "h-5 w-5",
                  !notification.read ? "text-primary" : "text-muted-foreground"
                )} />
              </div>

              
              <div
                className="flex-1 min-w-0 cursor-pointer"
                onClick={() => handleNotificationClick(notification)}
              >
                <div className="flex items-start justify-between gap-2 mb-1">
                  <h3 className={cn(
                    "text-base font-medium truncate",
                    !notification.read ? "text-foreground" : "text-muted-foreground"
                  )}>
                    {notification.title}
                  </h3>
                  {!notification.read && (
                    <div className="h-2 w-2 rounded-full bg-primary shrink-0 mt-2" />
                  )}
                </div>
                <p className={cn(
                  "text-sm line-clamp-2 mb-2",
                  !notification.read ? "text-foreground" : "text-muted-foreground"
                )}>
                  {notification.message}
                </p>
                <p className="text-xs text-muted-foreground">
                  {formatDistanceToNow(new Date(notification.created_at), { addSuffix: true })}
                </p>
              </div>

              
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleDeleteNotification(notification.id);
                }}
                disabled={deleteNotification.isPending}
                className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                title="Delete notification"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>
        ))}
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
