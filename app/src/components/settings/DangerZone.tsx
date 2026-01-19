import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog';
import { Loader2, AlertTriangle } from 'lucide-react';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';
import { useNavigate } from 'react-router-dom';

export function DangerZone() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [isDeleting, setIsDeleting] = useState(false);
  const [confirmPassword, setConfirmPassword] = useState('');
  const [confirmText, setConfirmText] = useState('');
  const [open, setOpen] = useState(false);

  const handleDeleteAccount = async () => {
    if (!user?.id) {
      toast.error('You must be logged in to delete your account');
      return;
    }

    if (!confirmPassword) {
      toast.error('Please enter your password');
      return;
    }

    if (confirmText !== 'DELETE MY ACCOUNT') {
      toast.error('Please type DELETE MY ACCOUNT to confirm');
      return;
    }

    setIsDeleting(true);

    try {
      const { error: signInError } = await supabase.auth.signInWithPassword({
        email: user.email || '',
        password: confirmPassword,
      });

      if (signInError) {
        toast.error('Incorrect password');
        setIsDeleting(false);
        return;
      }

      const { error: deleteError } = await supabase.rpc('delete_user_account', {
        user_id_to_delete: user.id
      });

      if (deleteError) {
        logger.error('Error deleting user account', deleteError);
        toast.error('Failed to delete account. Please try again or contact support.');
        setIsDeleting(false);
        return;
      }

      try {
        const { error: authDeleteError } = await supabase.rpc('delete_auth_user', {
          user_id_to_delete: user.id
        });

        if (authDeleteError) {
          logger.warn('Auth user deletion failed, data deleted', authDeleteError);
        }
      } catch (authError) {
        logger.warn('Auth deletion RPC not available, data deleted', authError);
      }

      await logout();
      toast.success('Your account has been permanently deleted');
      navigate('/login');
    } catch (error) {
      logger.error('Error deleting account', error);
      toast.error('Failed to delete account. Please try again or contact support.');
    } finally {
      setIsDeleting(false);
      setConfirmPassword('');
      setConfirmText('');
      setOpen(false);
    }
  };

  return (
    <div className="rounded-lg border border-border/60 bg-card/50 p-6 relative overflow-hidden">
      {/* Subtle danger accent */}
      <div className="absolute left-0 top-0 bottom-0 w-1 bg-destructive/60" />

      <div className="space-y-4 pl-4">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-destructive/10">
              <AlertTriangle className="h-4 w-4 text-destructive" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-foreground">Delete Account</h3>
              <p className="text-xs text-muted-foreground">
                Permanently delete your account and all data
              </p>
            </div>
          </div>

          <AlertDialog open={open} onOpenChange={setOpen}>
            <AlertDialogTrigger asChild>
              <Button variant="outline" size="sm" className="text-destructive border-destructive/30 hover:bg-destructive/10 hover:border-destructive/50">
                Delete Account
              </Button>
            </AlertDialogTrigger>
          <AlertDialogContent className="max-w-2xl">
            <AlertDialogHeader>
              <AlertDialogTitle className="flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-destructive" />
                Delete Account Permanently
              </AlertDialogTitle>
              <AlertDialogDescription className="space-y-4 pt-4">
                <p className="text-foreground font-medium">
                  This action cannot be undone. This will permanently delete your account and remove all your data from our servers.
                </p>

                <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-4">
                  <p className="text-sm font-medium text-destructive mb-2">The following data will be permanently deleted:</p>
                  <ul className="text-sm text-muted-foreground space-y-1 list-disc list-inside">
                    <li>Profile information (username, bio, avatar, banner, settings)</li>
                    <li>All your picks (and all shares of those picks to others)</li>
                    <li>All your direct messages, group messages, and conversations</li>
                    <li>All friendships and friend requests</li>
                    <li>All groups you own (including all members, messages, and invites)</li>
                    <li>All group memberships and invites</li>
                    <li>All pick shares to you and message reactions</li>
                    <li>All notification history</li>
                    <li>All active sessions</li>
                    <li>Your authentication credentials</li>
                  </ul>
                </div>

                <div className="rounded-lg border border-warning/20 bg-warning/5 p-4 mt-4">
                  <p className="text-sm font-semibold text-foreground mb-2">Impact on others:</p>
                  <ul className="text-sm text-muted-foreground space-y-1 list-disc list-inside">
                    <li>Groups you own will be completely deleted for all members</li>
                    <li>Picks you shared will no longer be accessible to recipients</li>
                    <li>Your messages will be deleted from all conversations</li>
                  </ul>
                </div>

                <div className="space-y-4 pt-2">
                  <div className="space-y-2">
                    <Label htmlFor="confirmPassword" className="text-foreground">
                      Enter your password to confirm
                    </Label>
                    <Input
                      id="confirmPassword"
                      type="password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      placeholder="Enter your password"
                      disabled={isDeleting}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="confirmText" className="text-foreground">
                      Type <strong>DELETE MY ACCOUNT</strong> to confirm
                    </Label>
                    <Input
                      id="confirmText"
                      type="text"
                      value={confirmText}
                      onChange={(e) => setConfirmText(e.target.value)}
                      placeholder="DELETE MY ACCOUNT"
                      disabled={isDeleting}
                    />
                  </div>
                </div>
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel disabled={isDeleting}>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={(e) => {
                  e.preventDefault();
                  handleDeleteAccount();
                }}
                disabled={isDeleting || !confirmPassword || confirmText !== 'DELETE MY ACCOUNT'}
                className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              >
                {isDeleting ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin mr-2" />
                    Deleting...
                  </>
                ) : (
                  'Delete My Account Permanently'
                )}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
        </div>
      </div>
    </div>
  );
}
