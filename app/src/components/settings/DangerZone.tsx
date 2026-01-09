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

      
      const tablesToDelete = [
        'user_notifications',
        'user_sessions',
        'user_messages',
        'message_reactions',
        'user_conversations',
        'pick_user_shares',
        'pick_group_shares',
        'user_picks',
        'user_group_invites',
        'user_group_members',
        'user_groups',
        'user_friendships',
        'user_profiles',
      ];

      for (const table of tablesToDelete) {
        try {
          
          if (table === 'user_friendships') {
            
            await supabase
              .from(table)
              .delete()
              .or(`user_id.eq.${user.id},friend_id.eq.${user.id}`);
          } else if (table === 'user_groups') {
            
            await supabase
              .from(table)
              .delete()
              .eq('owner_id', user.id);
          } else if (table === 'user_picks') {
            
            await supabase
              .from(table)
              .delete()
              .eq('owner_id', user.id);
          } else if (table === 'user_profiles') {
            
            await supabase
              .from(table)
              .delete()
              .eq('id', user.id);
          } else {
            
            await supabase
              .from(table)
              .delete()
              .eq('user_id', user.id);
          }
        } catch (error) {
          logger.warn(`Error deleting from ${table}`, error);
          
        }
      }

      
      
      

      
      try {
        const { error: deleteAuthError } = await supabase.rpc('delete_user_account', {
          user_id_to_delete: user.id
        });

        if (deleteAuthError) {
          logger.error('Error deleting auth user via RPC', deleteAuthError);
          
          await logout();
          toast.success('Account data deleted. Please contact support to complete account deletion.');
          navigate('/login');
          return;
        }
      } catch (rpcError) {
        logger.warn('RPC function not available, signing out user', rpcError);
        
        await logout();
        toast.success('Account data deleted. Please contact support to complete account deletion.');
        navigate('/login');
        return;
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
    <div className="rounded-lg border-2 border-destructive/50 bg-destructive/5 p-6">
      <div className="space-y-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="h-6 w-6 text-destructive shrink-0 mt-0.5" />
          <div className="space-y-1">
            <h3 className="text-lg font-semibold text-destructive">Danger Zone</h3>
            <p className="text-sm text-foreground">
              Once you delete your account, there is no going back. Please be certain.
            </p>
          </div>
        </div>

        <AlertDialog open={open} onOpenChange={setOpen}>
          <AlertDialogTrigger asChild>
            <Button variant="destructive" className="w-full sm:w-auto">
              Delete My Account
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
                    <li>Profile information (username, bio, avatar, banner)</li>
                    <li>All your picks (saved and active)</li>
                    <li>All your messages and conversations</li>
                    <li>All friendships and friend requests</li>
                    <li>All group memberships and owned groups</li>
                    <li>All notification history</li>
                    <li>All active sessions</li>
                    <li>Your authentication credentials</li>
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
  );
}
