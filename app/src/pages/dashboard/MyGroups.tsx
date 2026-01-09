import { useState } from 'react';
import { Users, Plus, Crown, UserMinus, Trash2, Settings, Mail, Check, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { useGroups, useDeleteGroup, useLeaveGroup, useGroupInvites, useAcceptGroupInvite, useDeclineGroupInvite } from '@/hooks/useGroups';
import { useAuth } from '@/contexts/AuthContext';
import { CreateGroupModal } from '@/components/groups/CreateGroupModal';
import { GroupDetailModal } from '@/components/groups/GroupDetailModal';
import { cn, getInitials } from '@/lib/utils';
import { format } from 'date-fns';
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

export default function MyGroups() {
  const { user } = useAuth();
  const { data: groups, isLoading } = useGroups();
  const { data: invites } = useGroupInvites();
  const acceptInvite = useAcceptGroupInvite();
  const declineInvite = useDeclineGroupInvite();
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [deleteGroupId, setDeleteGroupId] = useState<string | null>(null);
  const [leaveGroupId, setLeaveGroupId] = useState<string | null>(null);

  const deleteGroup = useDeleteGroup();
  const leaveGroup = useLeaveGroup();

  const pendingInvites = invites?.filter(inv => inv.invitee_id === user?.id) || [];

  const handleDeleteGroup = async () => {
    if (!deleteGroupId) return;
    try {
      await deleteGroup.mutateAsync(deleteGroupId);
      setDeleteGroupId(null);
    } catch (error) {
      
    }
  };

  const handleLeaveGroup = async () => {
    if (!leaveGroupId) return;
    try {
      await leaveGroup.mutateAsync(leaveGroupId);
      setLeaveGroupId(null);
      setSelectedGroupId(null);
    } catch (error) {
      
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <div className="min-w-0 flex-1">
          <h1 className="text-2xl font-bold text-foreground leading-tight truncate">My Groups</h1>
          <p className="text-sm text-muted-foreground mt-1 leading-tight truncate">
            Create and manage your groups
          </p>
        </div>
        <Button onClick={() => setCreateModalOpen(true)} className="shrink-0">
          <Plus className="h-4 w-4 mr-2 shrink-0" />
          Create Group
        </Button>
      </div>

      {pendingInvites.length > 0 && (
        <div className="stat-card">
          <div className="flex items-center gap-2 mb-4">
            <Mail className="h-5 w-5 text-primary" />
            <h2 className="text-lg font-semibold text-foreground">Pending Invites</h2>
          </div>
          <div className="space-y-2">
            {pendingInvites.map((invite) => {
              const inviter = invite.inviter_profile;
              const group = invite.group;
              if (!inviter || !group) return null;
              const displayName = inviter.display_name || inviter.username;
              return (
                <div key={invite.id} className="flex items-center gap-3 p-4 rounded-lg border border-border">
                  <Avatar className="h-10 w-10">
                    {inviter.profile_picture_url ? (
                      <AvatarImage src={inviter.profile_picture_url} alt={displayName} />
                    ) : null}
                    <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                      {getInitials(displayName)}
                    </AvatarFallback>
                  </Avatar>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-foreground">
                      <span className="font-semibold">{displayName}</span> invited you to join{' '}
                      <span className="font-semibold">{group.name}</span>
                    </p>
                    {group.description && (
                      <p className="text-xs text-muted-foreground truncate mt-1">{group.description}</p>
                    )}
                  </div>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => declineInvite.mutate(invite.id)}
                      disabled={declineInvite.isPending}
                    >
                      <X className="h-4 w-4 mr-1" />
                      Decline
                    </Button>
                    <Button
                      size="sm"
                      onClick={() => acceptInvite.mutate(invite.id)}
                      disabled={acceptInvite.isPending}
                    >
                      <Check className="h-4 w-4 mr-1" />
                      Accept
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="stat-card">
        {isLoading ? (
          <div className="text-center py-8 text-muted-foreground">Loading...</div>
        ) : !groups || groups.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            <Users className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p className="text-lg font-medium mb-2">No groups yet</p>
            <p className="text-sm mb-4">Create your first group to get started!</p>
            <Button onClick={() => setCreateModalOpen(true)}>
              <Plus className="h-4 w-4 mr-2" />
              Create Group
            </Button>
          </div>
        ) : (
          <div className="space-y-2">
            {groups.map((group) => (
              <div
                key={group.id}
                className="flex items-center gap-3 p-4 rounded-lg hover:bg-secondary/50 cursor-pointer transition-colors border border-border"
                onClick={() => setSelectedGroupId(group.id)}
              >
                <Avatar className="h-12 w-12 flex-shrink-0">
                  {group.profile_picture_url ? (
                    <AvatarImage src={group.profile_picture_url} alt={group.name} />
                  ) : null}
                  <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                    {getInitials(group.name)}
                  </AvatarFallback>
                </Avatar>
                <div className="flex-1 min-w-0">
                  <h3 className="text-sm font-semibold text-foreground truncate">
                    {group.name}
                  </h3>
                  {group.description && (
                    <p className="text-xs text-muted-foreground truncate mt-1">
                      {group.description}
                    </p>
                  )}
                  <div className="flex items-center gap-3 mt-1">
                    <p className="text-xs text-muted-foreground">
                      {group.member_count || 0} {group.member_count === 1 ? 'member' : 'members'}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Created {format(new Date(group.created_at), 'MMM d, yyyy')}
                    </p>
                  </div>
                </div>
                {group.is_owner && (
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={(e) => {
                      e.stopPropagation();
                      setDeleteGroupId(group.id);
                    }}
                    className="text-destructive hover:text-destructive"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
                {!group.is_owner && (
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={(e) => {
                      e.stopPropagation();
                      setLeaveGroupId(group.id);
                    }}
                    className="text-muted-foreground"
                  >
                    <UserMinus className="h-4 w-4" />
                  </Button>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      <CreateGroupModal
        open={createModalOpen}
        onOpenChange={setCreateModalOpen}
      />

      {selectedGroupId && (
        <GroupDetailModal
          groupId={selectedGroupId}
          open={!!selectedGroupId}
          onOpenChange={(open) => {
            if (!open) setSelectedGroupId(null);
          }}
        />
      )}

      <AlertDialog open={!!deleteGroupId} onOpenChange={(open) => !open && setDeleteGroupId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Group</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this group? This action cannot be undone and all members will be removed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteGroup}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={!!leaveGroupId} onOpenChange={(open) => !open && setLeaveGroupId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Leave Group</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to leave this group? You can rejoin later if you're invited.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleLeaveGroup}>
              Leave
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

