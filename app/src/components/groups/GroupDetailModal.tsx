import { useState, useEffect, useRef } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials } from '@/lib/utils';
import { useGroup, useUpdateGroup, useRemoveGroupMember, useDeleteGroup, useLeaveGroup, useGroupInvites, useSendGroupInvite, useAcceptGroupInvite, useDeclineGroupInvite } from '@/hooks/useGroups';
import { useFriends } from '@/hooks/useFriends';
import { useSearchUsers, useSearchUsersByFriendCode, UserProfile as UserProfileType } from '@/hooks/useFriends';
import { useAuth } from '@/contexts/AuthContext';
import { Crown, UserMinus, Trash2, Settings, Search, Hash, UserPlus, X, Pencil, Save, MessageSquare, Upload, Users, Calendar } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { logger } from '@/lib/logger';
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
import { UserProfileCard } from '@/components/friends/UserProfileCard';
import { Switch } from '@/components/ui/switch';
import { ImageCropper } from '@/components/ui/image-cropper';
import { uploadGroupPicture, deleteGroupPicture, validateImageFile } from '@/lib/imageUpload';

interface GroupDetailModalProps {
  groupId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}


function MemberItem({
  member,
  isOwner,
  currentUserId,
  onRemove,
  onViewProfile,
}: {
  member: any;
  isOwner: boolean;
  currentUserId: string;
  onRemove: () => void;
  onViewProfile: () => void;
}) {
  const isGroupOwner = member.user_id === member.group?.owner_id;
  const displayName = member.user_profile?.display_name || member.user_profile?.username || 'Unknown';
  const profilePictureUrl = member.user_profile?.profile_picture_url;

  return (
    <div className="flex items-center gap-3 p-3 rounded-lg hover:bg-secondary/50 transition-colors">
      <Avatar 
        className="h-10 w-10 cursor-pointer"
        onClick={onViewProfile}
      >
        {profilePictureUrl ? (
          <AvatarImage src={profilePictureUrl} alt={displayName} />
        ) : null}
        <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
          {getInitials(displayName)}
        </AvatarFallback>
      </Avatar>
      <div 
        className="flex-1 min-w-0 cursor-pointer"
        onClick={onViewProfile}
      >
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium text-foreground truncate">{displayName}</p>
          {isGroupOwner && (
            <Crown className="h-4 w-4 text-yellow-500 flex-shrink-0" />
          )}
        </div>
        <p className="text-xs text-muted-foreground truncate">
          {member.user_profile?.username || 'Unknown'}
        </p>
        <p className="text-xs text-muted-foreground">
          Joined {format(new Date(member.joined_at), 'MMM d, yyyy')}
        </p>
      </div>
      {isOwner && !isGroupOwner && member.user_id !== currentUserId && (
        <Button
          size="sm"
          variant="ghost"
          onClick={onRemove}
          className="text-destructive hover:text-destructive"
        >
          <UserMinus className="h-4 w-4" />
        </Button>
      )}
    </div>
  );
}


function MemberSelectionItem({
  userProfile,
  isSelected,
  onToggle,
}: {
  userProfile: UserProfileType;
  isSelected: boolean;
  onToggle: () => void;
}) {
  const displayName = userProfile.display_name || userProfile.username;
  const profilePictureUrl = userProfile.profile_picture_url;

  return (
    <div
      className={cn(
        'flex items-center gap-3 p-2 rounded-lg cursor-pointer transition-colors border',
        isSelected ? 'bg-primary/10 border-primary' : 'hover:bg-secondary/50 border-border'
      )}
      onClick={onToggle}
    >
      <Avatar className="h-8 w-8">
        {profilePictureUrl ? (
          <AvatarImage src={profilePictureUrl} alt={displayName} />
        ) : null}
        <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-xs font-semibold text-white">
          {getInitials(displayName)}
        </AvatarFallback>
      </Avatar>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-foreground truncate">{displayName}</p>
        <p className="text-xs text-muted-foreground truncate">{userProfile.username}</p>
      </div>
      {isSelected && (
        <div className="flex-shrink-0 w-5 h-5 rounded-full bg-primary flex items-center justify-center">
          <X className="h-3 w-3 text-primary-foreground" />
        </div>
      )}
    </div>
  );
}

export function GroupDetailModal({ groupId, open, onOpenChange }: GroupDetailModalProps) {
  const { user } = useAuth();
  const navigate = useNavigate();
  const { data: group, isLoading } = useGroup(groupId);
  const { data: invites } = useGroupInvites(groupId);
  const updateGroup = useUpdateGroup();
  const sendInvite = useSendGroupInvite();
  const acceptInvite = useAcceptGroupInvite();
  const declineInvite = useDeclineGroupInvite();
  const removeMember = useRemoveGroupMember();
  const deleteGroup = useDeleteGroup();
  const leaveGroup = useLeaveGroup();
  const { data: friends } = useFriends();
  const searchUsers = useSearchUsers();
  const searchByFriendCode = useSearchUsersByFriendCode();

  const [isEditing, setIsEditing] = useState(false);
  const [editName, setEditName] = useState('');
  const [editDescription, setEditDescription] = useState('');
  const [editProfilePictureUrl, setEditProfilePictureUrl] = useState<string | null>(null);
  const [editMembersCanInvite, setEditMembersCanInvite] = useState(false);
  const [addMemberMode, setAddMemberMode] = useState<'friends' | 'username' | 'friendcode'>('friends');
  const [searchQuery, setSearchQuery] = useState('');
  const [friendCodeResult, setFriendCodeResult] = useState<UserProfileType | null>(null);
  const [selectedMemberIds, setSelectedMemberIds] = useState<Set<string>>(new Set());
  const [removeMemberId, setRemoveMemberId] = useState<string | null>(null);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showLeaveConfirm, setShowLeaveConfirm] = useState(false);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [cropperOpen, setCropperOpen] = useState(false);
  const [cropperImageSrc, setCropperImageSrc] = useState<string>('');
  const groupPictureInputRef = useRef<HTMLInputElement>(null);
  const [activeTab, setActiveTab] = useState<string>('members');

  
  useEffect(() => {
    if (group && !isEditing) {
      
      setEditName(group.name);
      setEditDescription(group.description || '');
      setEditProfilePictureUrl(group.profile_picture_url || null);
      setEditMembersCanInvite(group.members_can_invite || false);
    }
  }, [group?.id, group?.profile_picture_url, isEditing]); 

  const isOwner = group?.is_owner || false;

  const handleSaveEdit = async () => {
    if (!group || !editName.trim()) {
      toast.error('Please enter a group name');
      return;
    }

    try {
      await updateGroup.mutateAsync({
        groupId: group.id,
        name: editName.trim(),
        description: editDescription.trim() || undefined,
        profile_picture_url: editProfilePictureUrl || null, 
        members_can_invite: editMembersCanInvite,
      });
      setIsEditing(false);
    } catch (error) {
      
    }
  };

  const handleSendInvites = async () => {
    if (!group || selectedMemberIds.size === 0) {
      toast.error('Please select at least one user to invite');
      return;
    }

    try {
      for (const userId of selectedMemberIds) {
        await sendInvite.mutateAsync({
          groupId: group.id,
          inviteeId: userId,
        });
      }
      setSelectedMemberIds(new Set());
      setSearchQuery('');
      setFriendCodeResult(null);
    } catch (error) {
      
    }
  };

  const handleRemoveMember = async () => {
    if (!group || !removeMemberId) return;

    try {
      await removeMember.mutateAsync({
        groupId: group.id,
        userId: removeMemberId,
      });
      setRemoveMemberId(null);
    } catch (error) {
      
    }
  };

  const handleDeleteGroup = async () => {
    if (!group) return;

    try {
      await deleteGroup.mutateAsync(group.id);
      setShowDeleteConfirm(false);
      onOpenChange(false);
    } catch (error) {
      
    }
  };

  const handleLeaveGroup = async () => {
    if (!group) return;

    try {
      await leaveGroup.mutateAsync(group.id);
      setShowLeaveConfirm(false);
      onOpenChange(false);
    } catch (error) {
      
    }
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) {
      toast.error('Please enter a search query');
      return;
    }

    try {
      if (addMemberMode === 'friendcode') {
        const result = await searchByFriendCode.mutateAsync(searchQuery);
        if (result) {
          setFriendCodeResult(result);
        } else {
          toast.info(`No user found with friend code "${searchQuery}"`);
          setFriendCodeResult(null);
        }
      } else {
        const results = await searchUsers.mutateAsync(searchQuery);
        setFriendCodeResult(null);
        if (results.length === 0) {
          toast.info(`No users found matching "${searchQuery}"`);
        }
      }
    } catch (error: any) {
      toast.error(error.message || 'Failed to search users');
    }
  };

  if (isLoading || !group) {
    return (
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-2xl">
          <DialogTitle className="sr-only">Loading Group</DialogTitle>
          <DialogDescription className="sr-only">Loading group information</DialogDescription>
          <div className="text-center py-8">Loading...</div>
        </DialogContent>
      </Dialog>
    );
  }

  const existingMemberIds = new Set([
    group.owner_id,
    ...(group.members || []).map((m: any) => m.user_id),
  ]);

  
  const pendingInviteIds = new Set(
    (invites || [])
      .filter((inv: any) => inv.status === 'pending')
      .map((inv: any) => inv.invitee_id)
  );

  return (
    <>
      <Dialog 
        open={open} 
        onOpenChange={(newOpen) => {
          
          if (!newOpen && isEditing) {
            return;
          }
          onOpenChange(newOpen);
        }}
      >
        <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <div className="flex items-center justify-between">
              <div>
                <DialogTitle>
                  {isEditing ? 'Edit Group' : group.name}
                </DialogTitle>
                <DialogDescription className="sr-only">
                  {isEditing ? 'Edit group details and settings' : 'View group details and members'}
                </DialogDescription>
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    onOpenChange(false);
                    navigate('/dashboard/messages', { 
                      state: { openConversation: { type: 'group', id: group.id } } 
                    });
                  }}
                  title="Open group chat"
                >
                  <MessageSquare className="h-4 w-4 mr-2" />
                  Chat
                </Button>
                {isOwner && (
                  <>
                    {isEditing ? (
                      <>
                        <Button 
                          size="sm" 
                          variant="outline" 
                          onClick={() => {
                            setIsEditing(false);
                            
                            if (group) {
                              setEditName(group.name);
                              setEditDescription(group.description || '');
                              setEditProfilePictureUrl(group.profile_picture_url || null);
                              setEditMembersCanInvite(group.members_can_invite || false);
                            }
                          }}
                        >
                          Cancel
                        </Button>
                        <Button size="sm" onClick={handleSaveEdit} disabled={updateGroup.isPending}>
                          <Save className="h-4 w-4 mr-2" />
                          Save
                        </Button>
                      </>
                    ) : (
                      <Button 
                        size="sm" 
                        variant="outline" 
                        onClick={() => {
                          setIsEditing(true);
                          setActiveTab('details'); 
                        }}
                      >
                        <Settings className="h-4 w-4 mr-2" />
                        Edit
                      </Button>
                    )}
                  </>
                )}
              </div>
            </div>
          </DialogHeader>

          <Tabs 
            value={activeTab} 
            onValueChange={(value) => {
              
              if (isEditing && activeTab === 'details' && value !== 'details') {
                return;
              }
              setActiveTab(value);
            }} 
            className="w-full"
          >
            <TabsList>
              <TabsTrigger value="members" disabled={isEditing}>Members</TabsTrigger>
              {(isOwner || group?.members_can_invite) && <TabsTrigger value="invites" disabled={isEditing}>Invites</TabsTrigger>}
              {(isOwner || group?.members_can_invite) && <TabsTrigger value="send" disabled={isEditing}>Send Invites</TabsTrigger>}
              <TabsTrigger value="details">Details</TabsTrigger>
            </TabsList>

            <TabsContent value="members" className="space-y-4">
              <div className="space-y-2">
                <p className="text-sm font-medium text-foreground">
                  {group.members?.length || 0} {group.members?.length === 1 ? 'Member' : 'Members'}
                </p>
                <div className="space-y-1 max-h-96 overflow-y-auto">
                  {group.members?.map((member: any) => (
                    <MemberItem
                      key={member.id}
                      member={{ ...member, group: { owner_id: group.owner_id } }}
                      isOwner={isOwner}
                      currentUserId={user?.id || ''}
                      onRemove={() => setRemoveMemberId(member.user_id)}
                      onViewProfile={() => setSelectedUserId(member.user_id)}
                    />
                  ))}
                </div>
              </div>
            </TabsContent>

            {(isOwner || group?.members_can_invite) && (
              <TabsContent value="invites" className="space-y-4">
                <div className="space-y-2">
                  <p className="text-sm font-medium text-foreground">Pending Invites</p>
                  {invites && invites.length > 0 ? (
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                      {invites.map((invite) => {
                        const invitee = invite.invitee_profile;
                        if (!invitee) return null;
                        const displayName = invitee.display_name || invitee.username;
                        return (
                          <div key={invite.id} className="flex items-center gap-3 p-3 rounded-lg border border-border">
                            <Avatar className="h-10 w-10">
                              {invitee.profile_picture_url ? (
                                <AvatarImage src={invitee.profile_picture_url} alt={displayName} />
                              ) : null}
                              <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                                {getInitials(displayName)}
                              </AvatarFallback>
                            </Avatar>
                            <div className="flex-1 min-w-0">
                              <p className="text-sm font-medium text-foreground truncate">{displayName}</p>
                              <p className="text-xs text-muted-foreground truncate">{invitee.username}</p>
                              <p className="text-xs text-muted-foreground">
                                Sent {format(new Date(invite.created_at), 'MMM d, yyyy')}
                              </p>
                            </div>
                            <div className="flex gap-2">
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => declineInvite.mutate(invite.id)}
                                disabled={declineInvite.isPending}
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground text-center py-4">
                      No pending invites
                    </p>
                  )}
                </div>
              </TabsContent>
            )}

            {(isOwner || group?.members_can_invite) && (
              <TabsContent value="send" className="space-y-4">
                <div className="space-y-4">
                  <Tabs value={addMemberMode} onValueChange={(value) => setAddMemberMode(value as any)}>
                    <TabsList className="grid w-full grid-cols-3">
                      <TabsTrigger value="friends">From Friends</TabsTrigger>
                      <TabsTrigger value="username">By Username</TabsTrigger>
                      <TabsTrigger value="friendcode">By Friend Code</TabsTrigger>
                    </TabsList>

                    <TabsContent value="friends" className="space-y-2 mt-4">
                      {friends && friends.length > 0 ? (
                        <div className="max-h-60 overflow-y-auto space-y-2">
                          {friends
                            .filter((f: any) => {
                              const friendId = f.friend_profile?.user_id;
                              return friendId && !existingMemberIds.has(friendId) && !pendingInviteIds.has(friendId);
                            })
                            .map((friendship: any) => {
                              const friend = friendship.friend_profile;
                              if (!friend) return null;
                              const isSelected = selectedMemberIds.has(friend.user_id);
                              return (
                                <MemberSelectionItem
                                  key={friend.user_id}
                                  userProfile={friend}
                                  isSelected={isSelected}
                                  onToggle={() => {
                                    if (isSelected) {
                                      setSelectedMemberIds(prev => {
                                        const newSet = new Set(prev);
                                        newSet.delete(friend.user_id);
                                        return newSet;
                                      });
                                    } else {
                                      setSelectedMemberIds(prev => new Set([...prev, friend.user_id]));
                                    }
                                  }}
                                />
                              );
                            })}
                        </div>
                      ) : (
                        <p className="text-sm text-muted-foreground text-center py-4">
                          No friends available to add
                        </p>
                      )}
                    </TabsContent>

                    <TabsContent value="username" className="space-y-2 mt-4">
                      <div className="flex gap-2">
                        <Input
                          placeholder="Search by username..."
                          value={searchQuery}
                          onChange={(e) => setSearchQuery(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') handleSearch();
                          }}
                          className="flex-1"
                        />
                        <Button onClick={handleSearch} disabled={searchUsers.isPending}>
                          <Search className="h-4 w-4 mr-2" />
                          Search
                        </Button>
                      </div>
                      {searchUsers.data && searchUsers.data.length > 0 && (
                        <div className="max-h-60 overflow-y-auto space-y-2">
                          {searchUsers.data
                            .filter(u => !existingMemberIds.has(u.user_id) && !pendingInviteIds.has(u.user_id))
                            .map((userProfile) => {
                              const isSelected = selectedMemberIds.has(userProfile.user_id);
                              return (
                                <MemberSelectionItem
                                  key={userProfile.user_id}
                                  userProfile={userProfile}
                                  isSelected={isSelected}
                                  onToggle={() => {
                                    if (isSelected) {
                                      setSelectedMemberIds(prev => {
                                        const newSet = new Set(prev);
                                        newSet.delete(userProfile.user_id);
                                        return newSet;
                                      });
                                    } else {
                                      setSelectedMemberIds(prev => new Set([...prev, userProfile.user_id]));
                                    }
                                  }}
                                />
                              );
                            })}
                        </div>
                      )}
                    </TabsContent>

                    <TabsContent value="friendcode" className="space-y-2 mt-4">
                      <div className="flex gap-2">
                        <Input
                          placeholder="Enter friend code..."
                          value={searchQuery}
                          onChange={(e) => setSearchQuery(e.target.value.toUpperCase())}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') handleSearch();
                          }}
                          className="flex-1"
                          maxLength={6}
                        />
                        <Button onClick={handleSearch} disabled={searchByFriendCode.isPending}>
                          <Hash className="h-4 w-4 mr-2" />
                          Search
                        </Button>
                      </div>
                      {friendCodeResult && !existingMemberIds.has(friendCodeResult.user_id) && !pendingInviteIds.has(friendCodeResult.user_id) && (
                        <div className="max-h-60 overflow-y-auto">
                          <MemberSelectionItem
                            userProfile={friendCodeResult}
                            isSelected={selectedMemberIds.has(friendCodeResult.user_id)}
                            onToggle={() => {
                              if (selectedMemberIds.has(friendCodeResult.user_id)) {
                                setSelectedMemberIds(prev => {
                                  const newSet = new Set(prev);
                                  newSet.delete(friendCodeResult.user_id);
                                  return newSet;
                                });
                              } else {
                                setSelectedMemberIds(prev => new Set([...prev, friendCodeResult.user_id]));
                              }
                            }}
                          />
                        </div>
                      )}
                    </TabsContent>
                  </Tabs>

                  {selectedMemberIds.size > 0 && (
                    <Button onClick={handleSendInvites} disabled={sendInvite.isPending} className="w-full">
                      <UserPlus className="h-4 w-4 mr-2" />
                      Send {selectedMemberIds.size} {selectedMemberIds.size === 1 ? 'Invite' : 'Invites'}
                    </Button>
                  )}
                </div>
              </TabsContent>
            )}

            <TabsContent value="details" className="space-y-6">
              {isEditing ? (
                <div className="space-y-6">
                  <div className="space-y-3">
                    <Label className="text-base font-semibold">Group Picture</Label>
                    <div className="flex items-start gap-6">
                      <div className="relative group flex-shrink-0">
                        <Avatar className="h-28 w-28 border-2 border-border">
                          {editProfilePictureUrl ? (
                            <AvatarImage src={editProfilePictureUrl} alt={group.name} />
                          ) : null}
                          <AvatarFallback className="bg-gradient-to-br from-primary/20 to-accent/20">
                            <Users className="h-14 w-14 text-muted-foreground" />
                          </AvatarFallback>
                        </Avatar>
                        <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity rounded-full flex items-center justify-center cursor-pointer"
                          onClick={() => groupPictureInputRef.current?.click()}
                        >
                          <Pencil className="h-6 w-6 text-white" />
                        </div>
                        <input
                          type="file"
                          ref={groupPictureInputRef}
                          accept="image/*"
                          className="hidden"
                          onChange={async (e) => {
                            const file = e.target.files?.[0];
                            if (!file || !user) return;
                            const validationError = await validateImageFile(file, 3);
                            if (validationError) {
                              toast.error(validationError);
                              return;
                            }
                            const imageUrl = URL.createObjectURL(file);
                            setCropperImageSrc(imageUrl);
                            setCropperOpen(true);
                            if (groupPictureInputRef.current) {
                              groupPictureInputRef.current.value = '';
                            }
                          }}
                        />
                      </div>
                      <div className="flex-1 space-y-3">
                        <div className="flex gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => groupPictureInputRef.current?.click()}
                          >
                            <Upload className="h-4 w-4 mr-2" />
                            {editProfilePictureUrl ? 'Change Picture' : 'Upload Picture'}
                          </Button>
                          {editProfilePictureUrl && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={async () => {
                                try {
                                  await deleteGroupPicture(editProfilePictureUrl);
                                  setEditProfilePictureUrl(null);
                                  toast.success('Group picture removed');
                                } catch (error: any) {
                                  toast.error('Failed to remove picture');
                                }
                              }}
                            >
                              <Trash2 className="h-4 w-4 mr-2" />
                              Remove
                            </Button>
                          )}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          Recommended: Square image, max 3MB. Click the picture to change it.
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="edit-name" className="text-base font-semibold">Group Name</Label>
                    <Input
                      id="edit-name"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      maxLength={100}
                      placeholder="Enter group name..."
                      className="text-base"
                    />
                    <p className="text-xs text-muted-foreground">
                      {editName.length}/100 characters
                    </p>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="edit-description" className="text-base font-semibold">Description</Label>
                    <Textarea
                      id="edit-description"
                      value={editDescription}
                      onChange={(e) => setEditDescription(e.target.value)}
                      rows={5}
                      maxLength={500}
                      placeholder="Tell others about this group..."
                      className="resize-none"
                    />
                    <p className="text-xs text-muted-foreground">
                      {editDescription.length}/500 characters
                    </p>
                  </div>

                  <div className="flex items-center justify-between p-4 border rounded-lg bg-secondary/50">
                    <div className="space-y-1 flex-1">
                      <Label htmlFor="members-can-invite" className="text-base font-semibold cursor-pointer">
                        Allow Members to Invite
                      </Label>
                      <p className="text-sm text-muted-foreground">
                        Let non-owner members send invites to the group
                      </p>
                    </div>
                    <Switch
                      id="members-can-invite"
                      checked={editMembersCanInvite}
                      onCheckedChange={setEditMembersCanInvite}
                    />
                  </div>

                  {isOwner && (
                    <div className="border-t border-destructive/20 pt-6 mt-6">
                      <p className="text-sm text-muted-foreground mb-4">
                        Permanently delete this group. This action cannot be undone.
                      </p>
                      <Button
                        variant="destructive"
                        onClick={() => setShowDeleteConfirm(true)}
                        className="w-full"
                      >
                        <Trash2 className="h-4 w-4 mr-2" />
                        Delete Group
                      </Button>
                    </div>
                  )}
                </div>
              ) : (
                <div className="space-y-6">
                  <div className="flex items-start gap-4 pt-2 pb-4 border-b">
                    <div className="flex-shrink-0">
                      {group.profile_picture_url ? (
                        <Avatar className="h-24 w-24 border-2 border-primary/30 ring-2 ring-primary/10">
                          <AvatarImage src={group.profile_picture_url} alt={group.name} />
                          <AvatarFallback className="bg-gradient-to-br from-primary/30 to-accent/30">
                            <Users className="h-12 w-12 text-primary" />
                          </AvatarFallback>
                        </Avatar>
                      ) : (
                        <div className="h-24 w-24 rounded-full border-2 border-primary/30 ring-2 ring-primary/10 bg-gradient-to-br from-primary/20 to-accent/20 flex items-center justify-center">
                          <Users className="h-12 w-12 text-primary" />
                        </div>
                      )}
                    </div>

                    <div className="flex-1 min-w-0">
                      <h2 className="text-2xl font-bold text-foreground mb-1">{group.name}</h2>
                      
                      {(() => {
                        const owner = group.members?.find((m: any) => m.user_id === group.owner_id);
                        const ownerName = owner?.user_profile?.display_name || owner?.user_profile?.username || 'Unknown';
                        return (
                          <div className="flex items-center gap-1.5 mb-3 text-sm text-muted-foreground">
                            <Crown className="h-3.5 w-3.5 text-yellow-500" />
                            <span>Owner: <span className="font-medium text-foreground">{ownerName}</span></span>
                          </div>
                        );
                      })()}
                      
                      <div className="flex items-center gap-4 flex-wrap">
                        <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
                          <Users className="h-4 w-4" />
                          <span className="font-medium text-foreground">{group.members?.length || 0}</span>
                          <span>{group.members?.length === 1 ? 'member' : 'members'}</span>
                        </div>
                        <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
                          <Calendar className="h-4 w-4" />
                          <span>Created {format(new Date(group.created_at), 'MMM d, yyyy')}</span>
                        </div>
                        {group.members_can_invite && (
                          <div className="flex items-center gap-1.5">
                            <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-primary/20 text-primary border border-primary/30">
                              Members can invite
                            </span>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  <div className="space-y-3">
                    <div className="flex items-center gap-2">
                      <Label className="text-base font-semibold text-foreground">About</Label>
                      <div className="flex-1 h-px bg-border"></div>
                    </div>
                    <div className="p-4 rounded-lg bg-secondary/50 border border-border/50">
                      <p className="text-sm text-foreground leading-relaxed">
                        {group.description ? (
                          <span>{group.description}</span>
                        ) : (
                          <span className="text-muted-foreground italic">
                            This group doesn't have a description yet.
                          </span>
                        )}
                      </p>
                    </div>
                  </div>

                  
                  <div className="flex gap-3 pt-2">
                    <Button
                      variant="default"
                      onClick={() => {
                        onOpenChange(false);
                        navigate('/dashboard/messages', { 
                          state: { openConversation: { type: 'group', id: group.id } } 
                        });
                      }}
                      className="flex-1"
                    >
                      <MessageSquare className="h-4 w-4 mr-2" />
                      Open Chat
                    </Button>
                    {!isOwner && (
                      <Button
                        variant="outline"
                        onClick={() => setShowLeaveConfirm(true)}
                        className="flex-1"
                      >
                        <UserMinus className="h-4 w-4 mr-2" />
                        Leave Group
                      </Button>
                    )}
                  </div>
                </div>
              )}
            </TabsContent>
          </Tabs>
        </DialogContent>
      </Dialog>

      
      <AlertDialog open={!!removeMemberId} onOpenChange={(open) => !open && setRemoveMemberId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove Member</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to remove this member from the group?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleRemoveMember}>
              Remove
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      
      <AlertDialog open={showDeleteConfirm} onOpenChange={setShowDeleteConfirm}>
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

      
      <AlertDialog open={showLeaveConfirm} onOpenChange={setShowLeaveConfirm}>
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

      {selectedUserId && (
        <UserProfileCard
          userId={selectedUserId}
          open={!!selectedUserId}
          onOpenChange={(open) => {
            if (!open) setSelectedUserId(null);
          }}
        />
      )}

      <ImageCropper
        open={cropperOpen}
        onOpenChange={setCropperOpen}
        imageSrc={cropperImageSrc}
        onCropComplete={async (croppedBlob: Blob) => {
          if (!user || !group) return;
          try {
            const file = new File([croppedBlob], 'group-picture.jpg', { type: 'image/jpeg' });
            if (editProfilePictureUrl) {
              try {
                await deleteGroupPicture(editProfilePictureUrl);
              } catch (err) {
                logger.warn('Error deleting old group picture', { error: err });
              }
            }
            const url = await uploadGroupPicture(file, user.id);
            setEditProfilePictureUrl(url);
            URL.revokeObjectURL(cropperImageSrc);
            toast.success('Group picture updated');
          } catch (error: any) {
            logger.error('Error uploading group picture', error as Error);
            toast.error(error.message || 'Failed to upload group picture');
          }
        }}
        aspectRatio={1}
        cropShape="round"
        title="Crop Group Picture"
      />
    </>
  );
}

