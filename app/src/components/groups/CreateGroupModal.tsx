import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials } from '@/lib/utils';
import { useCreateGroup } from '@/hooks/useGroups';
import { useFriends } from '@/hooks/useFriends';
import { useSearchUsers, useSearchUsersByFriendCode, UserProfile as UserProfileType } from '@/hooks/useFriends';
import { useAuth } from '@/contexts/AuthContext';
import { Search, Hash, UserPlus, X } from 'lucide-react';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';

interface CreateGroupModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
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
      <Avatar className="h-8 w-8 shrink-0">
        {profilePictureUrl ? (
          <AvatarImage src={profilePictureUrl} alt={displayName} />
        ) : null}
        <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-xs font-semibold text-white">
          {getInitials(displayName)}
        </AvatarFallback>
      </Avatar>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-foreground truncate leading-tight">{displayName}</p>
        <p className="text-xs text-muted-foreground truncate leading-tight">{userProfile.username}</p>
      </div>
      {isSelected && (
        <div className="flex-shrink-0 w-5 h-5 rounded-full bg-primary flex items-center justify-center">
          <X className="h-3 w-3 shrink-0 text-primary-foreground" />
        </div>
      )}
    </div>
  );
}

export function CreateGroupModal({ open, onOpenChange }: CreateGroupModalProps) {
  const { user } = useAuth();
  const createGroup = useCreateGroup();
  const { data: friends } = useFriends();
  const searchUsers = useSearchUsers();
  const searchByFriendCode = useSearchUsersByFriendCode();

  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [selectedInviteeIds, setSelectedInviteeIds] = useState<Set<string>>(new Set());
  const [addMemberMode, setAddMemberMode] = useState<'friends' | 'username' | 'friendcode'>('friends');
  const [searchQuery, setSearchQuery] = useState('');
  const [friendCodeResult, setFriendCodeResult] = useState<UserProfileType | null>(null);

  
  useEffect(() => {
    if (!open) {
      setName('');
      setDescription('');
      setSelectedInviteeIds(new Set());
      setSearchQuery('');
      setFriendCodeResult(null);
    }
  }, [open]);

  const handleAddMember = (userId: string) => {
    setSelectedInviteeIds(prev => new Set([...prev, userId]));
  };

  const handleRemoveMember = (userId: string) => {
    setSelectedInviteeIds(prev => {
      const newSet = new Set(prev);
      newSet.delete(userId);
      return newSet;
    });
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

  const handleCreate = async () => {
    if (!name.trim()) {
      toast.error('Please enter a group name');
      return;
    }

    try {
      await createGroup.mutateAsync({
        name: name.trim(),
        description: description.trim() || undefined,
        inviteeIds: Array.from(selectedInviteeIds),
      });
      
      
      setName('');
      setDescription('');
      setSelectedInviteeIds(new Set());
      setSearchQuery('');
      setFriendCodeResult(null);
      onOpenChange(false);
    } catch (error) {
      
    }
  };

  const selectedMembers = Array.from(selectedInviteeIds).map(userId => {
    
    const friend = friends?.find(f => f.friend_profile?.user_id === userId)?.friend_profile;
    if (friend) return friend;
    
    
    if (searchUsers.data) {
      const found = searchUsers.data.find(u => u.user_id === userId);
      if (found) return found;
    }
    
    
    if (friendCodeResult && friendCodeResult.user_id === userId) {
      return friendCodeResult;
    }
    
    return null;
  }).filter(Boolean) as UserProfileType[];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Create Group</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="group-name">Group Name *</Label>
            <Input
              id="group-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Enter group name"
              maxLength={100}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="group-description">Description</Label>
            <Textarea
              id="group-description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Enter group description (optional)"
              rows={3}
              maxLength={500}
            />
          </div>

          <div className="space-y-4 border-t pt-4">
            <div className="flex items-center justify-between gap-2">
              <Label className="whitespace-nowrap">Send Invites (Optional)</Label>
              <span className="text-sm text-muted-foreground whitespace-nowrap">
                {selectedInviteeIds.size} {selectedInviteeIds.size === 1 ? 'invite' : 'invites'} selected
              </span>
            </div>

            {selectedMembers.length > 0 && (
              <div className="space-y-2">
                <p className="text-sm font-medium text-foreground whitespace-nowrap">Selected Invitees</p>
                <div className="flex flex-wrap gap-2">
                  {selectedMembers.map((member) => {
                    const displayName = member.display_name || member.username;
                    return (
                      <div
                        key={member.user_id}
                        className="flex items-center gap-2 px-3 py-1.5 bg-primary/10 rounded-full border border-primary"
                      >
                        <span className="text-sm text-foreground truncate max-w-[150px]">{displayName}</span>
                        <button
                          onClick={() => handleRemoveMember(member.user_id)}
                          className="text-primary hover:text-primary/80 shrink-0"
                        >
                          <X className="h-3 w-3 shrink-0" />
                        </button>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            <Tabs value={addMemberMode} onValueChange={(value) => setAddMemberMode(value as any)}>
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="friends">From Friends</TabsTrigger>
                <TabsTrigger value="username">By Username</TabsTrigger>
                <TabsTrigger value="friendcode">By Friend Code</TabsTrigger>
              </TabsList>

              <TabsContent value="friends" className="space-y-2 mt-4">
                {friends && friends.length > 0 ? (
                  <div className="max-h-60 overflow-y-auto space-y-2">
                    {friends.map((friendship: any) => {
                      const friend = friendship.friend_profile;
                      if (!friend) return null;
                              const isSelected = selectedInviteeIds.has(friend.user_id);
                              return (
                                <MemberSelectionItem
                                  key={friend.user_id}
                                  userProfile={friend}
                                  isSelected={isSelected}
                                  onToggle={() => {
                                    if (isSelected) {
                                      handleRemoveMember(friend.user_id);
                                    } else {
                                      handleAddMember(friend.user_id);
                                    }
                                  }}
                                />
                              );
                            })}
                        </div>
                      ) : (
                        <p className="text-sm text-muted-foreground text-center py-4 whitespace-nowrap">
                          No friends to invite. Add friends first!
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
                  <Button onClick={handleSearch} disabled={searchUsers.isPending} className="shrink-0">
                    <Search className="h-4 w-4 mr-2 shrink-0" />
                    <span className="whitespace-nowrap">Search</span>
                  </Button>
                </div>
                      {searchUsers.data && searchUsers.data.length > 0 && (
                        <div className="max-h-60 overflow-y-auto space-y-2">
                          {searchUsers.data.map((userProfile) => {
                            const isSelected = selectedInviteeIds.has(userProfile.user_id);
                      return (
                        <MemberSelectionItem
                          key={userProfile.user_id}
                          userProfile={userProfile}
                          isSelected={isSelected}
                          onToggle={() => {
                            if (isSelected) {
                              handleRemoveMember(userProfile.user_id);
                            } else {
                              handleAddMember(userProfile.user_id);
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
                  <Button onClick={handleSearch} disabled={searchByFriendCode.isPending} className="shrink-0">
                    <Hash className="h-4 w-4 mr-2 shrink-0" />
                    <span className="whitespace-nowrap">Search</span>
                  </Button>
                </div>
                      {friendCodeResult && (
                        <div className="max-h-60 overflow-y-auto">
                          <MemberSelectionItem
                            userProfile={friendCodeResult}
                            isSelected={selectedInviteeIds.has(friendCodeResult.user_id)}
                            onToggle={() => {
                              if (selectedInviteeIds.has(friendCodeResult.user_id)) {
                                handleRemoveMember(friendCodeResult.user_id);
                              } else {
                                handleAddMember(friendCodeResult.user_id);
                              }
                            }}
                          />
                        </div>
                      )}
              </TabsContent>
            </Tabs>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            <span className="whitespace-nowrap">Cancel</span>
          </Button>
          <Button onClick={handleCreate} disabled={!name.trim() || createGroup.isPending}>
            <span className="whitespace-nowrap">{createGroup.isPending ? 'Creating...' : 'Create Group'}</span>
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

