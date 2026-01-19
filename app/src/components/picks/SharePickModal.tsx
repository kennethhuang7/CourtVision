import { useState, useEffect } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { useUpdatePickVisibility } from '@/hooks/useUpdatePickVisibility';
import { useGroups } from '@/hooks/useGroups';
import { useFriends } from '@/hooks/useFriends';
import { FriendSelectModal } from './FriendSelectModal';
import { GroupSelectModal } from './GroupSelectModal';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Label } from '@/components/ui/label';
import { Users, UserPlus, Globe, Lock, X, ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { PickVisibility } from '@/hooks/useUpdatePickVisibility';

interface SharePickModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  pickId: string;
  currentVisibility: PickVisibility;
  currentGroupId?: string | null;
}

export function SharePickModal({ open, onOpenChange, pickId, currentVisibility, currentGroupId }: SharePickModalProps) {
  const { user } = useAuth();
  const updateVisibility = useUpdatePickVisibility();
  const { data: groups = [] } = useGroups();
  const { data: friendships = [] } = useFriends();

  
  const friends = friendships
    .map((f: any) => f.friend_profile)
    .filter(Boolean);

  
  const [shareMode, setShareMode] = useState<'private' | 'shared'>('private');

  
  const [shareFriends, setShareFriends] = useState(false);
  const [shareGroups, setShareGroups] = useState(false);
  const [sharePublic, setSharePublic] = useState(false);

  const [selectedGroupIds, setSelectedGroupIds] = useState<string[]>([]);
  const [selectedFriendIds, setSelectedFriendIds] = useState<string[]>([]);

  
  const [friendSelectModalOpen, setFriendSelectModalOpen] = useState(false);
  const [groupSelectModalOpen, setGroupSelectModalOpen] = useState(false);

  useEffect(() => {
    if (open) {
      
      if (!currentVisibility || currentVisibility === 'private') {
        setShareMode('private');
        setShareFriends(false);
        setShareGroups(false);
        setSharePublic(false);
      } else {
        setShareMode('shared');
        setShareFriends(currentVisibility === 'friends' || currentVisibility === 'custom');
        setShareGroups(currentVisibility === 'group' || currentVisibility === 'custom');
        setSharePublic(currentVisibility === 'public');
      }

      if (currentVisibility === 'group' && currentGroupId) {
        setSelectedGroupIds([currentGroupId]);
      } else {
        setSelectedGroupIds([]);
      }
      setSelectedFriendIds([]);
    }
  }, [open, currentVisibility, currentGroupId]);

  const handleSave = async () => {
    try {
      
      let visibility: PickVisibility;

      if (shareMode === 'private') {
        visibility = 'private';
      } else if (sharePublic) {
        
        visibility = 'public';
      } else if (shareFriends && shareGroups) {
        
        visibility = 'custom';
      } else if (shareFriends) {
        visibility = 'friends';
      } else if (shareGroups) {
        visibility = 'group';
      } else {
        
        visibility = 'private';
      }

      await updateVisibility.mutateAsync({
        pickId,
        visibility,
        groupIds: shareGroups ? selectedGroupIds : undefined,
        friendIds: shareFriends ? selectedFriendIds : undefined,
        previousVisibility: currentVisibility
      });
      onOpenChange(false);
    } catch (error) {
      
    }
  };

  const handleUnshare = async () => {
    try {
      await updateVisibility.mutateAsync({ pickId, visibility: 'private' });
      onOpenChange(false);
    } catch (error) {
      
    }
  };

  const isShared = currentVisibility && currentVisibility !== 'private';

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{isShared ? 'Edit Pick Sharing' : 'Share Pick'}</DialogTitle>
          <DialogDescription>
            {isShared 
              ? 'Change who can see this pick or unshare it'
              : 'Choose who can see this pick'}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <RadioGroup value={shareMode} onValueChange={(value) => setShareMode(value as 'private' | 'shared')}>
            <div className="flex items-center space-x-2 p-3 rounded-lg border hover:bg-accent/50 cursor-pointer">
              <RadioGroupItem value="private" id="private" />
              <Label htmlFor="private" className="flex-1 cursor-pointer flex items-center gap-2">
                <Lock className="h-4 w-4 text-muted-foreground" />
                <div>
                  <div className="font-medium">Private</div>
                  <div className="text-xs text-muted-foreground">Only you can see this pick</div>
                </div>
              </Label>
            </div>

            <div className="space-y-2">
              <div className="p-3 rounded-lg border hover:bg-accent/50">
                <div className="flex items-center space-x-2 cursor-pointer">
                  <RadioGroupItem value="shared" id="shared" />
                  <Label htmlFor="shared" className="flex-1 cursor-pointer flex items-center gap-2">
                    <Users className="h-4 w-4 text-muted-foreground" />
                    <div>
                      <div className="font-medium">Shared</div>
                      <div className="text-xs text-muted-foreground">Share with friends, groups, or everyone</div>
                    </div>
                  </Label>
                </div>

                {shareMode === 'shared' && (
                  <div className="ml-6 mt-3 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div
                        className="flex items-center gap-3 cursor-pointer group flex-1"
                        onClick={() => setShareFriends(!shareFriends)}
                      >
                        <div className={cn(
                          "w-4 h-4 rounded border-2 flex items-center justify-center transition-colors",
                          shareFriends
                            ? "border-primary bg-primary"
                            : "border-muted-foreground/30 group-hover:border-muted-foreground/50"
                        )}>
                          {shareFriends && (
                            <svg className="w-3 h-3 text-primary-foreground" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                              <path d="M5 13l4 4L19 7"></path>
                            </svg>
                          )}
                        </div>
                        <div className="flex items-center gap-2">
                          <UserPlus className="h-4 w-4 text-muted-foreground" />
                          <span className="font-medium">Friends</span>
                          <span className="text-sm text-muted-foreground">
                            ({selectedFriendIds.length})
                          </span>
                        </div>
                      </div>
                      {shareFriends && friends.length > 0 && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => setFriendSelectModalOpen(true)}
                          className="h-7 text-xs"
                        >
                          Select
                        </Button>
                      )}
                    </div>
                    {shareFriends && friends.length === 0 && (
                      <p className="text-sm text-muted-foreground ml-7">You don't have any friends yet</p>
                    )}

                    
                    <div className="flex items-center justify-between gap-3">
                      <div
                        className="flex items-center gap-3 cursor-pointer group flex-1"
                        onClick={() => setShareGroups(!shareGroups)}
                      >
                        <div className={cn(
                          "w-4 h-4 rounded border-2 flex items-center justify-center transition-colors",
                          shareGroups
                            ? "border-primary bg-primary"
                            : "border-muted-foreground/30 group-hover:border-muted-foreground/50"
                        )}>
                          {shareGroups && (
                            <svg className="w-3 h-3 text-primary-foreground" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                              <path d="M5 13l4 4L19 7"></path>
                            </svg>
                          )}
                        </div>
                        <div className="flex items-center gap-2">
                          <Users className="h-4 w-4 text-muted-foreground" />
                          <span className="font-medium">Groups</span>
                          <span className="text-sm text-muted-foreground">
                            ({selectedGroupIds.length})
                          </span>
                        </div>
                      </div>
                      {shareGroups && groups.length > 0 && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => setGroupSelectModalOpen(true)}
                          className="h-7 text-xs"
                        >
                          Select
                        </Button>
                      )}
                    </div>
                    {shareGroups && groups.length === 0 && (
                      <p className="text-sm text-muted-foreground ml-7">You're not in any groups yet</p>
                    )}

                    <div
                      className="flex items-center gap-3 cursor-pointer group"
                      onClick={() => setSharePublic(!sharePublic)}
                    >
                      <div className={cn(
                        "w-4 h-4 rounded border-2 flex items-center justify-center transition-colors",
                        sharePublic
                          ? "border-primary bg-primary"
                          : "border-muted-foreground/30 group-hover:border-muted-foreground/50"
                      )}>
                        {sharePublic && (
                          <svg className="w-3 h-3 text-primary-foreground" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                            <path d="M5 13l4 4L19 7"></path>
                          </svg>
                        )}
                      </div>
                      <div className="flex items-center gap-2">
                        <Globe className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Public</span>
                        <span className="text-xs text-muted-foreground">(Everyone)</span>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </RadioGroup>
        </div>

        <div className="flex justify-between gap-2">
          {isShared && (
            <Button
              variant="outline"
              onClick={handleUnshare}
              disabled={updateVisibility.isPending}
            >
              <X className="h-4 w-4 mr-2" />
              Unshare
            </Button>
          )}
          <div className="flex gap-2 ml-auto">
            <Button
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={updateVisibility.isPending}
            >
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              disabled={
                updateVisibility.isPending ||
                (shareMode === 'shared' && !shareFriends && !shareGroups && !sharePublic) ||
                (shareFriends && selectedFriendIds.length === 0) ||
                (shareGroups && selectedGroupIds.length === 0)
              }
            >
              {updateVisibility.isPending ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </div>
      </DialogContent>

      <FriendSelectModal
        open={friendSelectModalOpen}
        onOpenChange={setFriendSelectModalOpen}
        friends={friends}
        selectedFriendIds={selectedFriendIds}
        onConfirm={setSelectedFriendIds}
      />

      <GroupSelectModal
        open={groupSelectModalOpen}
        onOpenChange={setGroupSelectModalOpen}
        groups={groups}
        selectedGroupIds={selectedGroupIds}
        onConfirm={setSelectedGroupIds}
      />
    </Dialog>
  );
}

