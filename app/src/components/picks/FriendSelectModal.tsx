import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Input } from '@/components/ui/input';
import { Search } from 'lucide-react';
import { getInitials } from '@/lib/utils';
import type { UserProfile } from '@/hooks/useFriends';

interface FriendSelectModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  friends: UserProfile[];
  selectedFriendIds: string[];
  onConfirm: (friendIds: string[]) => void;
}

export function FriendSelectModal({
  open,
  onOpenChange,
  friends,
  selectedFriendIds,
  onConfirm,
}: FriendSelectModalProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set(selectedFriendIds));
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (open) {
      setSelected(new Set(selectedFriendIds));
      setSearchQuery('');
    }
  }, [open, selectedFriendIds]);

  const filteredFriends = friends.filter(friend => {
    const displayName = friend.display_name || friend.username;
    const username = friend.username;
    const query = searchQuery.toLowerCase();
    return displayName.toLowerCase().includes(query) || username.toLowerCase().includes(query);
  });

  const toggleFriend = (userId: string) => {
    const newSelected = new Set(selected);
    if (newSelected.has(userId)) {
      newSelected.delete(userId);
    } else {
      newSelected.add(userId);
    }
    setSelected(newSelected);
  };

  const handleSelectAll = () => {
    if (selected.size === friends.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(friends.map(f => f.user_id)));
    }
  };

  const handleConfirm = () => {
    onConfirm(Array.from(selected));
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Select Friends</DialogTitle>
          <DialogDescription>
            Choose which friends can see this pick
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search friends..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>

          <div className="flex items-center justify-between p-3 rounded-lg border">
            <div className="flex items-center gap-2">
              <Checkbox
                checked={selected.size === friends.length && friends.length > 0}
                onCheckedChange={handleSelectAll}
                id="select-all"
              />
              <label htmlFor="select-all" className="text-sm font-medium cursor-pointer">
                Select All ({friends.length})
              </label>
            </div>
            <span className="text-xs text-muted-foreground">
              {selected.size} selected
            </span>
          </div>

          <div className="max-h-[400px] overflow-y-auto space-y-2">
            {filteredFriends.length === 0 ? (
              <p className="text-center text-sm text-muted-foreground py-8">
                {searchQuery ? 'No friends found' : 'No friends yet'}
              </p>
            ) : (
              filteredFriends.map((friend) => {
                const displayName = friend.display_name || friend.username;
                const isSelected = selected.has(friend.user_id);

                return (
                  <div
                    key={friend.user_id}
                    className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent/50 cursor-pointer transition-colors"
                    onClick={() => toggleFriend(friend.user_id)}
                  >
                    <Checkbox
                      checked={isSelected}
                      onCheckedChange={() => toggleFriend(friend.user_id)}
                    />
                    <Avatar className="h-10 w-10">
                      {friend.profile_picture_url && (
                        <AvatarImage src={friend.profile_picture_url} alt={displayName} />
                      )}
                      <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-white text-sm">
                        {getInitials(displayName)}
                      </AvatarFallback>
                    </Avatar>
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-foreground truncate">{displayName}</p>
                      <p className="text-xs text-muted-foreground truncate">@{friend.username}</p>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={selected.size === 0}>
            Confirm ({selected.size})
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
