import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { useFriends } from '@/hooks/useFriends';
import { useState } from 'react';
import { UserProfileCard } from './UserProfileCard';
import { cn, getInitials } from '@/lib/utils';

interface FriendsListModalProps {
  userId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function FriendsListModal({ userId, open, onOpenChange }: FriendsListModalProps) {
  const { data: friends, isLoading } = useFriends(userId);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Friends</DialogTitle>
          </DialogHeader>
          <div className="max-h-[60vh] overflow-y-auto">
            {isLoading ? (
              <div className="text-center py-8 text-muted-foreground whitespace-nowrap">Loading...</div>
            ) : !friends || friends.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground whitespace-nowrap">No friends yet</div>
            ) : (
              <div className="space-y-2">
                {friends.map((friendship: any) => {
                  const friend = friendship.friend_profile;
                  if (!friend) return null;

                  const displayName = friend.display_name || friend.username;
                  const profilePictureUrl = friend.profile_picture_url;

                  return (
                    <div
                      key={friendship.id}
                      className="flex items-center gap-3 p-3 rounded-lg hover:bg-secondary/50 cursor-pointer transition-colors"
                      onClick={() => setSelectedUserId(friend.user_id)}
                    >
                      <Avatar className="h-10 w-10 shrink-0">
                        {profilePictureUrl ? (
                          <AvatarImage src={profilePictureUrl} alt={displayName} />
                        ) : null}
                        <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                          {getInitials(displayName)}
                        </AvatarFallback>
                      </Avatar>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-foreground truncate leading-tight">
                          {displayName}
                        </p>
                        <p className="text-xs text-muted-foreground truncate leading-tight">
                          {friend.username}
                        </p>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>

      {selectedUserId && (
        <UserProfileCard
          userId={selectedUserId}
          open={!!selectedUserId}
          onOpenChange={(open) => {
            if (!open) setSelectedUserId(null);
          }}
        />
      )}
    </>
  );
}

