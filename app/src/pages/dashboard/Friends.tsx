import { useState } from 'react';
import { Search, UserPlus, UserMinus, Hash } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { useFriends, useFriendRequests, useAcceptFriendRequest, useDeclineFriendRequest, useRemoveFriend } from '@/hooks/useFriends';
import { useSearchUsers, useSearchUsersByFriendCode, useFriendshipStatus, useSendFriendRequest, UserProfile as UserProfileType } from '@/hooks/useFriends';
import { useAuth } from '@/contexts/AuthContext';
import { UserProfileCard } from '@/components/friends/UserProfileCard';
import { cn, getInitials } from '@/lib/utils';
import { toast } from 'sonner';
import { Check, X } from 'lucide-react';
import { logger } from '@/lib/logger';


function SearchResultItem({ 
  userProfile, 
  onSelect, 
  onAddFriend, 
  isAdding 
}: { 
  userProfile: UserProfileType; 
  onSelect: () => void;
  onAddFriend: (userId: string) => void;
  isAdding: boolean;
}) {
  const { data: friendshipStatus } = useFriendshipStatus(userProfile.user_id);
  const displayName = userProfile.display_name || userProfile.username;
  const profilePictureUrl = userProfile.profile_picture_url;

  return (
    <div className="flex items-center gap-3 p-3 rounded-lg hover:bg-secondary/50 transition-colors">
      <Avatar 
        className="h-10 w-10 cursor-pointer"
        onClick={onSelect}
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
        onClick={onSelect}
      >
        <p className="text-sm font-medium text-foreground truncate">
          {displayName}
        </p>
        <p className="text-xs text-muted-foreground truncate">
          {userProfile.username}
        </p>
      </div>
      {friendshipStatus && friendshipStatus.status === 'none' && (
        <Button
          size="sm"
          onClick={() => onAddFriend(userProfile.user_id)}
          disabled={isAdding}
        >
          <UserPlus className="h-4 w-4 mr-2 shrink-0" />
          <span className="whitespace-nowrap">Add Friend</span>
        </Button>
      )}
      {friendshipStatus && friendshipStatus.status === 'pending' && (
        <Button size="sm" variant="outline" disabled>
          {friendshipStatus.isRequester ? 'Requested' : 'Pending'}
        </Button>
      )}
      {friendshipStatus && friendshipStatus.status === 'accepted' && (
        <Button size="sm" variant="outline" disabled>
          Friends
        </Button>
      )}
    </div>
  );
}

export default function Friends() {
  const { user } = useAuth();
  const { data: friends, isLoading: isLoadingFriends } = useFriends();
  const { data: friendRequests, isLoading: isLoadingRequests } = useFriendRequests();
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [searchMode, setSearchMode] = useState<'username' | 'friendcode'>('username');
  const [friendCodeResult, setFriendCodeResult] = useState<UserProfileType | null>(null);
  
  const searchUsers = useSearchUsers();
  const searchByFriendCode = useSearchUsersByFriendCode();
  const sendFriendRequest = useSendFriendRequest();
  const acceptFriendRequest = useAcceptFriendRequest();
  const declineFriendRequest = useDeclineFriendRequest();
  const removeFriend = useRemoveFriend();

  const handleSearch = async () => {
    if (!searchQuery.trim()) {
      toast.error(`Please enter a ${searchMode === 'username' ? 'username' : 'friend code'} to search`);
      return;
    }
    
    try {
      if (searchMode === 'friendcode') {
        const result = await searchByFriendCode.mutateAsync(searchQuery);
        if (!result) {
          toast.info(`No user found with friend code "${searchQuery}"`);
          setFriendCodeResult(null);
        } else {
          setFriendCodeResult(result);
        }
      } else {
        const results = await searchUsers.mutateAsync(searchQuery);
        setFriendCodeResult(null); 
        if (results.length === 0) {
          toast.info(`No users found matching "${searchQuery}"`);
        }
      }
    } catch (error: any) {
      logger.error('Search error', error as Error);
      toast.error(error.message || 'Failed to search users. Make sure the user has logged in at least once to create their profile.');
      setFriendCodeResult(null);
    }
  };

  const handleAddFriend = async (userId: string) => {
    try {
      await sendFriendRequest.mutateAsync(userId);
    } catch (error) {
      
    }
  };

  return (
    <div className="space-y-6">
      <div className="min-w-0">
        <h1 className="text-2xl font-bold text-foreground leading-tight truncate">My Friends</h1>
        <p className="text-sm text-muted-foreground mt-1 leading-tight truncate">
          Search for users and manage your friends list
        </p>
      </div>

      <div className="stat-card">
        <div className="space-y-4">
          <div>
            <h3 className="font-semibold text-foreground mb-2">Search Users</h3>
            <div className="flex gap-2 mb-2">
              <Button
                variant={searchMode === 'username' ? 'default' : 'outline'}
                size="sm"
                onClick={() => {
                  setSearchMode('username');
                  setSearchQuery('');
                  setFriendCodeResult(null);
                }}
              >
                <Search className="h-4 w-4 mr-2 shrink-0" />
                <span className="whitespace-nowrap">Username</span>
              </Button>
              <Button
                variant={searchMode === 'friendcode' ? 'default' : 'outline'}
                size="sm"
                onClick={() => {
                  setSearchMode('friendcode');
                  setSearchQuery('');
                  setFriendCodeResult(null);
                }}
              >
                <Hash className="h-4 w-4 mr-2 shrink-0" />
                <span className="whitespace-nowrap">Friend Code</span>
              </Button>
            </div>
            <div className="flex gap-2">
              <Input
                placeholder={searchMode === 'username' ? 'Search by username...' : 'Enter friend code...'}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value.toUpperCase())}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    handleSearch();
                  }
                }}
                className="flex-1"
                maxLength={searchMode === 'friendcode' ? 6 : undefined}
              />
              <Button onClick={handleSearch} disabled={searchUsers.isPending || searchByFriendCode.isPending}>
                <Search className="h-4 w-4 mr-2 shrink-0" />
                <span className="whitespace-nowrap">Search</span>
              </Button>
            </div>
          </div>

          {(searchUsers.data && searchUsers.data.length > 0) || friendCodeResult ? (
            <div className="space-y-2 mt-4">
              <p className="text-sm font-medium text-foreground">Search Results</p>
              <div className="space-y-2">
                {searchMode === 'friendcode' && friendCodeResult ? (
                  <SearchResultItem
                    key={friendCodeResult.user_id}
                    userProfile={friendCodeResult}
                    onSelect={() => setSelectedUserId(friendCodeResult.user_id)}
                    onAddFriend={handleAddFriend}
                    isAdding={sendFriendRequest.isPending}
                  />
                ) : (
                  searchUsers.data?.map((userProfile) => (
                    <SearchResultItem
                      key={userProfile.user_id}
                      userProfile={userProfile}
                      onSelect={() => setSelectedUserId(userProfile.user_id)}
                      onAddFriend={handleAddFriend}
                      isAdding={sendFriendRequest.isPending}
                    />
                  ))
                )}
              </div>
            </div>
          ) : null}

          {((searchUsers.data && searchUsers.data.length === 0) || (!friendCodeResult && searchMode === 'friendcode')) && searchQuery && (
            <p className="text-sm text-muted-foreground mt-4">No users found</p>
          )}
        </div>
      </div>

      {friendRequests && (friendRequests.received.length > 0 || friendRequests.sent.length > 0) && (
        <div className="stat-card">
          <div>
            {friendRequests.received.length > 0 && (
              <div className="mb-6">
                <h3 className="font-semibold text-foreground mb-4">Friend Requests</h3>
                <div className="space-y-2">
                  {friendRequests.received.map((request: any) => {
                    const requester = request.requester_profile;
                    if (!requester) return null;
                    
                    const displayName = requester.display_name || requester.username;
                    const profilePictureUrl = requester.profile_picture_url;
                    
                    return (
                      <div
                        key={request.id}
                        className="flex items-center gap-3 p-3 rounded-lg bg-secondary/30 border border-border"
                      >
                        <Avatar 
                          className="h-10 w-10 cursor-pointer"
                          onClick={() => setSelectedUserId(requester.user_id)}
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
                          onClick={() => setSelectedUserId(requester.user_id)}
                        >
                          <p className="text-sm font-medium text-foreground truncate">
                            {displayName}
                          </p>
                          <p className="text-xs text-muted-foreground truncate">
                            {requester.username}
                          </p>
                        </div>
                        <div className="flex gap-2">
                          <Button
                            size="sm"
                            onClick={() => acceptFriendRequest.mutate(request.id)}
                            disabled={acceptFriendRequest.isPending}
                          >
                            <Check className="h-4 w-4 mr-1 shrink-0" />
                            <span className="whitespace-nowrap">Accept</span>
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => declineFriendRequest.mutate(request.id)}
                            disabled={declineFriendRequest.isPending}
                          >
                            <X className="h-4 w-4 mr-1 shrink-0" />
                            <span className="whitespace-nowrap">Decline</span>
                          </Button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {friendRequests.sent.length > 0 && (
              <div>
                <h3 className="font-semibold text-foreground mb-4">Sent Requests</h3>
                <div className="space-y-2">
                  {friendRequests.sent.map((request: any) => {
                    const addressee = request.addressee_profile;
                    if (!addressee) return null;
                    
                    const displayName = addressee.display_name || addressee.username;
                    const profilePictureUrl = addressee.profile_picture_url;
                    
                    return (
                      <div
                        key={request.id}
                        className="flex items-center gap-3 p-3 rounded-lg hover:bg-secondary/50 cursor-pointer transition-colors"
                        onClick={() => setSelectedUserId(addressee.user_id)}
                      >
                        <Avatar className="h-10 w-10">
                          {profilePictureUrl ? (
                            <AvatarImage src={profilePictureUrl} alt={displayName} />
                          ) : null}
                          <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                            {getInitials(displayName)}
                          </AvatarFallback>
                        </Avatar>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-foreground truncate">
                            {displayName}
                          </p>
                          <p className="text-xs text-muted-foreground truncate">
                            {addressee.username}
                          </p>
                        </div>
                        <Button size="sm" variant="outline" disabled>
                          Requested
                        </Button>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="stat-card">
        <div>
          <h3 className="font-semibold text-foreground mb-4">Your Friends</h3>
          {isLoadingFriends ? (
            <div className="text-center py-8 text-muted-foreground">Loading...</div>
          ) : !friends || friends.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              No friends yet. Search for users above to add friends!
            </div>
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
                    <Avatar className="h-10 w-10">
                      {profilePictureUrl ? (
                        <AvatarImage src={profilePictureUrl} alt={displayName} />
                      ) : null}
                      <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                        {getInitials(displayName)}
                      </AvatarFallback>
                    </Avatar>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-foreground truncate">
                        {displayName}
                      </p>
                      <p className="text-xs text-muted-foreground truncate">
                        {friend.username}
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {selectedUserId && (
        <UserProfileCard
          userId={selectedUserId}
          open={!!selectedUserId}
          onOpenChange={(open) => {
            if (!open) setSelectedUserId(null);
          }}
        />
      )}
    </div>
  );
}

