import { useMemo, useState } from 'react';
import { Search, UserPlus, UserMinus, Hash, MessageSquare } from 'lucide-react';
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
import { useCreateDM } from '@/hooks/useCreateDM';
import { useNavigate } from 'react-router-dom';
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
    <div className="flex items-center gap-3 rounded-lg border border-transparent p-3 hover:border-border/60 hover:bg-secondary/40 transition-colors">
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
  const [friendsFilter, setFriendsFilter] = useState('');
  const [unfriendConfirm, setUnfriendConfirm] = useState<{
    open: boolean;
    friendshipId?: string;
    friendName?: string;
  }>({ open: false });
  
  const searchUsers = useSearchUsers();
  const searchByFriendCode = useSearchUsersByFriendCode();
  const sendFriendRequest = useSendFriendRequest();
  const acceptFriendRequest = useAcceptFriendRequest();
  const declineFriendRequest = useDeclineFriendRequest();
  const removeFriend = useRemoveFriend();
  const createDM = useCreateDM();
  const navigate = useNavigate();

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

  const filteredFriends = useMemo(() => {
    const list = friends || [];
    const q = friendsFilter.trim().toLowerCase();
    if (!q) return list;
    return list.filter((friendship: any) => {
      const friend = friendship?.friend_profile;
      if (!friend) return false;
      const displayName = (friend.display_name || '').toLowerCase();
      const username = (friend.username || '').toLowerCase();
      const friendCode = (friend.friend_code || '').toLowerCase();
      const userIdStr = (friend.user_id || '').toLowerCase();
      return (
        displayName.includes(q) ||
        username.includes(q) ||
        friendCode.includes(q) ||
        userIdStr.includes(q)
      );
    });
  }, [friends, friendsFilter]);

  const handleMessageFriend = async (otherUserId: string) => {
    try {
      const conversationId = await createDM.mutateAsync(otherUserId);
      navigate('/dashboard/messages', {
        state: { openConversation: { type: 'dm', id: conversationId } },
      });
    } catch (error) {
      
    }
  };

  const friendCount = friends?.length || 0;
  const hasFriendRequests =
    !!friendRequests && (friendRequests.received.length > 0 || friendRequests.sent.length > 0);

  return (
    <div className="space-y-6">
      <div className="min-w-0">
        <h1 className="text-3xl font-bold text-foreground leading-tight truncate">My Friends</h1>
        <p className="text-sm text-muted-foreground mt-1 leading-tight truncate">
          Search for users and manage your friends list
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-12">
        <div className="lg:col-span-7">
          <div className="stat-card border-border/80">
            <div>
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-4">
                <div className="flex items-center gap-2 min-w-0">
                  <h3 className="font-semibold text-foreground truncate">Your Friends</h3>
                  <span className="inline-flex shrink-0 items-center rounded-full border border-border bg-secondary/30 px-2 py-0.5 text-xs text-muted-foreground">
                    {friendCount}
                  </span>
                </div>
                <div className="sm:w-[360px]">
                  <Input
                    placeholder="Search friends (name, username, code, id)…"
                    value={friendsFilter}
                    onChange={(e) => setFriendsFilter(e.target.value)}
                  />
                </div>
              </div>
              {isLoadingFriends ? (
                <div className="text-center py-8 text-muted-foreground">Loading...</div>
              ) : !friends || friends.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No friends yet. Use “Search Users” to add your first friend.
                </div>
              ) : (
                <div className="space-y-2">
                  {filteredFriends.length === 0 ? (
                    <div className="text-center py-8 text-muted-foreground">
                      No friends match "{friendsFilter.trim()}"
                    </div>
                  ) : filteredFriends.map((friendship: any) => {
                    const friend = friendship.friend_profile;
                    if (!friend) return null;
                    
                    const displayName = friend.display_name || friend.username;
                    const profilePictureUrl = friend.profile_picture_url;
                    const friendCode = friend.friend_code as string | null | undefined;
                    
                    return (
                      <div
                        key={friendship.id}
                        className="flex flex-col gap-3 rounded-lg border border-transparent p-3 hover:border-border/60 hover:bg-secondary/40 cursor-pointer transition-colors sm:flex-row sm:items-center"
                        onClick={() => setSelectedUserId(friend.user_id)}
                      >
                        <div className="flex items-center gap-3 min-w-0">
                          <Avatar className="h-10 w-10 shrink-0">
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
                              {friendCode ? <span className="ml-2 font-mono text-[11px] text-muted-foreground/80">#{friendCode}</span> : null}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-2 sm:ml-auto sm:shrink-0">
                          <Button
                            size="icon"
                            variant="outline"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleMessageFriend(friend.user_id);
                            }}
                            disabled={createDM.isPending}
                            title="Message"
                          >
                            <MessageSquare className="h-4 w-4" />
                            <span className="sr-only">Message</span>
                          </Button>
                          <Button
                            size="icon"
                            variant="outline"
                            onClick={(e) => {
                              e.stopPropagation();
                              setUnfriendConfirm({ open: true, friendshipId: friendship.id, friendName: displayName });
                            }}
                            disabled={removeFriend.isPending}
                            title="Remove friend"
                          >
                            <UserMinus className="h-4 w-4" />
                            <span className="sr-only">Remove friend</span>
                          </Button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="space-y-6 lg:col-span-5">
          <div className="stat-card">
            <div className="space-y-4">
              <div>
                <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-2">
                  <h3 className="font-semibold text-foreground">Search Users</h3>
                  <div className="flex w-fit items-center gap-1 rounded-lg border border-border bg-secondary/20 p-1">
                    <Button
                      variant={searchMode === 'username' ? 'default' : 'ghost'}
                      size="sm"
                      className="h-8 px-3"
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
                      variant={searchMode === 'friendcode' ? 'default' : 'ghost'}
                      size="sm"
                      className="h-8 px-3"
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
                </div>
                <div className="flex flex-col gap-2 sm:flex-row">
                  <Input
                    placeholder={searchMode === 'username' ? 'Search by username...' : 'Enter friend code...'}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(searchMode === 'friendcode' ? e.target.value.toUpperCase() : e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        handleSearch();
                      }
                    }}
                    className="flex-1 min-w-0"
                    maxLength={searchMode === 'friendcode' ? 6 : undefined}
                  />
                  <Button
                    onClick={handleSearch}
                    disabled={searchUsers.isPending || searchByFriendCode.isPending}
                    className="sm:w-auto"
                  >
                    <Search className="h-4 w-4 mr-2 shrink-0" />
                    <span className="whitespace-nowrap">Search</span>
                  </Button>
                </div>
                <p className="mt-2 text-xs text-muted-foreground">
                  {searchMode === 'friendcode'
                    ? 'Use a friend code to find a specific user quickly.'
                    : 'Search by username to browse users.'}
                </p>
              </div>

              {(searchUsers.data && searchUsers.data.length > 0) || friendCodeResult ? (
                <div className="space-y-2">
                  <p className="text-sm font-medium text-foreground">Search Results</p>
                  <div className="space-y-2 max-h-[340px] overflow-auto pr-1">
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
                <p className="text-sm text-muted-foreground">No users found</p>
              )}
            </div>
          </div>

          {hasFriendRequests && (
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
                            className="flex flex-col gap-3 p-3 rounded-lg bg-secondary/30 border border-border sm:flex-row sm:items-center"
                          >
                            <div className="flex items-center gap-3 min-w-0">
                              <Avatar 
                                className="h-10 w-10 shrink-0 cursor-pointer"
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
                            </div>
                            <div className="flex flex-wrap gap-2 sm:ml-auto sm:justify-end">
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
                            className="flex flex-col gap-3 p-3 rounded-lg hover:bg-secondary/50 cursor-pointer transition-colors sm:flex-row sm:items-center"
                            onClick={() => setSelectedUserId(addressee.user_id)}
                          >
                            <div className="flex items-center gap-3 min-w-0">
                              <Avatar className="h-10 w-10 shrink-0">
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
                            </div>
                            <Button size="sm" variant="outline" disabled className="sm:ml-auto">
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
        </div>
      </div>

      <AlertDialog
        open={unfriendConfirm.open}
        onOpenChange={(open) => setUnfriendConfirm((s) => ({ ...s, open }))}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove friend?</AlertDialogTitle>
            <AlertDialogDescription>
              {unfriendConfirm.friendName ? (
                <>This will remove <strong>{unfriendConfirm.friendName}</strong> from your friends list.</>
              ) : (
                <>This will remove the selected friend from your friends list.</>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={async () => {
                const id = unfriendConfirm.friendshipId;
                setUnfriendConfirm({ open: false });
                if (!id) return;
                try {
                  await removeFriend.mutateAsync(id);
                } catch (e) {
                  
                }
              }}
              disabled={removeFriend.isPending}
            >
              Remove
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
    </div>
  );
}

