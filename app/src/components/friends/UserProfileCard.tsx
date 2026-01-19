import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { Dialog, DialogContent, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { useFriendshipStatus, useSendFriendRequest, useAcceptFriendRequest, useDeclineFriendRequest, useRemoveFriend, useFriendCount } from '@/hooks/useFriends';
import { useAuth } from '@/contexts/AuthContext';
import { supabase } from '@/lib/supabase';
import { format } from 'date-fns';
import { ExternalLink, UserPlus, UserMinus, Check, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { logger } from '@/lib/logger';
import { 
  FaInstagram, 
  FaTwitter, 
  FaDiscord, 
  FaTiktok, 
  FaFacebook, 
  FaSpotify, 
  FaGithub, 
  FaTwitch, 
  FaYoutube, 
  FaReddit 
} from 'react-icons/fa';

interface UserProfileCardProps {
  userId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}


const getPlatformUrl = (platformId: string, username: string): string | null => {
  if (!username) return null;
  
  const cleanUsername = username.replace(/^[@u/]/, '');
  
  const urlMap: Record<string, string> = {
    instagram: `https://instagram.com/${profile.social_links.instagram.startsWith('@') ? profile.social_links.instagram.slice(1) : profile.social_links.instagram}`,
    twitter: `https://twitter.com/${profile.social_links.twitter.startsWith('@') ? profile.social_links.twitter.slice(1) : profile.social_links.twitter}`,
    discord: null, 
    tiktok: `https://tiktok.com/@${profile.social_links.tiktok.startsWith('@') ? profile.social_links.tiktok.slice(1) : profile.social_links.tiktok}`,
    facebook: `https://facebook.com/${profile.social_links.facebook}`,
    spotify: `https://open.spotify.com/user/${profile.social_links.spotify}`,
    github: `https://github.com/${profile.social_links.github}`,
    twitch: `https://twitch.tv/${profile.social_links.twitch}`,
    youtube: `https://youtube.com/${profile.social_links.youtube.startsWith('@') ? profile.social_links.youtube : '@' + profile.social_links.youtube}`,
    reddit: `https://reddit.com/u/${profile.social_links.reddit.startsWith('u/') ? profile.social_links.reddit.slice(2) : profile.social_links.reddit}`,
  };
  
  return urlMap[platformId] || null;
};

export function UserProfileCard({ userId, open, onOpenChange }: UserProfileCardProps): JSX.Element {
  const { user: currentUser } = useAuth();
  
  
  const { data: targetProfile, isLoading: isLoadingTarget } = useQuery({
    queryKey: ['user-profile', userId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('user_profiles')
        .select('*')
        .eq('user_id', userId)
        .single();
      
      if (error) throw error;
      return data;
    },
    enabled: open && !!userId,
  });

  const { data: friendshipStatus } = useFriendshipStatus(userId);
  const sendFriendRequest = useSendFriendRequest();
  const acceptFriendRequest = useAcceptFriendRequest();
  const declineFriendRequest = useDeclineFriendRequest();
  const removeFriend = useRemoveFriend();
  const friendCount = useFriendCount(userId);

  
  const { data: userPicks = [] } = useQuery({
    queryKey: ['userPicks', userId],
    queryFn: async () => {
      if (!userId) return [];

      
      const { data: picksData, error: picksError } = await supabase
        .from('user_picks')
        .select(`
          id,
          owner_id,
          player_id,
          game_id,
          stat_name,
          line_value,
          over_under,
          prediction_id,
          visibility,
          shared_group_id,
          is_active,
          created_at,
          updated_at
        `)
        .eq('owner_id', userId)
        .eq('is_active', true)
        .order('created_at', { ascending: false });

      if (picksError) {
        logger.error('Error fetching user picks', picksError as Error);
        return [];
      }
      if (!picksData || picksData.length === 0) return [];

      
      const playerIds = Array.from(new Set(picksData.map(p => p.player_id)));
      const gameIds = Array.from(new Set(picksData.map(p => p.game_id)));

      
      const { data: statsData, error: statsError } = await supabase
        .from('player_game_stats')
        .select('player_id, game_id, points, rebounds_total, assists, steals, blocks, turnovers, three_pointers_made')
        .in('player_id', playerIds)
        .in('game_id', gameIds);

      if (statsError) {
        logger.error('Error fetching player_game_stats', statsError as Error);
        return picksData.map(p => ({ ...p, result: 'pending' as const }));
      }

      
      const statColumnMap: Record<string, keyof (typeof statsData)[0]> = {
        points: 'points',
        rebounds: 'rebounds_total',
        assists: 'assists',
        steals: 'steals',
        blocks: 'blocks',
        turnovers: 'turnovers',
        threePointersMade: 'three_pointers_made',
      };

      const statsMap = new Map(
        (statsData || []).map(s => [`${s.player_id}-${s.game_id}`, s])
      );

      
      return picksData.map(pick => {
        const stats = statsMap.get(`${pick.player_id}-${pick.game_id}`);
        const statColumn = statColumnMap[pick.stat_name] as keyof (typeof statsData)[0] | undefined;
        const actualStat = stats && statColumn ? (stats[statColumn] as number | null) : null;

        let result: 'win' | 'loss' | 'pending' = 'pending';
        if (actualStat !== null && actualStat !== undefined) {
          if (pick.over_under === 'over') {
            result = actualStat > pick.line_value ? 'win' : 'loss';
          } else {
            result = actualStat < pick.line_value ? 'win' : 'loss';
          }
        }

        return {
          ...pick,
          result,
        };
      });
    },
    enabled: open && !!userId,
  });

  
  const userStats = useMemo(() => {
    const completedPicks = userPicks.filter(p => p.result && p.result !== 'pending');
    const wins = completedPicks.filter(p => p.result === 'win').length;
    const losses = completedPicks.filter(p => p.result === 'loss').length;
    return { wins, losses };
  }, [userPicks]);

  if (isLoadingTarget || !targetProfile) {
    return (
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-md p-0">
          <DialogTitle className="sr-only">Loading Profile</DialogTitle>
          <DialogDescription className="sr-only">Loading user profile information</DialogDescription>
          <div className="p-6 text-center">Loading...</div>
        </DialogContent>
      </Dialog>
    );
  }

  const isOwnProfile = currentUser?.id === userId;
  const displayName = targetProfile.display_name || targetProfile.username;
  const username = targetProfile.username;
  const profilePictureUrl = targetProfile.profile_picture_url;
  const bannerUrl = targetProfile.banner_url;
  const aboutMe = targetProfile.about_me;
  const created_at = targetProfile.created_at;
  const displayUserStats = (targetProfile as any).display_user_stats || false;

  
  const platforms = [
    { id: 'instagram', username: (targetProfile as any).instagram_username, show: (targetProfile as any).show_instagram_on_profile, icon: FaInstagram },
    { id: 'twitter', username: (targetProfile as any).twitter_username, show: (targetProfile as any).show_twitter_on_profile, icon: FaTwitter },
    { id: 'discord', username: (targetProfile as any).discord_username, show: (targetProfile as any).show_discord_on_profile, icon: FaDiscord },
    { id: 'tiktok', username: (targetProfile as any).tiktok_username, show: (targetProfile as any).show_tiktok_on_profile, icon: FaTiktok },
    { id: 'facebook', username: (targetProfile as any).facebook_username, show: (targetProfile as any).show_facebook_on_profile, icon: FaFacebook },
    { id: 'spotify', username: (targetProfile as any).spotify_username, show: (targetProfile as any).show_spotify_on_profile, icon: FaSpotify },
    { id: 'github', username: (targetProfile as any).github_username, show: (targetProfile as any).show_github_on_profile, icon: FaGithub },
    { id: 'twitch', username: (targetProfile as any).twitch_username, show: (targetProfile as any).show_twitch_on_profile, icon: FaTwitch },
    { id: 'youtube', username: (targetProfile as any).youtube_username, show: (targetProfile as any).show_youtube_on_profile, icon: FaYoutube },
    { id: 'reddit', username: (targetProfile as any).reddit_username, show: (targetProfile as any).show_reddit_on_profile, icon: FaReddit },
  ].filter(p => p.username && p.show);

  const handleFriendAction = async () => {
    if (!friendshipStatus) return;

    if (friendshipStatus.status === 'none') {
      await sendFriendRequest.mutateAsync(userId);
    } else if (friendshipStatus.status === 'pending') {
      if (friendshipStatus.isRequester) {
        
      } else {
        await acceptFriendRequest.mutateAsync(friendshipStatus.friendshipId!);
      }
    }
  };

  const handleRemoveFriend = async () => {
    if (!friendshipStatus?.friendshipId) return;
    if (confirm(`Are you sure you want to remove ${displayName} from your friends?`)) {
      await removeFriend.mutateAsync(friendshipStatus.friendshipId);
      onOpenChange(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md p-0 overflow-hidden">
        <DialogTitle className="sr-only">{displayName}'s Profile</DialogTitle>
        <DialogDescription className="sr-only">View {displayName}'s profile information, social links, and friend status</DialogDescription>
        <div
          className="relative w-full bg-gradient-to-br from-primary to-accent"
          style={{ aspectRatio: '3 / 1' }}
        >
          {bannerUrl && (
            <img
              src={bannerUrl}
              alt="Banner"
              className="absolute inset-0 w-full h-full"
              style={{ objectFit: 'cover', objectPosition: 'center' }}
            />
          )}
        </div>

        <div className="p-6 pt-0">
          <div className="relative -mt-12 mb-4">
            <div className="relative w-24 h-24 rounded-full overflow-hidden ring-4 ring-background">
              {profilePictureUrl ? (
                <img src={profilePictureUrl} alt={displayName} className="w-full h-full object-cover" />
              ) : (
                <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-primary to-accent text-white text-2xl font-bold">
                  {getInitials(displayName)}
                </div>
              )}
            </div>
          </div>

          <div>
            <h3 className="text-xl font-bold text-foreground">{displayName}</h3>
            <p className="text-sm text-muted-foreground">@{username}</p>
            
            <div className="mt-5 mb-6 pb-5 border-b border-border">
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <div className="text-lg font-semibold text-foreground leading-tight">{friendCount}</div>
                  <div className="text-xs text-muted-foreground mt-0.5">{friendCount === 1 ? 'Friend' : 'Friends'}</div>
                </div>
                
                {displayUserStats && (userStats.wins > 0 || userStats.losses > 0) ? (
                  <div>
                    <div className="text-lg font-semibold text-foreground leading-tight">
                      {userStats.wins}W / {userStats.losses}L
                    </div>
                    <div className="text-xs text-muted-foreground mt-0.5">Record</div>
                  </div>
                ) : (
                  <div>
                    <div className="text-lg font-semibold text-foreground leading-tight">-</div>
                    <div className="text-xs text-muted-foreground mt-0.5">Record</div>
                  </div>
                )}
                
                {created_at ? (
                  <div>
                    <div className="text-lg font-semibold text-foreground leading-tight">
                      {format(new Date(created_at), 'MMM d, yyyy')}
                    </div>
                    <div className="text-xs text-muted-foreground mt-0.5">Joined</div>
                  </div>
                ) : (
                  <div>
                    <div className="text-lg font-semibold text-foreground leading-tight">-</div>
                    <div className="text-xs text-muted-foreground mt-0.5">Joined</div>
                  </div>
                )}
              </div>
            </div>

            {!isOwnProfile && friendshipStatus && (
              <div className="mt-4">
                {friendshipStatus.status === 'none' && (
                  <Button
                    size="sm"
                    onClick={handleFriendAction}
                    disabled={sendFriendRequest.isPending}
                    className="w-full"
                  >
                    <UserPlus className="h-4 w-4 mr-2" />
                    Add Friend
                  </Button>
                )}
                {friendshipStatus.status === 'pending' && (
                  <div className="flex gap-2">
                    {friendshipStatus.isRequester ? (
                      <Button size="sm" variant="outline" className="w-full" disabled>
                        Requested
                      </Button>
                    ) : (
                      <>
                        <Button
                          size="sm"
                          onClick={() => acceptFriendRequest.mutate(friendshipStatus.friendshipId!)}
                          disabled={acceptFriendRequest.isPending}
                          className="flex-1"
                        >
                          <Check className="h-4 w-4 mr-2" />
                          Accept
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => declineFriendRequest.mutate(friendshipStatus.friendshipId!)}
                          disabled={declineFriendRequest.isPending}
                          className="flex-1"
                        >
                          <X className="h-4 w-4 mr-2" />
                          Decline
                        </Button>
                      </>
                    )}
                  </div>
                )}
                {friendshipStatus.status === 'accepted' && (
                  <Button 
                    size="sm" 
                    variant="outline" 
                    className="w-full text-destructive hover:text-destructive hover:bg-destructive/10"
                    onClick={handleRemoveFriend}
                    disabled={removeFriend.isPending}
                  >
                    <UserMinus className="h-4 w-4 mr-2" />
                    {removeFriend.isPending ? 'Removing...' : 'Remove Friend'}
                  </Button>
                )}
              </div>
            )}

            {aboutMe && (
              <div className="mb-4 pb-5 border-b border-border">
                <div className="text-xs text-muted-foreground mb-2 uppercase tracking-wider font-medium">About</div>
                <div className="text-sm text-muted-foreground leading-relaxed">
                  {aboutMe}
                </div>
              </div>
            )}

            {platforms.length > 0 && (
              <div>
                <p className="text-xs text-muted-foreground mb-3 uppercase tracking-wider font-medium">Connections</p>
                <div className="space-y-2">
                  {platforms.map((platform) => {
                    const IconComponent = platform.icon;
                    const url = getPlatformUrl(platform.id, platform.username);
                    const displayUsername = platform.username.replace(/^[@u/]/, '');
                    return (
                      <a
                        key={platform.id}
                        href={url || '#'}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-3 text-sm text-foreground hover:text-primary transition-colors group"
                        onClick={(e) => {
                          if (!url) e.preventDefault();
                        }}
                      >
                        <div className="flex-shrink-0 w-5 h-5 flex items-center justify-center">
                          <IconComponent className="h-5 w-5" />
                        </div>
                        <span className="flex-1">{displayUsername}</span>
                        {url && (
                          <ExternalLink className="h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors opacity-0 group-hover:opacity-100" />
                        )}
                      </a>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
