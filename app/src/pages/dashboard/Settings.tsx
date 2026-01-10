import { useState, useRef, useEffect, useMemo, useCallback } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { useTheme, ThemeMode, UIDensity } from '@/contexts/ThemeContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Slider } from '@/components/ui/slider';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Textarea } from '@/components/ui/textarea';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import DOMPurify from 'dompurify';

import { toast } from 'sonner';
import { format } from 'date-fns';
import { User, Palette, Bell, Database, Save, Eye, EyeOff, Check, RefreshCw, Sun, Moon, Pencil, X, Upload, Trash2, AlertTriangle, ExternalLink, Hash, Copy, Monitor } from 'lucide-react';
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
import { cn, getInitials } from '@/lib/utils';
import { useUserProfile, useUpdateUserProfile, useCheckUsernameAvailability } from '@/hooks/useUserProfile';
import { useFriendCount } from '@/hooks/useFriends';
import { useUserPicks } from '@/hooks/useUserPicks';
import { uploadProfilePicture, uploadBanner, deleteProfilePicture, deleteBanner, validateImageFile } from '@/lib/imageUpload';
import { supabase } from '@/lib/supabase';
import { useNotifications } from '@/contexts/NotificationContext';
import { useDoNotDisturb } from '@/contexts/DoNotDisturbContext';
import { useCache } from '@/contexts/CacheContext';
import type { CacheRetentionDays } from '@/lib/cache';
import { ImageCropper } from '@/components/ui/image-cropper';
import { FriendsListModal } from '@/components/friends/FriendsListModal';
import { getSkinTonePreference, setSkinTonePreference } from '@/lib/emojiUtils';
import type { SkinTone } from '@/lib/emojiData';
import { CacheManagementModal } from '@/components/settings/CacheManagementModal';
import { DeviceManagement } from '@/components/settings/DeviceManagement';
import { ExportDataSection } from '@/components/settings/ExportDataSection';
import { DangerZone } from '@/components/settings/DangerZone';
import { getAvailableSounds, type NotificationSoundType } from '@/lib/notificationSounds';
import { Volume2, FolderOpen, FileText, Image as ImageIcon, CheckCircle2, Info, Database as DatabaseIcon } from 'lucide-react';
import { logger } from '@/lib/logger';


const SettingsSection = ({ title, description, children }: { title: string; description?: string; children: React.ReactNode }) => (
  <div className="stat-card space-y-4">
    <div>
      <h3 className="font-semibold text-foreground">{title}</h3>
      {description && <p className="text-sm text-muted-foreground">{description}</p>}
    </div>
    {children}
  </div>
);

export default function Settings() {
  const { user } = useAuth();
  const { theme, setTheme, density, setDensity, fontScale, setFontScale, zoomLevel, setZoomLevel, dateFormat, setDateFormat, timeFormat, setTimeFormat } = useTheme();
  const { settings: notificationSettings, updateSettings: updateNotificationSettings, requestDesktopPermission, hasDesktopPermission, previewSound } = useNotifications();
  const { isEnabled: doNotDisturb, enable, disable } = useDoNotDisturb();
  const { retentionDays, setRetentionDays, modelPerfRetentionDays, setModelPerfRetentionDays, storageUsage, cacheCounts, clearCache, refreshStats, getAllCacheEntries, deleteCacheEntries, getAllModelPerformanceEntries, deleteModelPerformanceEntries } = useCache();


  const [cacheModalOpen, setCacheModalOpen] = useState(false);
  const [cacheEntries, setCacheEntries] = useState<Array<{
    date: string;
    type: 'prediction' | 'gameResult';
    size: number;
    cachedAt: number;
    models?: string;
  }>>([]);
  const [modelPerfEntries, setModelPerfEntries] = useState<Array<{
    cacheKey: string;
    timePeriod: string;
    stat: string;
    models: string[];
    size: number;
    cachedAt: number;
  }>>([]);


  const [retentionConfirmOpen, setRetentionConfirmOpen] = useState(false);
  const [pendingRetentionDays, setPendingRetentionDays] = useState<CacheRetentionDays | null>(null);
  const [clearCacheConfirmOpen, setClearCacheConfirmOpen] = useState(false);
  const [skinTone, setSkinTone] = useState<SkinTone>(() => getSkinTonePreference());
  
  
  const { data: profile, isLoading: isLoadingProfile } = useUserProfile();
  const updateProfile = useUpdateUserProfile();
  const checkUsername = useCheckUsernameAvailability();
  const friendCount = useFriendCount();
  const [friendsModalOpen, setFriendsModalOpen] = useState(false);
  const { data: userPicks = [] } = useUserPicks();
  
  
  const userStats = useMemo(() => {
    const completedPicks = userPicks.filter(p => p.result && p.result !== 'pending');
    const wins = completedPicks.filter(p => p.result === 'win').length;
    const losses = completedPicks.filter(p => p.result === 'loss').length;
    return { wins, losses };
  }, [userPicks]);
  
  
  const [displayName, setDisplayName] = useState('');
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [displayUserStats, setDisplayUserStats] = useState(false);
  const [showEmail, setShowEmail] = useState(false);
  const [aboutMe, setAboutMe] = useState('');
  const [isEditingDisplayName, setIsEditingDisplayName] = useState(false);
  const [isEditingUsername, setIsEditingUsername] = useState(false);
  const [isEditingEmail, setIsEditingEmail] = useState(false);
  const [isEditingAboutMe, setIsEditingAboutMe] = useState(false);
  const [usernameError, setUsernameError] = useState<string | null>(null);
  const [isCheckingUsername, setIsCheckingUsername] = useState(false);
  const [linkWarningUrl, setLinkWarningUrl] = useState<string | null>(null);

  
  const [profilePictureUrl, setProfilePictureUrl] = useState<string | null>(null);
  const [bannerUrl, setBannerUrl] = useState<string | null>(null);
  const profilePictureInputRef = useRef<HTMLInputElement>(null);
  const bannerInputRef = useRef<HTMLInputElement>(null);
  const displayNameInputRef = useRef<HTMLInputElement>(null);
  const usernameInputRef = useRef<HTMLInputElement>(null);
  const emailInputRef = useRef<HTMLInputElement>(null);
  const aboutMeTextareaRef = useRef<HTMLTextAreaElement>(null);
  
  
  const [cropperOpen, setCropperOpen] = useState(false);
  const [cropperImageSrc, setCropperImageSrc] = useState<string>('');
  const [cropperType, setCropperType] = useState<'profile' | 'banner'>('profile');

  
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showCurrentPassword, setShowCurrentPassword] = useState(false);
  const [showNewPassword, setShowNewPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);

  
  const [instagramUsername, setInstagramUsername] = useState('');
  const [twitterUsername, setTwitterUsername] = useState('');
  const [discordUsername, setDiscordUsername] = useState('');
  const [tiktokUsername, setTiktokUsername] = useState('');
  const [facebookUsername, setFacebookUsername] = useState('');
  const [spotifyUsername, setSpotifyUsername] = useState('');
  const [githubUsername, setGithubUsername] = useState('');
  const [twitchUsername, setTwitchUsername] = useState('');
  const [youtubeUsername, setYoutubeUsername] = useState('');
  const [redditUsername, setRedditUsername] = useState('');
  
  const [showInstagramOnProfile, setShowInstagramOnProfile] = useState(false);
  const [showTwitterOnProfile, setShowTwitterOnProfile] = useState(false);
  const [showDiscordOnProfile, setShowDiscordOnProfile] = useState(false);
  const [showTiktokOnProfile, setShowTiktokOnProfile] = useState(false);
  const [showFacebookOnProfile, setShowFacebookOnProfile] = useState(false);
  const [showSpotifyOnProfile, setShowSpotifyOnProfile] = useState(false);
  const [showGithubOnProfile, setShowGithubOnProfile] = useState(false);
  const [showTwitchOnProfile, setShowTwitchOnProfile] = useState(false);
  const [showYoutubeOnProfile, setShowYoutubeOnProfile] = useState(false);
  const [showRedditOnProfile, setShowRedditOnProfile] = useState(false);
  
  const [isSavingConnections, setIsSavingConnections] = useState(false);
  const [activeConnections, setActiveConnections] = useState<Array<{ id: string; platform: typeof platforms[0] }>>([]);
  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);

  
  useEffect(() => {
    
    if (typeof window !== 'undefined' && window.electron?.getAppSettings) {
      setIsLoadingAppSettings(true);
      
      setTimeout(() => {
        window.electron.getAppSettings()
          .then((settings) => {
            setAppSettings(settings);
          })
          .catch((err) => {
            
            
            if (err.message?.includes('No handler registered')) {
              logger.warn('Electron app settings not available - running in web mode or main process not ready');
            } else {
              logger.error('Error loading app settings', err as Error);
            }
            setAppSettings(null); 
          })
          .finally(() => {
            setIsLoadingAppSettings(false);
          });
      }, 500); 
    } else {
      
      setIsLoadingAppSettings(false);
    }
  }, []);

  
  const handleAppSettingChange = async (key: keyof typeof appSettings, value: boolean) => {
    if (!window.electron?.setAppSettings || !appSettings) return;

    const newSettings = { ...appSettings, [key]: value };
    
    
    if (key === 'minimizeToTray' && !value) {
      newSettings.startMinimized = false;
    }
    
    setAppSettings(newSettings);

    try {
      const settingsToUpdate: Partial<typeof appSettings> = { [key]: value };
      
      
      if (key === 'minimizeToTray' && !value) {
        settingsToUpdate.startMinimized = false;
      }
      
      await window.electron.setAppSettings(settingsToUpdate);
      
      
      const updatedSettings = await window.electron.getAppSettings();
      setAppSettings(updatedSettings);
      
      
      if (key === 'hardwareAcceleration') {
        toast.info('Hardware acceleration setting will take effect after you restart the app.');
      } else {
        toast.success('Setting updated successfully');
      }
    } catch (err) {
      logger.error('Error updating app setting', err as Error);
      toast.error('Failed to update setting');
      
      setAppSettings(appSettings);
    }
  };

  
  const getDisplayedState = (state: boolean) => {
    
    return notificationSettings.enabled ? state : false;
  };

  
  const handleRetentionChange = (value: string) => {
    const newRetention: CacheRetentionDays = value === 'all' ? 'all' : parseInt(value) as CacheRetentionDays;

    
    const currentDays = retentionDays === 'all' ? Infinity : retentionDays;
    const newDays = newRetention === 'all' ? Infinity : newRetention;

    if (newDays < currentDays) {
      
      setPendingRetentionDays(newRetention);
      setRetentionConfirmOpen(true);
    } else {
      
      setRetentionDays(newRetention);
    }
  };

  
  const confirmRetentionChange = async () => {
    if (pendingRetentionDays !== null) {
      try {
        setRetentionDays(pendingRetentionDays);
        setRetentionConfirmOpen(false);
        setPendingRetentionDays(null);
        toast.success('Cache retention updated');
      } catch (error) {
        toast.error('Failed to update cache retention');
        logger.error('Failed to update cache retention', error as Error);
      }
    }
  };

  
  const cancelRetentionChange = () => {
    setRetentionConfirmOpen(false);
    setPendingRetentionDays(null);
  };
  
  
  const handleDesktopNotificationToggle = async (enabled: boolean) => {
    if (enabled && hasDesktopPermission !== true) {
      const granted = await requestDesktopPermission();
      if (!granted) {
        toast.error('Desktop notification permission was denied. Please enable it in your browser settings.');
        return;
      }
    }
    updateNotificationSettings({ desktop: enabled });
  };

  
  const [defaultTimeWindow, setDefaultTimeWindow] = useState('L10');
  const [defaultStat, setDefaultStat] = useState('points');
  const [defaultConfidence, setDefaultConfidence] = useState('all');
  const [autoRefresh, setAutoRefresh] = useState('never');
  
  
  const [errorLoggingEnabled, setErrorLoggingEnabled] = useState(true);
  const [errorLogFolder, setErrorLogFolder] = useState<string>('');
  const [errorLogLevel, setErrorLogLevel] = useState<'debug' | 'info' | 'warn' | 'error'>('error');

  
  const [exportFolder, setExportFolder] = useState<string>('');

  
  const [appSettings, setAppSettings] = useState<{
    hardwareAcceleration: boolean;
    minimizeToTray: boolean;
    startWithSystem: boolean;
    startMinimized: boolean;
    alwaysOnTop: boolean;
  } | null>(null);
  const [isLoadingAppSettings, setIsLoadingAppSettings] = useState(false);

  
  const [discordRichPresenceEnabled, setDiscordRichPresenceEnabled] = useState(false);

  
  useEffect(() => {
    if (profile) {
      setDisplayName(profile.display_name || profile.username || '');
      setUsername(profile.username || '');
      setAboutMe(profile.about_me || '');
      setProfilePictureUrl(profile.profile_picture_url);
      setBannerUrl(profile.banner_url);
      setDisplayUserStats((profile as any).display_user_stats || false);
      setDiscordRichPresenceEnabled((profile as any).discord_rich_presence_enabled || false);
      
      
      if (profile.default_time_window) {
        setDefaultTimeWindow(profile.default_time_window);
      }
      if (profile.default_stat) {
        setDefaultStat(profile.default_stat);
      }
      if (profile.default_confidence_filter) {
        setDefaultConfidence(profile.default_confidence_filter);
      }
      if (profile.auto_refresh_interval) {
        setAutoRefresh(profile.auto_refresh_interval);
        localStorage.setItem('courtvision-auto-refresh-interval', profile.auto_refresh_interval);
      } else {
        const defaultInterval = 'never';
        setAutoRefresh(defaultInterval);
        localStorage.setItem('courtvision-auto-refresh-interval', defaultInterval);
      }
      
      
      if ((profile as any).error_logging_enabled !== undefined) {
        setErrorLoggingEnabled((profile as any).error_logging_enabled !== null ? (profile as any).error_logging_enabled : true);
      }
      
      
      try {
        const loggerConfig = localStorage.getItem('courtvision-logger-config');
        if (loggerConfig) {
          const config = JSON.parse(loggerConfig);
          setErrorLogFolder(config.logFolder || '');
          setErrorLogLevel(config.logLevel || 'error');
        }
      } catch (e) {
        logger.warn('Failed to load error logging config from localStorage', { error: e });
      }

      
      try {
        const storedExportFolder = localStorage.getItem('courtvision-export-folder');
        if (storedExportFolder) {
          setExportFolder(storedExportFolder);
        }
      } catch (e) {
        logger.warn('Failed to load export folder from localStorage', { error: e });
      }

      
      try {
        const loggerConfig = {
          enabled: (profile as any).error_logging_enabled !== null ? (profile as any).error_logging_enabled : true,
          logFolder: errorLogFolder || '',
          logLevel: errorLogLevel || 'error',
        };
        localStorage.setItem('courtvision-logger-config', JSON.stringify(loggerConfig));
        logger.reloadConfig();
      } catch (e) {
        logger.warn('Failed to sync error logging config to localStorage', { error: e });
      }
      
      setInstagramUsername((profile as any).instagram_username || '');
      setTwitterUsername((profile as any).twitter_username || '');
      setDiscordUsername((profile as any).discord_username || '');
      setTiktokUsername((profile as any).tiktok_username || '');
      setFacebookUsername((profile as any).facebook_username || '');
      setSpotifyUsername((profile as any).spotify_username || '');
      setGithubUsername((profile as any).github_username || '');
      setTwitchUsername((profile as any).twitch_username || '');
      setYoutubeUsername((profile as any).youtube_username || '');
      setRedditUsername((profile as any).reddit_username || '');
      
      setShowInstagramOnProfile((profile as any).show_instagram_on_profile || false);
      setShowTwitterOnProfile((profile as any).show_twitter_on_profile || false);
      setShowDiscordOnProfile((profile as any).show_discord_on_profile || false);
      setShowTiktokOnProfile((profile as any).show_tiktok_on_profile || false);
      setShowFacebookOnProfile((profile as any).show_facebook_on_profile || false);
      setShowSpotifyOnProfile((profile as any).show_spotify_on_profile || false);
      setShowGithubOnProfile((profile as any).show_github_on_profile || false);
      setShowTwitchOnProfile((profile as any).show_twitch_on_profile || false);
      setShowYoutubeOnProfile((profile as any).show_youtube_on_profile || false);
      setShowRedditOnProfile((profile as any).show_reddit_on_profile || false);
    }
  }, [profile]);

  
  useEffect(() => {
    if (isEditingDisplayName && displayNameInputRef.current) {
      displayNameInputRef.current.focus();
    }
  }, [isEditingDisplayName]);

  useEffect(() => {
    if (isEditingUsername && usernameInputRef.current) {
      usernameInputRef.current.focus();
    }
  }, [isEditingUsername]);

  useEffect(() => {
    if (isEditingEmail && emailInputRef.current) {
      emailInputRef.current.focus();
    }
  }, [isEditingEmail]);

  useEffect(() => {
    if (isEditingAboutMe && aboutMeTextareaRef.current) {
      aboutMeTextareaRef.current.focus();
      
      const textarea = aboutMeTextareaRef.current;
      const length = textarea.value.length;
      textarea.setSelectionRange(length, length);
    }
  }, [isEditingAboutMe]);

  useEffect(() => {
    if (user) {
      setEmail(user.email || '');
    }
  }, [user]);

  
  const defaultProfileBg = 'bg-gradient-to-br from-primary to-accent';

  
  const handleProfilePictureSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !user) return;

    const validationError = await validateImageFile(file, 3);
    if (validationError) {
      toast.error(validationError);
      return;
    }

    
    const imageUrl = URL.createObjectURL(file);
    setCropperImageSrc(imageUrl);
    setCropperType('profile');
    setCropperOpen(true);
    
    
    if (profilePictureInputRef.current) {
      profilePictureInputRef.current.value = '';
    }
  };

  
  const handleProfilePictureCropComplete = async (croppedBlob: Blob) => {
    if (!user) return;

    try {
      
      const file = new File([croppedBlob], 'profile-picture.jpg', { type: 'image/jpeg' });

      
      if (profilePictureUrl) {
        try {
          await deleteProfilePicture(profilePictureUrl);
        } catch (err) {
          logger.warn('Error deleting old profile picture', { error: err });
        }
      }

      const url = await uploadProfilePicture(file, user.id);
      setProfilePictureUrl(url);
      await updateProfile.mutateAsync({ profile_picture_url: url });
      toast.success('Profile picture updated');
      
      
      URL.revokeObjectURL(cropperImageSrc);
    } catch (error: any) {
      logger.error('Error uploading profile picture', error as Error);
      toast.error(error.message || 'Failed to upload profile picture');
    }
  };

  
  const handleBannerSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !user) return;

    const validationError = await validateImageFile(file, 10); 
    if (validationError) {
      toast.error(validationError);
      return;
    }

    
    const imageUrl = URL.createObjectURL(file);
    setCropperImageSrc(imageUrl);
    setCropperType('banner');
    setCropperOpen(true);
    
    
    if (bannerInputRef.current) {
      bannerInputRef.current.value = '';
    }
  };

  
  const handleBannerCropComplete = async (croppedBlob: Blob) => {
    if (!user) return;

    try {
      
      const file = new File([croppedBlob], 'banner.jpg', { type: 'image/jpeg' });

      
      if (bannerUrl) {
        try {
          await deleteBanner(bannerUrl);
        } catch (err) {
          logger.warn('Error deleting old banner', { error: err });
        }
      }

      const url = await uploadBanner(file, user.id);
      setBannerUrl(url);
      await updateProfile.mutateAsync({ banner_url: url });
      toast.success('Banner updated');
      
      
      URL.revokeObjectURL(cropperImageSrc);
    } catch (error: any) {
      logger.error('Error uploading banner', error as Error);
      toast.error(error.message || 'Failed to upload banner');
    }
  };

  
  const handleRemoveProfilePicture = async () => {
    if (!profilePictureUrl || !user) return;

    try {
      await deleteProfilePicture(profilePictureUrl);
      setProfilePictureUrl(null);
      await updateProfile.mutateAsync({ profile_picture_url: null });
      toast.success('Profile picture removed');
    } catch (error: any) {
      logger.error('Error removing profile picture', error as Error);
      toast.error('Failed to remove profile picture');
    }
  };

  
  const handleRemoveBanner = async () => {
    if (!bannerUrl || !user) return;

    try {
      await deleteBanner(bannerUrl);
      setBannerUrl(null);
      await updateProfile.mutateAsync({ banner_url: null });
      toast.success('Banner removed');
    } catch (error: any) {
      logger.error('Error removing banner', error as Error);
      toast.error('Failed to remove banner');
    }
  };

  
  const handleUsernameChange = async (newUsername: string) => {
    setUsername(newUsername);
    setUsernameError(null);

    if (!newUsername || newUsername === profile?.username) {
      return;
    }

    
    if (newUsername.length < 3) {
      setUsernameError('Username must be at least 3 characters');
      return;
    }

    if (!/^[a-zA-Z0-9._-]+$/.test(newUsername)) {
      setUsernameError('Username can only contain letters, numbers, dots, dashes, and underscores');
      return;
    }

    setIsCheckingUsername(true);
    try {
      const isAvailable = await checkUsername.mutateAsync(newUsername);
      if (!isAvailable) {
        setUsernameError('Username is already taken');
      }
    } catch (error) {
      logger.error('Error checking username', error as Error);
    } finally {
      setIsCheckingUsername(false);
    }
  };

  
  const handleSaveDisplayName = async () => {
    if (!displayName.trim()) {
      toast.error('Display name cannot be empty');
      return;
    }
    try {
      await updateProfile.mutateAsync({ display_name: displayName.trim() });
      setIsEditingDisplayName(false);
    } catch (error) {
      
    }
  };

  
  const handleSaveUsername = async () => {
    if (usernameError || !username.trim()) {
      return;
    }
    if (username === profile?.username) {
      setIsEditingUsername(false);
      return;
    }
    try {
      await updateProfile.mutateAsync({ username: username.trim() });
      setIsEditingUsername(false);
      setUsernameError(null);
    } catch (error) {
      
    }
  };

  
  const handleSaveEmail = async () => {
    if (!user) return;
    
    try {
      const { error } = await supabase.auth.updateUser({ email: email });
      if (error) throw error;
      toast.success('Email update requested. Please check your new email for confirmation.');
      setIsEditingEmail(false);
    } catch (error: any) {
      logger.error('Error updating email', error as Error);
      toast.error(error.message || 'Failed to update email');
    }
  };

  
  const handleSaveAboutMe = async () => {
    try {
      await updateProfile.mutateAsync({ about_me: aboutMe });
      setIsEditingAboutMe(false);
    } catch (error) {
      
    }
  };

  
  const handleSaveDisplayUserStats = async (newValue: boolean) => {
    try {
      await updateProfile.mutateAsync({ display_user_stats: newValue } as any);
    } catch (error) {
      
    }
  };

  
  const handleAboutMeLinkClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.target as HTMLElement;
    if (target.tagName === 'A') {
      e.preventDefault();
      const url = (target as HTMLAnchorElement).href;
      setLinkWarningUrl(url);
    }
  };

  
  const renderAboutMe = (text: string) => {
    if (!text) return null;

    
    const linkRegex = /\[([^\]]+)\]\(([^)]+)\)/g;
    const boldRegex = /\*\*([^*]+)\*\*/g;
    const italicRegex = /\*([^*]+)\*/g;

    let html = text
      .replace(/\n/g, '<br />')
      .replace(linkRegex, '<a href="$2" class="text-primary hover:underline" target="_blank" rel="noopener noreferrer">$1</a>')
      .replace(boldRegex, '<strong>$1</strong>')
      .replace(italicRegex, '<em>$1</em>');

    
    
    const sanitizedHtml = DOMPurify.sanitize(html, {
      ALLOWED_TAGS: ['a', 'strong', 'em', 'br'],
      ALLOWED_ATTR: ['href', 'target', 'rel', 'class'],
      ALLOWED_URI_REGEXP: /^(?:(?:https?|mailto):)/i, 
    });

    return <div dangerouslySetInnerHTML={{ __html: sanitizedHtml }} />;
  };

  const handleSaveAccount = async () => {
    if (!currentPassword || !newPassword || !confirmPassword) {
      toast.error('Please fill in all password fields');
      return;
    }

    if (newPassword !== confirmPassword) {
      toast.error('New passwords do not match');
      return;
    }

    if (newPassword.length < 6) {
      toast.error('Password must be at least 6 characters long');
      return;
    }

    try {
      
      const { error } = await supabase.auth.updateUser({
        password: newPassword,
      });

      if (error) throw error;

      toast.success('Password updated successfully');
      
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (error: any) {
      logger.error('Error updating password', error as Error);
      toast.error(error.message || 'Failed to update password');
    }
  };

  
  const handleSaveConnections = async () => {
    setIsSavingConnections(true);
    try {
      await updateProfile.mutateAsync({
        instagram_username: instagramUsername || null,
        twitter_username: twitterUsername || null,
        discord_username: discordUsername || null,
        tiktok_username: tiktokUsername || null,
        facebook_username: facebookUsername || null,
        spotify_username: spotifyUsername || null,
        github_username: githubUsername || null,
        twitch_username: twitchUsername || null,
        youtube_username: youtubeUsername || null,
        reddit_username: redditUsername || null,
        show_instagram_on_profile: showInstagramOnProfile,
        show_twitter_on_profile: showTwitterOnProfile,
        show_discord_on_profile: showDiscordOnProfile,
        show_tiktok_on_profile: showTiktokOnProfile,
        show_facebook_on_profile: showFacebookOnProfile,
        show_spotify_on_profile: showSpotifyOnProfile,
        show_github_on_profile: showGithubOnProfile,
        show_twitch_on_profile: showTwitchOnProfile,
        show_youtube_on_profile: showYoutubeOnProfile,
        show_reddit_on_profile: showRedditOnProfile,
      } as any);
    } catch (error) {
      
    } finally {
      setIsSavingConnections(false);
    }
  };

  
  const platforms = [
    { 
      id: 'instagram', 
      label: 'Instagram', 
      prefix: '@', 
      placeholder: 'username', 
      maxLength: 30,
      username: instagramUsername,
      setUsername: setInstagramUsername,
      showOnProfile: showInstagramOnProfile,
      setShowOnProfile: setShowInstagramOnProfile,
      icon: FaInstagram
    },
    { 
      id: 'twitter', 
      label: 'Twitter / X', 
      prefix: '@', 
      placeholder: 'username', 
      maxLength: 15,
      username: twitterUsername,
      setUsername: setTwitterUsername,
      showOnProfile: showTwitterOnProfile,
      setShowOnProfile: setShowTwitterOnProfile,
      icon: FaTwitter
    },
    { 
      id: 'discord', 
      label: 'Discord', 
      prefix: '', 
      placeholder: 'username#1234', 
      maxLength: 37,
      username: discordUsername,
      setUsername: setDiscordUsername,
      showOnProfile: showDiscordOnProfile,
      setShowOnProfile: setShowDiscordOnProfile,
      icon: FaDiscord,
      helpText: 'Include your discriminator (e.g., username#1234)'
    },
    { 
      id: 'tiktok', 
      label: 'TikTok', 
      prefix: '@', 
      placeholder: 'username', 
      maxLength: 24,
      username: tiktokUsername,
      setUsername: setTiktokUsername,
      showOnProfile: showTiktokOnProfile,
      setShowOnProfile: setShowTiktokOnProfile,
      icon: FaTiktok
    },
    { 
      id: 'facebook', 
      label: 'Facebook', 
      prefix: '', 
      placeholder: 'username or profile ID', 
      maxLength: 50,
      username: facebookUsername,
      setUsername: setFacebookUsername,
      showOnProfile: showFacebookOnProfile,
      setShowOnProfile: setShowFacebookOnProfile,
      icon: FaFacebook
    },
    { 
      id: 'spotify', 
      label: 'Spotify', 
      prefix: '', 
      placeholder: 'username', 
      maxLength: 30,
      username: spotifyUsername,
      setUsername: setSpotifyUsername,
      showOnProfile: showSpotifyOnProfile,
      setShowOnProfile: setShowSpotifyOnProfile,
      icon: FaSpotify
    },
    { 
      id: 'github', 
      label: 'GitHub', 
      prefix: '@', 
      placeholder: 'username', 
      maxLength: 39,
      username: githubUsername,
      setUsername: setGithubUsername,
      showOnProfile: showGithubOnProfile,
      setShowOnProfile: setShowGithubOnProfile,
      icon: FaGithub
    },
    { 
      id: 'twitch', 
      label: 'Twitch', 
      prefix: '', 
      placeholder: 'username', 
      maxLength: 25,
      username: twitchUsername,
      setUsername: setTwitchUsername,
      showOnProfile: showTwitchOnProfile,
      setShowOnProfile: setShowTwitchOnProfile,
      icon: FaTwitch
    },
    { 
      id: 'youtube', 
      label: 'YouTube', 
      prefix: '@', 
      placeholder: 'username or channel ID', 
      maxLength: 30,
      username: youtubeUsername,
      setUsername: setYoutubeUsername,
      showOnProfile: showYoutubeOnProfile,
      setShowOnProfile: setShowYoutubeOnProfile,
      icon: FaYoutube
    },
    { 
      id: 'reddit', 
      label: 'Reddit', 
      prefix: 'u/', 
      placeholder: 'username', 
      maxLength: 20,
      username: redditUsername,
      setUsername: setRedditUsername,
      showOnProfile: showRedditOnProfile,
      setShowOnProfile: setShowRedditOnProfile,
      icon: FaReddit
    },
  ];

  
  const getPlatformUrl = (platformId: string, username: string): string | null => {
    if (!username) return null;
    
    const urlMap: Record<string, (u: string) => string> = {
      instagram: (u) => `https://instagram.com/${u.startsWith('@') ? u.slice(1) : u}`,
      twitter: (u) => `https://twitter.com/${u.startsWith('@') ? u.slice(1) : u}`,
      discord: (u) => u.startsWith('http') ? u : `https://discord.gg/${u}`,
      tiktok: (u) => `https://tiktok.com/@${u.startsWith('@') ? u.slice(1) : u}`,
      facebook: (u) => `https://facebook.com/${u}`,
      spotify: (u) => `https://open.spotify.com/user/${u}`,
      github: (u) => `https://github.com/${u}`,
      twitch: (u) => `https://twitch.tv/${u}`,
      youtube: (u) => {
        
        if (u.startsWith('@')) {
          return `https://youtube.com/@${u.slice(1)}`;
        }
        return `https://youtube.com/c/${u}`;
      },
      reddit: (u) => `https://reddit.com/u/${u.startsWith('u/') ? u.slice(2) : u}`,
    };
    
    const urlBuilder = urlMap[platformId];
    return urlBuilder ? urlBuilder(username) : null;
  };

  
  const visiblePlatforms = platforms.filter(
    (platform) => platform.showOnProfile && platform.username
  );

  
  useEffect(() => {
    
    if (!profile) return;
    
    const active: Array<{ id: string; platform: typeof platforms[0] }> = [];
    platforms.forEach((platform) => {
      if (platform.username) {
        active.push({ id: platform.id, platform });
      }
    });
    setActiveConnections(active);
  }, [
    instagramUsername, twitterUsername, discordUsername, tiktokUsername,
    facebookUsername, spotifyUsername, githubUsername, twitchUsername,
    youtubeUsername, redditUsername, profile
  ]);

  
  const handleAddConnection = (platformId: string) => {
    const platform = platforms.find(p => p.id === platformId);
    if (!platform) return;
    
    
    if (activeConnections.some(ac => ac.id === platformId)) return;
    
    setActiveConnections([...activeConnections, { id: platformId, platform }]);
  };

  
  const handleRemoveConnection = (platformId: string) => {
    setActiveConnections(activeConnections.filter(ac => ac.id !== platformId));
    
    const platform = platforms.find(p => p.id === platformId);
    if (platform) {
      platform.setUsername('');
      platform.setShowOnProfile(false);
    }
  };

  
  const handleDragStart = (index: number) => {
    setDraggedIndex(index);
  };

  const handleDragOver = (e: React.DragEvent, index: number) => {
    e.preventDefault();
    if (draggedIndex === null) return;
    
    if (draggedIndex !== index) {
      const newConnections = [...activeConnections];
      const draggedItem = newConnections[draggedIndex];
      newConnections.splice(draggedIndex, 1);
      newConnections.splice(index, 0, draggedItem);
      setActiveConnections(newConnections);
      setDraggedIndex(index);
    }
  };

  const handleDragEnd = () => {
    setDraggedIndex(null);
  };

  const handleSelectLogFolder = async () => {
    if (!window.electron) {
      toast.error('Folder selection is only available in the Electron app');
      return;
    }

    try {
      const folder = await window.electron.selectFolder();
      if (folder) {
        setErrorLogFolder(folder);
        toast.success('Log folder selected');
      }
    } catch (err) {
      toast.error('Failed to select folder');
    }
  };

  const handleSelectExportFolder = async () => {
    if (!window.electron) {
      toast.error('Folder selection is only available in the Electron app');
      return;
    }

    try {
      const folder = await window.electron.selectFolder();
      if (folder) {
        setExportFolder(folder);
        toast.success('Export folder selected');
      }
    } catch (err) {
      toast.error('Failed to select folder');
    }
  };

  const handleSavePreferences = async () => {
    if (!profile) {
      toast.error('Profile not loaded');
      return;
    }

    try {
      
      await updateProfile.mutateAsync({
        default_time_window: defaultTimeWindow,
        default_stat: defaultStat,
        default_confidence_filter: defaultConfidence,
        auto_refresh_interval: autoRefresh,
        error_logging_enabled: errorLoggingEnabled,
      } as any);
      localStorage.setItem('courtvision-auto-refresh-interval', autoRefresh);
      
      
      try {
        const loggerConfig = {
          enabled: errorLoggingEnabled,
          logFolder: errorLogFolder || '',
          logLevel: errorLogLevel,
        };
        localStorage.setItem('courtvision-logger-config', JSON.stringify(loggerConfig));
        
        logger.reloadConfig();
      } catch (e) {
        logger.warn('Failed to sync error logging config to localStorage', { error: e });
      }

      
      try {
        if (exportFolder) {
          localStorage.setItem('courtvision-export-folder', exportFolder);
        } else {
          localStorage.removeItem('courtvision-export-folder');
        }
      } catch (e) {
        logger.warn('Failed to save export folder to localStorage', { error: e });
      }

      toast.success('Preferences saved successfully');
    } catch (err) {
      logger.error('Error saving data preferences', err as Error);
      toast.error('Failed to save preferences');
    }
  };

  const themes: { id: ThemeMode; label: string; color: string; bgClass: string }[] = [
    { id: 'light', label: 'Light', color: 'bg-white', bgClass: 'bg-white border-border' },
    { id: 'dark', label: 'Dark', color: 'bg-zinc-700', bgClass: 'bg-zinc-700' },
    { id: 'midnight', label: 'Midnight', color: 'bg-slate-800', bgClass: 'bg-slate-800' },
    { id: 'amoled', label: 'AMOLED', color: 'bg-black', bgClass: 'bg-black border border-zinc-800' },
    { id: 'sync', label: 'Sync', color: '', bgClass: '' },
  ];

  const fontScaleOptions = [
    { value: 14, label: 'Small' },
    { value: 16, label: 'Medium' },
    { value: 18, label: 'Large' },
  ];
  const zoomOptions = [
    { value: 85, label: 'Small' },
    { value: 100, label: 'Medium' },
    { value: 115, label: 'Large' },
  ];

  
  const displayNameToShow = profile?.display_name || profile?.username || user?.username || 'User';
  const usernameToShow = profile?.username || user?.username || 'user';

  if (isLoadingProfile) {
  return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent mx-auto mb-2"></div>
          <p className="text-muted-foreground">Loading profile...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in max-w-6xl">
      <div className="min-w-0">
        <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Settings</h1>
        <p className="text-muted-foreground leading-tight truncate">Manage your account and preferences</p>
      </div>

      <Tabs defaultValue="account" className="space-y-6">
        <TabsList className="bg-card border border-border">
          <TabsTrigger value="account" className="gap-2">
            <User className="h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Account</span>
          </TabsTrigger>
          {window.electron && (
            <TabsTrigger value="application" className="gap-2">
              <Monitor className="h-4 w-4 shrink-0" />
              <span className="whitespace-nowrap">Application</span>
            </TabsTrigger>
          )}
          <TabsTrigger value="display" className="gap-2">
            <Palette className="h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Display</span>
          </TabsTrigger>
          <TabsTrigger value="notifications" className="gap-2">
            <Bell className="h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Notifications</span>
          </TabsTrigger>
          <TabsTrigger value="data" className="gap-2">
            <Database className="h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Data</span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="account" className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <Tabs defaultValue="profile" className="w-full">
                <TabsList className="grid w-full grid-cols-4">
                  <TabsTrigger value="profile">Profile</TabsTrigger>
                  <TabsTrigger value="connections">Connections</TabsTrigger>
                  <TabsTrigger value="password">Password</TabsTrigger>
                  <TabsTrigger value="security">Security</TabsTrigger>
                </TabsList>

                <TabsContent value="profile" className="space-y-4 mt-4">
                  <SettingsSection title="Profile Information" description="Manage your profile details.">
                    <div className="space-y-6">
              <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <div>
                            <Label>Display Name</Label>
                            <p className="text-xs text-muted-foreground">This is how your name appears to others. It doesn't have to be unique.</p>
                          </div>
                        </div>
                        {isEditingDisplayName ? (
                          <div className="space-y-2">
                <Input
                              ref={displayNameInputRef}
                              dir="ltr"
                              value={displayName}
                              onChange={(e) => setDisplayName(e.target.value)}
                              placeholder="Display Name"
                              maxLength={100}
                            />
                            <div className="flex gap-2">
                              <Button size="sm" onClick={handleSaveDisplayName}>Save</Button>
                              <Button size="sm" variant="outline" onClick={() => {
                                setDisplayName(profile?.display_name || '');
                                setIsEditingDisplayName(false);
                              }}>Cancel</Button>
              </div>
                          </div>
                        ) : (
                          <div className="flex items-center justify-between">
                            <span className="text-foreground">{displayNameToShow}</span>
                            <Button size="sm" variant="outline" onClick={() => setIsEditingDisplayName(true)}>
                              <Pencil className="h-3 w-3 mr-1 shrink-0" />
                              <span className="whitespace-nowrap">Edit</span>
                            </Button>
                          </div>
                        )}
                      </div>

                      
                      <div className="space-y-2 border-t border-border pt-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <Label>Username</Label>
                            <p className="text-xs text-muted-foreground">Your unique username. This is used to identify you and must be unique.</p>
                          </div>
                        </div>
                        {isEditingUsername ? (
              <div className="space-y-2">
                <Input
                              ref={usernameInputRef}
                              dir="ltr"
                  value={username}
                              onChange={(e) => handleUsernameChange(e.target.value)}
                              placeholder="Username"
                              maxLength={50}
                              className={usernameError ? 'border-destructive' : ''}
                            />
                            {usernameError && (
                              <p className="text-sm text-destructive">{usernameError}</p>
                            )}
                            {isCheckingUsername && (
                              <p className="text-sm text-muted-foreground">Checking availability...</p>
                            )}
                            <div className="flex gap-2">
                              <Button 
                                size="sm" 
                                onClick={handleSaveUsername}
                                disabled={!!usernameError || isCheckingUsername}
                              >
                                Save
                              </Button>
                              <Button size="sm" variant="outline" onClick={() => {
                                setUsername(profile?.username || '');
                                setUsernameError(null);
                                setIsEditingUsername(false);
                              }}>Cancel</Button>
              </div>
            </div>
                        ) : (
                          <div className="flex items-center justify-between">
                            <span className="text-foreground">{usernameToShow}</span>
                            <Button size="sm" variant="outline" onClick={() => setIsEditingUsername(true)}>
                              <Pencil className="h-3 w-3 mr-1 shrink-0" />
                              <span className="whitespace-nowrap">Edit</span>
                            </Button>
                          </div>
                        )}
                      </div>

                      
                      <div className="space-y-2 border-t border-border pt-4">
              <div className="flex items-center justify-between">
                          <div>
                            <Label>Email</Label>
                            <p className="text-xs text-muted-foreground">Your email address. You'll need to confirm any changes.</p>
                          </div>
                        </div>
                        {isEditingEmail ? (
                          <div className="space-y-2">
                            <div className="flex items-center gap-2">
                              <Input
                                ref={emailInputRef}
                                dir="ltr"
                                type={showEmail ? 'text' : 'password'}
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                placeholder="Email"
                              />
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => setShowEmail(!showEmail)}
                              >
                                {showEmail ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                              </Button>
                            </div>
                            <div className="flex gap-2">
                              <Button size="sm" onClick={handleSaveEmail}>Save</Button>
                              <Button size="sm" variant="outline" onClick={() => {
                                setEmail(user?.email || '');
                                setIsEditingEmail(false);
                              }}>Cancel</Button>
                            </div>
                          </div>
                        ) : (
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              <span className="text-foreground">
                                {showEmail ? email : (() => {
                                  const parts = email.split('@');
                                  const localPart = parts[0] || '';
                                  const domain = parts[1] || '';
                                  return '•'.repeat(Math.min(localPart.length, 20)) + (domain ? '@' + domain : '');
                                })()}
                              </span>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => setShowEmail(!showEmail)}
                              >
                                {showEmail ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                              </Button>
                            </div>
                            <Button size="sm" variant="outline" onClick={() => setIsEditingEmail(true)}>
                              <Pencil className="h-3 w-3 mr-1 shrink-0" />
                              <span className="whitespace-nowrap">Edit</span>
                            </Button>
                          </div>
                        )}
                      </div>

                      <div className="space-y-2 border-t border-border pt-4">
                        <div className="flex items-center justify-between">
                          <div className="flex-1">
                            <Label>Display User Stats</Label>
                            <p className="text-xs text-muted-foreground">Show your win/loss record on your profile</p>
                          </div>
                          <Switch
                            checked={displayUserStats}
                            onCheckedChange={(checked) => {
                              setDisplayUserStats(checked);
                              handleSaveDisplayUserStats(checked);
                            }}
                          />
                        </div>
                      </div>

                      <div className="space-y-2 border-t border-border pt-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <Label>About Me</Label>
                            <p className="text-xs text-muted-foreground">Tell others about yourself. Supports basic markdown: **bold**, *italic*, [links](url).</p>
                          </div>
                        </div>
                        {isEditingAboutMe ? (
                          <div className="space-y-2">
                            <Textarea
                              ref={aboutMeTextareaRef}
                              value={aboutMe}
                              onChange={(e) => setAboutMe(e.target.value)}
                              placeholder="Write something about yourself..."
                              rows={4}
                              maxLength={500}
                            />
                            <p className="text-xs text-muted-foreground">
                              {aboutMe.length}/500 characters. Use **bold**, *italic*, and [text](url) for links.
                            </p>
                            <div className="flex gap-2">
                              <Button size="sm" onClick={handleSaveAboutMe}>Save</Button>
                              <Button size="sm" variant="outline" onClick={() => {
                                setAboutMe(profile?.about_me || '');
                                setIsEditingAboutMe(false);
                              }}>Cancel</Button>
                            </div>
                          </div>
                        ) : (
                          <div className="space-y-2">
                            {aboutMe ? (
                              <div 
                                className="text-sm text-muted-foreground prose prose-invert max-w-none"
                                onClick={handleAboutMeLinkClick}
                              >
                                {renderAboutMe(aboutMe)}
                              </div>
                            ) : (
                              <p className="text-sm text-muted-foreground italic">No about me set</p>
                            )}
                            <Button size="sm" variant="outline" onClick={() => setIsEditingAboutMe(true)}>
                              <Pencil className="h-3 w-3 mr-1" />
                              {aboutMe ? 'Edit' : 'Add'}
                            </Button>
                          </div>
                        )}
              </div>
            </div>
          </SettingsSection>
                </TabsContent>

                <TabsContent value="connections" className="space-y-4 mt-4">
                  <div className="space-y-6">
                    <div>
                      <h3 className="text-lg font-semibold text-foreground mb-2">Add accounts to your profile</h3>
                      <p className="text-sm text-muted-foreground">
                        This information will not be shared outside of CourtVision without your permission, and is used in accordance with our Privacy Policy.
                      </p>
                    </div>

                    <div>
                      <div className="grid grid-cols-5 gap-3">
                        {platforms.map((platform) => {
                          const IconComponent = platform.icon;
                          const isActive = activeConnections.some(ac => ac.id === platform.id);
                          return (
                            <div
                              key={platform.id}
                              className={cn(
                                "relative w-16 h-16 rounded-full border-2 transition-all cursor-pointer flex items-center justify-center mx-auto",
                                isActive 
                                  ? "border-primary bg-primary/10" 
                                  : "border-border hover:border-primary/50 bg-secondary/30"
                              )}
                              onClick={() => {
                                if (isActive) {
                                  handleRemoveConnection(platform.id);
                                } else {
                                  handleAddConnection(platform.id);
                                }
                              }}
                            >
                              <IconComponent className="h-7 w-7 text-foreground" />
                              {isActive && (
                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleRemoveConnection(platform.id);
                                  }}
                                  className="absolute -top-1 -right-1 h-5 w-5 rounded-full bg-destructive text-destructive-foreground flex items-center justify-center hover:bg-destructive/90 transition-colors border-2 border-background"
                                >
                                  <X className="h-3 w-3" />
                </button>
                              )}
              </div>
                          );
                        })}
                      </div>
                    </div>

                    {activeConnections.length > 0 && (
                      <div>
                        <div className="p-4 border rounded-lg bg-secondary/30 space-y-3">
                          {activeConnections.map((connection, index) => {
                            
                            const currentPlatform = platforms.find(p => p.id === connection.id);
                            if (!currentPlatform) return null;
                            
                            const IconComponent = currentPlatform.icon;
                            
                            return (
                              <div
                                key={connection.id}
                                draggable
                                onDragStart={(e) => {
                                  
                                  const target = e.target as HTMLElement;
                                  if (target.closest('input, button, label, [role="switch"]')) {
                                    e.preventDefault();
                                    return;
                                  }
                                  handleDragStart(index);
                                }}
                                onDragOver={(e) => handleDragOver(e, index)}
                                onDragEnd={handleDragEnd}
                                className={cn(
                                  "flex items-center gap-4 p-4 rounded-lg bg-background border cursor-move transition-all",
                                  draggedIndex === index ? "opacity-50 border-primary" : "hover:border-primary/50",
                                  !currentPlatform.username ? "border-red-500/50 bg-red-500/5" : "border-border"
                                )}
                              >
                                <div className="flex-shrink-0">
                                  <div className="w-10 h-10 rounded-full bg-secondary flex items-center justify-center">
                                    <IconComponent className="h-6 w-6 text-foreground" />
                                  </div>
                                </div>

                                <div className="flex-1 min-w-0">
                                  <div className="space-y-1">
                                    <div className="flex items-center gap-2">
                                      <Input
                                        dir="ltr"
                                        value={currentPlatform.username}
                                        onChange={(e) => currentPlatform.setUsername(e.target.value)}
                                        placeholder={currentPlatform.placeholder}
                                        maxLength={currentPlatform.maxLength}
                                        className={cn(
                                          "h-8 text-sm font-medium bg-transparent border-none p-0 focus-visible:ring-0 focus-visible:ring-offset-0",
                                          !currentPlatform.username && "text-destructive placeholder:text-destructive/50"
                                        )}
                                        onClick={(e) => e.stopPropagation()}
                                        onMouseDown={(e) => e.stopPropagation()}
                                      />
                                      {!currentPlatform.username && (
                                        <span className="text-xs text-destructive flex-shrink-0">*</span>
                                      )}
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <p className="text-xs text-muted-foreground">{currentPlatform.label}</p>
                                      {!currentPlatform.username && (
                                        <span className="text-xs text-destructive">Username required</span>
                                      )}
                                    </div>
                                  </div>
                                </div>

                                <div 
                                  className="flex items-center gap-4 flex-shrink-0"
                                  onMouseDown={(e) => e.stopPropagation()}
                                  onClick={(e) => e.stopPropagation()}
                                >
                                  <div className="flex items-center gap-2">
                                    <Label 
                                      htmlFor={`show-${connection.id}`} 
                                      className="text-xs text-muted-foreground cursor-pointer"
                                      onMouseDown={(e) => e.stopPropagation()}
                                    >
                                      Display on profile
                                    </Label>
                                    <Switch
                                      id={`show-${connection.id}`}
                                      checked={currentPlatform.showOnProfile}
                                      onCheckedChange={(checked) => {
                                        currentPlatform.setShowOnProfile(checked);
                                      }}
                                      disabled={!currentPlatform.username}
                                      onMouseDown={(e) => {
                                        e.stopPropagation();
                                        e.preventDefault();
                                      }}
                                      onClick={(e) => {
                                        e.stopPropagation();
                                      }}
                                    />
                                  </div>
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleRemoveConnection(connection.id);
                                    }}
                                    onMouseDown={(e) => e.stopPropagation()}
                                    className="text-muted-foreground hover:text-destructive transition-colors p-1"
                                  >
                                    <X className="h-4 w-4" />
                                  </button>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}

                    {activeConnections.length === 0 && (
                      <div className="text-center py-12 text-muted-foreground border border-dashed rounded-lg bg-secondary/30">
                        <p className="text-sm">No connections added yet. Click on a platform above to add it.</p>
                      </div>
                    )}

                    <Button 
                      size="sm" 
                      onClick={handleSaveConnections}
                      disabled={isSavingConnections || activeConnections.length === 0 || activeConnections.some(ac => !ac.platform.username)}
                      className="w-full"
                    >
                      {isSavingConnections ? 'Saving...' : 'Save Connections'}
                    </Button>
                  </div>
                </TabsContent>

                <TabsContent value="password" className="space-y-4 mt-4">
                  <SettingsSection title="Change Password" description="Update your account password. You'll need to enter your current password.">
                    <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="currentPassword">Current Password</Label>
                        <div className="relative">
                <Input
                  id="currentPassword"
                            dir="ltr"
                            type={showCurrentPassword ? 'text' : 'password'}
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                            placeholder="Enter your current password"
                            className="pr-10"
                          />
                          <Button
                            variant="ghost"
                            size="sm"
                            type="button"
                            onClick={() => setShowCurrentPassword(!showCurrentPassword)}
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                          >
                            {showCurrentPassword ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                          </Button>
              </div>
                      </div>
                <div className="space-y-2">
                  <Label htmlFor="newPassword">New Password</Label>
                        <div className="relative">
                  <Input
                    id="newPassword"
                            dir="ltr"
                            type={showNewPassword ? 'text' : 'password'}
                    value={newPassword}
                    onChange={(e) => setNewPassword(e.target.value)}
                            placeholder="Enter your new password"
                            className="pr-10"
                          />
                          <Button
                            variant="ghost"
                            size="sm"
                            type="button"
                            onClick={() => setShowNewPassword(!showNewPassword)}
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                          >
                            {showNewPassword ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                          </Button>
                        </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="confirmPassword">Confirm Password</Label>
                        <div className="relative">
                  <Input
                    id="confirmPassword"
                            dir="ltr"
                            type={showConfirmPassword ? 'text' : 'password'}
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                            placeholder="Confirm your new password"
                            className="pr-10"
                          />
                          <Button
                            variant="ghost"
                            size="sm"
                            type="button"
                            onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                          >
                            {showConfirmPassword ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                          </Button>
                </div>
              </div>
                      <Button 
                        size="sm" 
                        onClick={handleSaveAccount}
                        disabled={!currentPassword || !newPassword || !confirmPassword}
                      >
                        Update Password
                      </Button>
            </div>
          </SettingsSection>
                </TabsContent>

                <TabsContent value="security" className="space-y-4 mt-4">
                  <SettingsSection title="Active Sessions" description="Manage devices that are logged into your account. You can log out of any device remotely.">
                    <DeviceManagement />
                  </SettingsSection>
                </TabsContent>
              </Tabs>
            </div>

            <div className="space-y-4">
              <SettingsSection title="Profile Preview">
                <div className="relative rounded-lg overflow-hidden border border-border bg-card">
                  <div
                    className="relative w-full bg-gradient-to-br from-primary to-accent group/banner z-10"
                    style={{ aspectRatio: '3 / 1' }}
                  >
                    {bannerUrl && (
                      <img
                        src={bannerUrl}
                        alt="Banner"
                        className="absolute inset-0 w-full h-full object-cover"
                        style={{ objectFit: 'cover', objectPosition: 'center' }}
                      />
                    )}
                    <div className="absolute inset-0 bg-black/0 group-hover/banner:bg-black/50 flex items-center justify-center gap-2 transition-colors pointer-events-none opacity-0 group-hover/banner:opacity-100">
                      <div className="pointer-events-auto">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => bannerInputRef.current?.click()}
                          className="text-white hover:bg-white/20"
                        >
                          <Upload className="h-4 w-4 mr-2" />
                          Change Banner
          </Button>
                      </div>
                      {bannerUrl && (
                        <div className="pointer-events-auto">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={handleRemoveBanner}
                            className="text-white hover:bg-white/20"
                          >
                            <Trash2 className="h-4 w-4 mr-2" />
                            Remove
                          </Button>
                        </div>
                      )}
                    </div>
                    <input
                      ref={bannerInputRef}
                      type="file"
                      accept="image/*"
                      onChange={handleBannerSelect}
                      className="hidden"
                    />
                  </div>

                  <div className="p-6 pt-0">
                    <div className="relative -mt-12 mb-4">
                      <div className="relative w-24 h-24 rounded-full overflow-hidden ring-4 ring-background group/profile z-20">
                        {profilePictureUrl ? (
                          <img src={profilePictureUrl} alt="Profile" className="w-full h-full object-cover" />
                        ) : (
                          <div className={cn("w-full h-full flex items-center justify-center text-white text-2xl font-bold", defaultProfileBg)}>
                            {getInitials(displayNameToShow)}
                          </div>
                        )}
                        <div className="absolute inset-0 bg-black/0 group-hover/profile:bg-black/50 flex items-center justify-center transition-colors pointer-events-none opacity-0 group-hover/profile:opacity-100">
                          <div className="pointer-events-auto">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => profilePictureInputRef.current?.click()}
                              className="text-white hover:bg-white/20"
                            >
                              <Pencil className="h-4 w-4 shrink-0" />
                            </Button>
                          </div>
                        </div>
                        <input
                          ref={profilePictureInputRef}
                          type="file"
                          accept="image/*"
                          onChange={handleProfilePictureSelect}
                          className="hidden"
                        />
                      </div>
                      {profilePictureUrl && (
                        <div className="absolute -bottom-2 left-1/2 -translate-x-1/2 opacity-0 group-hover/profile:opacity-100 transition-opacity pointer-events-none z-30">
                          <div className="pointer-events-auto">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={handleRemoveProfilePicture}
                              className="text-destructive hover:text-destructive"
                            >
                              <Trash2 className="h-3 w-3 mr-1" />
                              Remove
          </Button>
                          </div>
                        </div>
                      )}
                    </div>

                    <div>
                      <h3 className="text-xl font-bold text-foreground">{displayNameToShow}</h3>
                      <p className="text-sm text-muted-foreground">@{usernameToShow}</p>
                      
                      <div className="mt-5 mb-6 pb-5 border-b border-border">
                        <div className="grid grid-cols-3 gap-4">
                          <div>
                            <button
                              onClick={() => setFriendsModalOpen(true)}
                              className="text-lg font-semibold text-foreground hover:text-primary transition-colors cursor-pointer block leading-tight"
                            >
                              {friendCount}
                            </button>
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
                          
                          {profile?.created_at ? (
                            <div>
                              <div className="text-lg font-semibold text-foreground leading-tight">
                                {format(new Date(profile.created_at), 'MMM d, yyyy')}
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

                      {profile && (profile as any).friend_code && (
                        <div className="mb-5 pb-5 border-b border-border">
                          <div className="text-xs text-muted-foreground mb-2 uppercase tracking-wider font-medium">Friend Code</div>
                          <div className="inline-flex items-center gap-2 bg-secondary/70 border border-border rounded-lg px-3 py-2">
                            <Hash className="h-4 w-4 text-muted-foreground" />
                            <span className="text-sm font-mono text-foreground">{(profile as any).friend_code}</span>
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-4 w-4 p-0 hover:bg-transparent text-muted-foreground hover:text-foreground"
                              onClick={(e) => {
                                e.stopPropagation();
                                navigator.clipboard.writeText((profile as any).friend_code);
                                toast.success('Friend code copied to clipboard!');
                              }}
                            >
                              <Copy className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      )}
                      
                      {aboutMe && (
                        <div className="mb-4 pb-5 border-b border-border">
                          <div className="text-xs text-muted-foreground mb-2 uppercase tracking-wider font-medium">About</div>
                          <div 
                            className="text-sm text-muted-foreground prose prose-invert max-w-none leading-relaxed"
                            onClick={handleAboutMeLinkClick}
                          >
                            {renderAboutMe(aboutMe)}
                          </div>
                        </div>
                      )}
                      
                      {visiblePlatforms.length > 0 && (
                        <div>
                          <p className="text-xs text-muted-foreground mb-3 uppercase tracking-wider font-medium">Connections</p>
                          <div className="space-y-2">
                            {visiblePlatforms.map((platform) => {
                              const IconComponent = platform.icon;
                              const url = getPlatformUrl(platform.id, platform.username);
                              const displayUsername = platform.username.replace(platform.prefix || '', '').replace('u/', '');
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
                                  <ExternalLink className="h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors opacity-0 group-hover:opacity-100" />
                                </a>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </SettingsSection>
            </div>
          </div>

          <div className="pt-6">
            <DangerZone />
          </div>
        </TabsContent>

        <TabsContent value="display" className="space-y-4">
          <SettingsSection title="Theme" description="Adjust the color of the interface for better visibility.">
            <div className="flex flex-wrap items-start gap-4">
              {themes.map((t) => (
                <div key={t.id} className="flex flex-col items-center gap-2 min-w-[3.5rem]">
                <button
                  onClick={() => setTheme(t.id)}
                  className={cn(
                      "relative w-14 h-14 rounded-lg transition-all duration-200 flex items-center justify-center flex-shrink-0",
                    t.id === 'sync' 
                      ? "bg-gradient-to-br from-white via-zinc-400 to-black border border-border"
                      : t.bgClass,
                    theme === t.id && "ring-2 ring-primary ring-offset-2 ring-offset-background"
                  )}
                  title={t.label}
                >
                  {t.id === 'sync' && (
                    <RefreshCw className="h-5 w-5 text-foreground" />
                  )}
                  {theme === t.id && t.id !== 'sync' && (
                    <Check className={cn(
                      "h-5 w-5",
                      t.id === 'light' ? "text-zinc-900" : "text-white"
                    )} />
                  )}
                  {theme === t.id && t.id === 'sync' && (
                    <div className="absolute -top-1 -right-1 w-4 h-4 bg-primary rounded-full flex items-center justify-center">
                      <Check className="h-3 w-3 text-white" />
                    </div>
                  )}
                </button>
                  <span className="text-xs text-muted-foreground text-center w-full break-words">{t.label}</span>
            </div>
              ))}
            </div>
          </SettingsSection>

          <SettingsSection title="UI Density" description="Adjust the space between elements in the interface.">
            <RadioGroup 
              value={density} 
              onValueChange={(value) => setDensity(value as UIDensity)}
              className="space-y-1"
            >
              {[
                { value: 'compact', label: 'Compact' },
                { value: 'default', label: 'Default' },
                { value: 'spacious', label: 'Spacious' },
              ].map((option) => (
                <div
                  key={option.value}
                  className={cn(
                    "flex items-center space-x-3 px-4 py-3 rounded-lg cursor-pointer transition-colors",
                    density === option.value 
                      ? "bg-secondary" 
                      : "hover:bg-secondary/50"
                  )}
                  onClick={() => setDensity(option.value as UIDensity)}
                >
                  <RadioGroupItem value={option.value} id={option.value} />
                  <Label htmlFor={option.value} className="cursor-pointer flex-1">
                    {option.label}
                  </Label>
                </div>
              ))}
            </RadioGroup>
          </SettingsSection>

          <SettingsSection title="Scaling">
            <div className="space-y-4">
              <div>
                <Label className="text-sm font-medium">Font size</Label>
                <p className="text-xs text-muted-foreground">Adjust the size of text throughout the app</p>
              </div>
              <RadioGroup 
                value={fontScale.toString()} 
                onValueChange={(value) => setFontScale(parseInt(value))}
                className="flex flex-wrap gap-4"
              >
                {fontScaleOptions.map((option) => (
                  <div
                    key={option.value}
                    className={cn(
                      "flex items-center space-x-2 px-4 py-2 rounded-lg cursor-pointer transition-colors border flex-shrink-0",
                      fontScale === option.value 
                        ? "bg-primary/15 border-primary" 
                        : "bg-secondary/50 border-border hover:bg-secondary"
                    )}
                    onClick={() => setFontScale(option.value)}
                  >
                    <RadioGroupItem value={option.value.toString()} id={`font-${option.value}`} />
                    <Label htmlFor={`font-${option.value}`} className="cursor-pointer whitespace-nowrap">
                      {option.label}
                    </Label>
                  </div>
                ))}
              </RadioGroup>
            </div>

            <div className="space-y-4 pt-4 border-t border-border">
              <div>
                <Label className="text-sm font-medium">Zoom level</Label>
                <p className="text-xs text-muted-foreground">Adjust the overall size of the interface</p>
              </div>
              <RadioGroup 
                value={zoomLevel.toString()} 
                onValueChange={(value) => setZoomLevel(parseInt(value))}
                className="flex flex-wrap gap-4"
              >
                {zoomOptions.map((option) => (
                  <div
                    key={option.value}
                    className={cn(
                      "flex items-center space-x-2 px-4 py-2 rounded-lg cursor-pointer transition-colors border flex-shrink-0",
                      zoomLevel === option.value 
                        ? "bg-primary/15 border-primary" 
                        : "bg-secondary/50 border-border hover:bg-secondary"
                    )}
                    onClick={() => setZoomLevel(option.value)}
                  >
                    <RadioGroupItem value={option.value.toString()} id={`zoom-${option.value}`} />
                    <Label htmlFor={`zoom-${option.value}`} className="cursor-pointer whitespace-nowrap">
                      {option.label}
                    </Label>
                  </div>
                ))}
              </RadioGroup>
            </div>
          </SettingsSection>

          <SettingsSection title="Date & Time">
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>Date Format</Label>
                <Select value={dateFormat} onValueChange={setDateFormat}>
                  <SelectTrigger className="w-[200px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="MM/DD/YYYY">MM/DD/YYYY</SelectItem>
                    <SelectItem value="DD/MM/YYYY">DD/MM/YYYY</SelectItem>
                    <SelectItem value="YYYY-MM-DD">YYYY-MM-DD</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Time Format</Label>
                <Select value={timeFormat} onValueChange={setTimeFormat}>
                  <SelectTrigger className="w-[200px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="12h">12-hour (3:45 PM)</SelectItem>
                    <SelectItem value="24h">24-hour (15:45)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </SettingsSection>

          <SettingsSection title="Messaging" description="Customize your messaging experience.">
            <div className="space-y-2">
              <Label>Default Emoji Skin Tone</Label>
              <Select
                value={skinTone}
                onValueChange={(value) => {
                  const newTone = value as SkinTone;
                  setSkinTone(newTone);
                  setSkinTonePreference(newTone);
                }}
              >
                <SelectTrigger className="w-[200px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="default">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍</span>
                      <span>Default</span>
                    </div>
                  </SelectItem>
                  <SelectItem value="light">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍🏻</span>
                      <span>Tone 1</span>
                    </div>
                  </SelectItem>
                  <SelectItem value="mediumLight">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍🏼</span>
                      <span>Tone 2</span>
                    </div>
                  </SelectItem>
                  <SelectItem value="medium">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍🏽</span>
                      <span>Tone 3</span>
                    </div>
                  </SelectItem>
                  <SelectItem value="mediumDark">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍🏾</span>
                      <span>Tone 4</span>
                    </div>
                  </SelectItem>
                  <SelectItem value="dark">
                    <div className="flex items-center gap-2">
                      <span className="text-xl">👍🏿</span>
                      <span>Tone 5</span>
                    </div>
                  </SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Choose your preferred default skin tone for emojis. You can also select skin tones individually when picking emojis.
              </p>
            </div>
          </SettingsSection>

          <Button variant="hero" onClick={handleSavePreferences} className="gap-2">
            <Save className="h-4 w-4" />
            Save Display Settings
          </Button>
        </TabsContent>

        <TabsContent value="notifications" className="space-y-4">
          <SettingsSection title="General">
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <Label>Do Not Disturb</Label>
                  <p className="text-sm text-muted-foreground">Temporarily disable all notifications while preserving your settings</p>
                </div>
                <Switch checked={doNotDisturb} onCheckedChange={(checked) => checked ? enable() : disable()} />
              </div>
              <div className="flex items-center justify-between">
                <div>
                  <Label>Enable Notifications</Label>
                  <p className="text-sm text-muted-foreground">Receive notifications about important events</p>
                </div>
                <Switch 
                  checked={notificationSettings.enabled} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ enabled })} 
                  disabled={doNotDisturb}
                />
              </div>
              <div className="flex items-center justify-between">
                <div>
                  <Label>Desktop Notifications</Label>
                  <p className="text-sm text-muted-foreground">Show browser notifications</p>
                </div>
                <Switch 
                  checked={getDisplayedState(notificationSettings.desktop)} 
                  onCheckedChange={handleDesktopNotificationToggle} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <div>
                  <Label>In-App Notifications</Label>
                  <p className="text-sm text-muted-foreground">Show notifications within the app</p>
                </div>
                <Switch
                  checked={getDisplayedState(notificationSettings.inApp)}
                  onCheckedChange={(enabled) => updateNotificationSettings({ inApp: enabled })}
                  disabled={!notificationSettings.enabled || doNotDisturb}
                />
              </div>
              <div className="flex items-center justify-between">
                <div>
                  <Label>Taskbar Flashing</Label>
                  <p className="text-sm text-muted-foreground">Flash taskbar icon when receiving notifications while app is not focused</p>
                </div>
                <Switch
                  checked={getDisplayedState(notificationSettings.taskbarFlashing)}
                  onCheckedChange={(enabled) => updateNotificationSettings({ taskbarFlashing: enabled })}
                  disabled={!notificationSettings.enabled || doNotDisturb}
                />
              </div>
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Notification Sound</Label>
                    <p className="text-sm text-muted-foreground">Play sound on new notifications</p>
                  </div>
                  <Switch 
                    checked={getDisplayedState(notificationSettings.sound)} 
                    onCheckedChange={(enabled) => updateNotificationSettings({ sound: enabled })}
                    disabled={!notificationSettings.enabled || doNotDisturb} 
                  />
                </div>
                {notificationSettings.sound && (
                  <div className="space-y-3">
                    <div className="flex items-center gap-2">
                      <Select
                        value={notificationSettings.soundType || 'chime'}
                        onValueChange={(value: NotificationSoundType) => {
                          updateNotificationSettings({ soundType: value });
                        }}
                        disabled={!notificationSettings.enabled || doNotDisturb}
                      >
                        <SelectTrigger className="flex-1">
                          <SelectValue placeholder="Select sound" />
                        </SelectTrigger>
                        <SelectContent className="max-h-[300px]">
                          {getAvailableSounds().map((sound) => (
                            <SelectItem key={sound.value} value={sound.value}>
                              {sound.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          const soundType = notificationSettings.soundType || 'chime';
                          previewSound(soundType);
                        }}
                        disabled={!notificationSettings.enabled || doNotDisturb}
                        title="Preview sound"
                      >
                        <Volume2 className="h-4 w-4" />
                      </Button>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <Label className="text-sm">Volume</Label>
                        <span className="text-sm text-muted-foreground">
                          {notificationSettings.soundVolume || 70}%
                        </span>
                      </div>
                      <Slider
                        value={[notificationSettings.soundVolume || 70]}
                        onValueChange={(value) => {
                          updateNotificationSettings({ soundVolume: value[0] });
                        }}
                        min={0}
                        max={100}
                        step={1}
                        disabled={!notificationSettings.enabled || doNotDisturb}
                        className="w-full"
                      />
                    </div>
                  </div>
                )}
              </div>
            </div>
          </SettingsSection>

          <SettingsSection title="Notification Types">
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <Label>New Predictions Available</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.newPredictions)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, newPredictions: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Game Results Updated</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.gameResults)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, gameResults: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Messages</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.messages)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, messages: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Pick Status (Won/Lost)</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.pickStatus)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, pickStatus: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>User Tailing Your Pick</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.pickTailed)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, pickTailed: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Invites (Friend Requests & Group Invites)</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.invites)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, invites: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Friend Request Accepted</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.friendRequestAccepted)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, friendRequestAccepted: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
              <div className="flex items-center justify-between">
                <Label>Group Updates (Member Additions/Removals & Changes)</Label>
                <Switch 
                  checked={getDisplayedState(notificationSettings.types.groupUpdates)} 
                  onCheckedChange={(enabled) => updateNotificationSettings({ types: { ...notificationSettings.types, groupUpdates: enabled } })} 
                  disabled={!notificationSettings.enabled || doNotDisturb} 
                />
              </div>
            </div>
          </SettingsSection>

          <div className="text-sm text-muted-foreground">
            Notification settings are saved automatically.
          </div>
        </TabsContent>

        <TabsContent value="data" className="space-y-4">
          <SettingsSection title="Default Preferences">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label>Default Time Window</Label>
                <Select value={defaultTimeWindow} onValueChange={setDefaultTimeWindow}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="L5">Last 5 Games</SelectItem>
                    <SelectItem value="L10">Last 10 Games</SelectItem>
                    <SelectItem value="L20">Last 20 Games</SelectItem>
                    <SelectItem value="L50">Last 50 Games</SelectItem>
                    <SelectItem value="All">All Games</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Default Statistic</Label>
                <Select value={defaultStat} onValueChange={setDefaultStat}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="points">Points</SelectItem>
                    <SelectItem value="rebounds">Rebounds</SelectItem>
                    <SelectItem value="assists">Assists</SelectItem>
                    <SelectItem value="steals">Steals</SelectItem>
                    <SelectItem value="blocks">Blocks</SelectItem>
                    <SelectItem value="turnovers">Turnovers</SelectItem>
                    <SelectItem value="threePointersMade">3-Pointers Made</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Default Confidence Filter</Label>
                <Select value={defaultConfidence} onValueChange={setDefaultConfidence}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Predictions</SelectItem>
                    <SelectItem value="high">High Only</SelectItem>
                    <SelectItem value="medium">Medium Only</SelectItem>
                    <SelectItem value="low">Low Only</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Auto-Refresh Interval</Label>
                <Select value={autoRefresh} onValueChange={setAutoRefresh}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="never">Never</SelectItem>
                    <SelectItem value="5">5 minutes</SelectItem>
                    <SelectItem value="15">15 minutes</SelectItem>
                    <SelectItem value="30">30 minutes</SelectItem>
                    <SelectItem value="60">1 hour</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </SettingsSection>

          <SettingsSection
            title="Cache Settings"
            description="Manage offline data storage to reduce API calls and improve performance."
          >
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>Prediction Cache Retention</Label>
                <Select
                  value={String(retentionDays)}
                  onValueChange={handleRetentionChange}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="7">7 days (~10-20 MB)</SelectItem>
                    <SelectItem value="14">14 days (~20-40 MB)</SelectItem>
                    <SelectItem value="30">30 days (~50-100 MB) - Recommended</SelectItem>
                    <SelectItem value="60">60 days (~100-200 MB)</SelectItem>
                    <SelectItem value="90">90 days (~150-300 MB)</SelectItem>
                    <SelectItem value="all">All time (unlimited)</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  Predictions are cached locally. Recent games are updated automatically in the background when viewed.
                </p>
              </div>

              <div className="space-y-2">
                <Label>Model Performance Cache</Label>
                <Select
                  value={modelPerfRetentionDays === 'off' ? 'off' : 'on'}
                  onValueChange={(value) => {
                    const days = value === 'off' ? 'off' : 30;
                    setModelPerfRetentionDays(days);
                  }}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="on">On - Cache all queries</SelectItem>
                    <SelectItem value="off">Off - No caching</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  When enabled, model performance queries are cached for faster loading. Cached data updates automatically when viewed.
                </p>
              </div>

              <div className="rounded-lg border border-border bg-muted/50 p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">Storage Used</span>
                  <span className="text-sm text-muted-foreground">{storageUsage.formattedSize}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">Cached Predictions</span>
                  <span className="text-sm text-muted-foreground">{cacheCounts.predictions} items</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">Cached Model Performance</span>
                  <span className="text-sm text-muted-foreground">{cacheCounts.modelPerformance} items</span>
                </div>
              </div>

              <div className="flex gap-2">
                <Button
                  type="button"
                  variant="outline"
                  onClick={async () => {
                    try {
                      const [predEntries, mpEntries] = await Promise.all([
                        getAllCacheEntries(),
                        getAllModelPerformanceEntries()
                      ]);
                      setCacheEntries(predEntries);
                      setModelPerfEntries(mpEntries);
                      setCacheModalOpen(true);
                    } catch (error) {
                      toast.error('Failed to load cache data');
                    }
                  }}
                  className="gap-2"
                >
                  <DatabaseIcon className="h-4 w-4" />
                  Manage Cache
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => setClearCacheConfirmOpen(true)}
                  className="gap-2"
                >
                  <Trash2 className="h-4 w-4" />
                  Clear All Cache
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={async () => {
                    try {
                      await refreshStats();
                      toast.success('Cache stats refreshed');
                    } catch (error) {
                      toast.error('Failed to refresh stats');
                    }
                  }}
                  className="gap-2"
                >
                  <RefreshCw className="h-4 w-4" />
                  Refresh Stats
                </Button>
              </div>

              <div className="rounded-lg border border-blue-500/50 bg-blue-500/10 p-3 text-sm text-blue-600 dark:text-blue-400 flex items-start gap-2">
                <Info className="h-4 w-4 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="font-medium mb-1">How caching works:</p>
                  <ul className="space-y-1 text-xs">
                    <li>• All predictions are cached when viewed</li>
                    <li>• Recent games update automatically in the background</li>
                    <li>• Works offline using cached data</li>
                  </ul>
                </div>
              </div>
            </div>
          </SettingsSection>

          <AlertDialog open={retentionConfirmOpen} onOpenChange={setRetentionConfirmOpen}>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete Cached Data?</AlertDialogTitle>
                <AlertDialogDescription>
                  Changing retention to {pendingRetentionDays === 'all' ? 'all time' : `${pendingRetentionDays} days`} will
                  delete cached predictions older than this period. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel onClick={cancelRetentionChange}>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={confirmRetentionChange}>Delete and Apply</AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>

          <AlertDialog open={clearCacheConfirmOpen} onOpenChange={setClearCacheConfirmOpen}>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Clear All Cache?</AlertDialogTitle>
                <AlertDialogDescription>
                  This will delete all cached predictions and model performance data. You'll need to re-download data when viewing predictions again. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction
                  onClick={async () => {
                    setClearCacheConfirmOpen(false);
                    const result = await clearCache();

                    if (result.wasEmpty) {
                      toast.info('Cache is already empty');
                    } else {
                      toast.success(`Cache cleared: ${result.deletedCount} ${result.deletedCount === 1 ? 'item' : 'items'} deleted`);
                    }
                  }}
                >
                  Clear All Cache
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>

          <SettingsSection
            title="Error Logging"
            description="Configure error and debug logging to help diagnose issues."
          >
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <Label>Enable Error Logging</Label>
                  <p className="text-sm text-muted-foreground">
                    {window.electron 
                      ? 'Log errors, warnings, and debug information to local files'
                      : 'Log errors to console (file logging requires Electron app)'}
                  </p>
                </div>
                <Switch
                  checked={errorLoggingEnabled}
                  onCheckedChange={setErrorLoggingEnabled}
                />
              </div>

              {errorLoggingEnabled && (
                <>
                  {window.electron ? (
                    <>
                      <div className="space-y-2">
                        <Label>Log Folder</Label>
                        <div className="flex gap-2">
                          <Input
                            value={errorLogFolder}
                            placeholder="Select a folder for error logs..."
                            readOnly
                            className="flex-1"
                          />
                          <Button
                            type="button"
                            variant="outline"
                            onClick={handleSelectLogFolder}
                          >
                            <FolderOpen className="h-4 w-4 mr-2" />
                            Select Folder
                          </Button>
                        </div>
                        {errorLogFolder ? (
                          <p className="text-xs text-muted-foreground">
                            Logs will be saved to: {errorLogFolder}
                          </p>
                        ) : (
                          <div className="rounded-lg border border-red-500/50 bg-red-500/10 p-3 text-sm text-red-600 dark:text-red-400">
                            <AlertTriangle className="h-4 w-4 inline mr-2" />
                            You must select a file destination for error logs to be saved.
                          </div>
                        )}
                      </div>

                      <div className="space-y-2">
                        <Label>Log Level</Label>
                        <Select value={errorLogLevel} onValueChange={(value: 'debug' | 'info' | 'warn' | 'error') => setErrorLogLevel(value)}>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="error">Errors Only</SelectItem>
                            <SelectItem value="warn">Warnings & Errors</SelectItem>
                            <SelectItem value="info">Info, Warnings & Errors</SelectItem>
                            <SelectItem value="debug">All (Debug Mode)</SelectItem>
                          </SelectContent>
                        </Select>
                        <p className="text-xs text-muted-foreground">
                          {errorLogLevel === 'error' && 'Only log errors'}
                          {errorLogLevel === 'warn' && 'Log warnings and errors'}
                          {errorLogLevel === 'info' && 'Log info, warnings, and errors'}
                          {errorLogLevel === 'debug' && 'Log everything (verbose)'}
                        </p>
                      </div>
                    </>
                  ) : (
                    <div className="rounded-lg border border-blue-500/50 bg-blue-500/10 p-3 text-sm text-blue-600 dark:text-blue-400">
                      Error logging is enabled. Logs will appear in the browser console. 
                      For file logging, use the Electron desktop app.
                    </div>
                  )}
                </>
              )}
            </div>
          </SettingsSection>

          <SettingsSection title="Export Your Data" description="Download all your data in JSON format for backup or portability.">
            <ExportDataSection />
          </SettingsSection>

          <Button variant="hero" onClick={handleSavePreferences} className="gap-2">
            <Save className="h-4 w-4" />
            Save Data Preferences
          </Button>
        </TabsContent>

        {window.electron && (
          <TabsContent value="application" className="space-y-4">
            {isLoadingAppSettings ? (
              <div className="flex items-center justify-center py-12">
                <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
              </div>
            ) : appSettings ? (
              <>
                <SettingsSection title="Performance" description="Configure application performance settings.">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Hardware Acceleration</Label>
                        <p className="text-sm text-muted-foreground">
                          Use GPU for rendering. Improves performance but may cause issues on some systems. Requires restart to take effect.
                        </p>
                      </div>
                      <Switch 
                        checked={appSettings.hardwareAcceleration}
                        onCheckedChange={(enabled) => handleAppSettingChange('hardwareAcceleration', enabled)}
                      />
                    </div>
                  </div>
                </SettingsSection>

                <SettingsSection title="Window Behavior" description="Control how the application window behaves.">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Minimize to System Tray</Label>
                        <p className="text-sm text-muted-foreground">
                          When enabled, closing or minimizing the window will send it to the system tray instead of closing completely.
                        </p>
                      </div>
                      <Switch 
                        checked={appSettings.minimizeToTray}
                        onCheckedChange={(enabled) => handleAppSettingChange('minimizeToTray', enabled)}
                      />
                    </div>
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Always on Top</Label>
                        <p className="text-sm text-muted-foreground">
                          Keep the application window above all other windows.
                        </p>
                      </div>
                      <Switch 
                        checked={appSettings.alwaysOnTop}
                        onCheckedChange={(enabled) => handleAppSettingChange('alwaysOnTop', enabled)}
                      />
                    </div>
                  </div>
                </SettingsSection>

                <SettingsSection title="Startup" description="Configure application startup behavior.">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Start with System</Label>
                        <p className="text-sm text-muted-foreground">
                          Automatically launch the application when your computer starts.
                        </p>
                      </div>
                      <Switch 
                        checked={appSettings.startWithSystem}
                        onCheckedChange={(enabled) => handleAppSettingChange('startWithSystem', enabled)}
                      />
                    </div>
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Start Minimized</Label>
                        <p className="text-sm text-muted-foreground">
                          Start the application in the system tray (requires "Minimize to System Tray" to be enabled).
                        </p>
                      </div>
                      <Switch 
                        checked={appSettings.startMinimized}
                        onCheckedChange={(enabled) => handleAppSettingChange('startMinimized', enabled)}
                        disabled={!appSettings.minimizeToTray}
                      />
                    </div>
                  </div>
                </SettingsSection>

                <SettingsSection title="Integrations" description="Connect with external services and platforms.">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Discord Rich Presence</Label>
                        <p className="text-sm text-muted-foreground">
                          Show your activity in CourtVision on your Discord profile. This feature will be implemented in a future update.
                        </p>
                      </div>
                      <Switch 
                        checked={discordRichPresenceEnabled}
                        onCheckedChange={async (enabled) => {
                          setDiscordRichPresenceEnabled(enabled);
                          try {
                            await updateProfile.mutateAsync({
                              discord_rich_presence_enabled: enabled,
                            } as any);
                            toast.success('Discord Rich Presence setting updated');
                          } catch (err) {
                            logger.error('Error updating Discord Rich Presence setting', err as Error);
                            toast.error('Failed to update setting');
                            
                            setDiscordRichPresenceEnabled(!enabled);
                          }
                        }}
                        disabled={updateProfile.isPending}
                      />
                    </div>
                  </div>
                </SettingsSection>
              </>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <p>Unable to load application settings.</p>
              </div>
            )}
          </TabsContent>
        )}
      </Tabs>

      <AlertDialog open={!!linkWarningUrl} onOpenChange={(open) => !open && setLinkWarningUrl(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-warning" />
              External Link Warning
            </AlertDialogTitle>
            <AlertDialogDescription>
              <p className="mb-2">Are you sure you want to visit this link?</p>
              <p className="font-mono text-xs break-all bg-muted p-2 rounded mb-2">{linkWarningUrl}</p>
              <p className="text-xs text-muted-foreground">
                <strong>Warning:</strong> We do not filter or moderate links in user profiles. 
                Please be cautious when clicking on external links, especially if they are embedded or shortened.
              </p>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (linkWarningUrl) {
                  window.open(linkWarningUrl, '_blank', 'noopener,noreferrer');
                }
                setLinkWarningUrl(null);
              }}
            >
              <ExternalLink className="h-4 w-4 mr-2" />
              Continue to Link
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <ImageCropper
        open={cropperOpen}
        onOpenChange={(open) => {
          setCropperOpen(open);
          if (!open && cropperImageSrc) {
            
            URL.revokeObjectURL(cropperImageSrc);
            setCropperImageSrc('');
          }
        }}
        imageSrc={cropperImageSrc}
        onCropComplete={cropperType === 'profile' ? handleProfilePictureCropComplete : handleBannerCropComplete}
        
        aspectRatio={cropperType === 'profile' ? 1 : 3}
        cropShape={cropperType === 'profile' ? 'round' : 'rect'}
        title={cropperType === 'profile' ? 'Crop Profile Picture' : 'Crop Banner'}
      />

      {profile?.user_id && (
        <FriendsListModal
          userId={profile.user_id}
          open={friendsModalOpen}
          onOpenChange={setFriendsModalOpen}
        />
      )}

      <CacheManagementModal
        open={cacheModalOpen}
        onOpenChange={setCacheModalOpen}
        entries={cacheEntries}
        modelPerformanceEntries={modelPerfEntries}
        onDelete={deleteCacheEntries}
        onDeleteModelPerformance={deleteModelPerformanceEntries}
        onRefresh={async () => {
          const [predEntries, mpEntries] = await Promise.all([
            getAllCacheEntries(),
            getAllModelPerformanceEntries()
          ]);
          setCacheEntries(predEntries);
          setModelPerfEntries(mpEntries);
        }}
      />
    </div>
  );
}
