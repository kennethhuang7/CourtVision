import { useState, useEffect, useRef } from 'react';
import { MessageSquare, Send, Trash2, Archive, ArchiveRestore, Plus, Smile, SmilePlus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials, cn } from '@/lib/utils';
import { Textarea } from '@/components/ui/textarea';
import { ScrollArea } from '@/components/ui/scroll-area';
import { PickShareMessageCard } from '@/components/messages/PickShareMessageCard';
import { EmojiPicker } from '@/components/chat/EmojiPicker';
import { ReactionBar } from '@/components/chat/ReactionBar';
import { useConversations, type Conversation } from '@/hooks/useConversations';
import { useMessages } from '@/hooks/useMessages';
import { useSendMessage } from '@/hooks/useSendMessage';
import { useDeleteMessage } from '@/hooks/useDeleteMessage';
import { useCreateDM } from '@/hooks/useCreateDM';
import { useMarkConversationRead } from '@/hooks/useMarkConversationRead';
import { useArchiveConversation } from '@/hooks/useArchiveConversation';
import { useEnsureGroupConversation } from '@/hooks/useEnsureGroupConversation';
import { useFriends } from '@/hooks/useFriends';
import { useGroups } from '@/hooks/useGroups';
import { useAuth } from '@/contexts/AuthContext';
import { useTheme } from '@/contexts/ThemeContext';
import { useUserProfile } from '@/hooks/useUserProfile';
import { useBatchMessageReactions } from '@/hooks/useMessageReactions';
import { useAddReaction } from '@/hooks/useAddReaction';
import { useReactionSubscription } from '@/hooks/useReactionSubscription';
import { format, formatDistanceToNow, isToday, isYesterday, isSameDay } from 'date-fns';
import { formatUserTime } from '@/lib/dateUtils';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { logger } from '@/lib/logger';
import { shouldDisplayAsLargeEmoji } from '@/lib/emojiUtils';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuTrigger,
  ContextMenuSeparator,
} from '@/components/ui/context-menu';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';


function getDateDividerLabel(date: Date): string {
  if (isToday(date)) {
    return 'Today';
  } else if (isYesterday(date)) {
    return 'Yesterday';
  } else {
    return format(date, 'MMMM d, yyyy');
  }
}

export function ChatWindow() {
  const { user } = useAuth();
  const { timeFormat } = useTheme();
  const { data: currentUserProfile } = useUserProfile();
  const [selectedConversation, setSelectedConversation] = useState<{
    type: 'dm' | 'group';
    id: string;
  } | null>(null);
  const [createDMOpen, setCreateDMOpen] = useState(false);
  const [messageContent, setMessageContent] = useState('');
  const [emojiPickerOpen, setEmojiPickerOpen] = useState(false);
  const [reactionPickerMessageId, setReactionPickerMessageId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const { data: conversations = [], isLoading: conversationsLoading } = useConversations();
  const { data: messages = [], isLoading: messagesLoading, hasMore, loadMore } = useMessages(
    selectedConversation?.type || 'dm',
    selectedConversation?.id || '',
    !!selectedConversation
  );
  const sendMessageMutation = useSendMessage();
  const deleteMessageMutation = useDeleteMessage();
  const createDMMutation = useCreateDM();
  const markReadMutation = useMarkConversationRead();
  const archiveMutation = useArchiveConversation();
  const ensureGroupConvMutation = useEnsureGroupConversation();
  const addReaction = useAddReaction();
  const { data: friends = [] } = useFriends();
  const { data: groups = [] } = useGroups();

  
  const messageIds = messages.map(m => m.id);
  const { data: reactionsMap = new Map() } = useBatchMessageReactions(messageIds, !!selectedConversation);

  
  useReactionSubscription(messageIds, !!selectedConversation);

  
  useEffect(() => {
    if (selectedConversation) {
      markReadMutation.mutate({
        conversationType: selectedConversation.type,
        conversationId: selectedConversation.id,
      });
      
      const conv = conversations.find(
        c => c.conversation_type === selectedConversation.type &&
        c.conversation_id === selectedConversation.id
      );
      if (conv?.is_archived) {
        archiveMutation.mutate({
          conversationType: selectedConversation.type,
          conversationId: selectedConversation.id,
          archive: false,
        });
      }
    }
  }, [selectedConversation, conversations]);

  
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  
  useEffect(() => {
    if (!messagesContainerRef.current || !selectedConversation) return;

    const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
    if (!viewport) return;

    const handleScroll = () => {
      if (viewport.scrollTop < 100 && hasMore && !messagesLoading) {
        const previousScrollHeight = viewport.scrollHeight;
        loadMore();
        setTimeout(() => {
          const newScrollHeight = viewport.scrollHeight;
          viewport.scrollTop = newScrollHeight - previousScrollHeight;
        }, 100);
      }
    };

    viewport.addEventListener('scroll', handleScroll);
    return () => viewport.removeEventListener('scroll', handleScroll);
  }, [hasMore, messagesLoading, selectedConversation, loadMore]);

  const handleSendMessage = async () => {
    if (!selectedConversation || !messageContent.trim()) return;

    try {
      await sendMessageMutation.mutateAsync({
        conversationType: selectedConversation.type,
        conversationId: selectedConversation.id,
        content: messageContent,
      });
      setMessageContent('');
    } catch (error) {
      
    }
  };

  const handleCreateDM = async (userId: string) => {
    try {
      const conversationId = await createDMMutation.mutateAsync(userId);
      setSelectedConversation({ type: 'dm', id: conversationId });
      setCreateDMOpen(false);
    } catch (error) {
      
    }
  };

  const handleOpenGroupChat = async (groupId: string) => {
    try {
      const conversationId = await ensureGroupConvMutation.mutateAsync(groupId);
      setSelectedConversation({ type: 'group', id: conversationId });
      setCreateDMOpen(false);
    } catch (error: any) {
      const existingConv = conversations.find(
        c => c.conversation_type === 'group' && c.group_id === groupId
      );
      if (existingConv) {
        setSelectedConversation({ type: 'group', id: existingConv.conversation_id });
      } else {
        setSelectedConversation({ type: 'group', id: groupId });
      }
      setCreateDMOpen(false);
    }
  };

  const handleDeleteMessage = async (messageId: string) => {
    try {
      await deleteMessageMutation.mutateAsync(messageId);
    } catch (error) {
      
    }
  };

  const handleEmojiSelect = (emoji: string) => {
    if (!textareaRef.current) return;

    
    const start = textareaRef.current.selectionStart;
    const end = textareaRef.current.selectionEnd;
    const newContent = messageContent.substring(0, start) + emoji + messageContent.substring(end);

    setMessageContent(newContent);

    
    setTimeout(() => {
      if (textareaRef.current) {
        const newPosition = start + emoji.length;
        textareaRef.current.selectionStart = newPosition;
        textareaRef.current.selectionEnd = newPosition;
        textareaRef.current.focus();
      }
    }, 0);
  };

  const getConversationDisplay = (conv: Conversation) => {
    if (conv.conversation_type === 'dm') {
      const profile = conv.other_user_profile;
      return {
        name: profile?.display_name || profile?.username || 'Unknown User',
        avatar: profile?.profile_picture_url,
        subtitle: `@${profile?.username || 'unknown'}`,
      };
    } else {
      const group = conv.group_info;
      return {
        name: group?.name || 'Unknown Group',
        avatar: group?.profile_picture_url || null,
        subtitle: 'Group chat',
      };
    }
  };

  const selectedConv = conversations.find(
    c => c.conversation_type === selectedConversation?.type && 
    c.conversation_id === selectedConversation?.id
  );

  return (
    <div className="flex h-full flex-col bg-background">
      <div className="flex flex-1 min-h-0">
        <div className="w-80 border-r border-border flex flex-col bg-card/50">
          <div className="h-14 px-4 border-b border-border flex items-center justify-between shrink-0 bg-card">
            <h2 className="text-base font-semibold text-foreground">Messages</h2>
            <Button
              size="sm"
              variant="ghost"
              className="h-8 w-8 p-0 hover:bg-secondary"
              onClick={() => setCreateDMOpen(true)}
              title="New conversation"
            >
              <Plus className="h-4 w-4" />
            </Button>
          </div>

          <ScrollArea className="flex-1">
            {conversationsLoading ? (
              <div className="p-4 text-center text-sm text-muted-foreground">
                Loading...
              </div>
            ) : conversations.length === 0 ? (
              <div className="p-4 text-center text-sm text-muted-foreground">
                <MessageSquare className="h-8 w-8 mx-auto mb-2 opacity-50" />
                <p>No conversations</p>
              </div>
            ) : (
              <div>
                {conversations.map((conv) => {
                  const display = getConversationDisplay(conv);
                  const isSelected = 
                    selectedConversation?.type === conv.conversation_type &&
                    selectedConversation?.id === conv.conversation_id;

                  return (
                    <button
                      key={conv.id}
                      onClick={() => setSelectedConversation({
                        type: conv.conversation_type,
                        id: conv.conversation_id,
                      })}
                      className={cn(
                        'w-full px-4 py-3 text-left transition-colors',
                        'hover:bg-secondary/50 active:bg-secondary/70',
                        isSelected && 'bg-secondary border-l-2 border-l-primary'
                      )}
                    >
                      <div className="flex items-start gap-3">
                        <Avatar className="h-12 w-12 flex-shrink-0">
                          {display.avatar ? (
                            <AvatarImage src={display.avatar} alt={display.name} />
                          ) : null}
                          <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
                            {getInitials(display.name)}
                          </AvatarFallback>
                        </Avatar>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2 mb-1">
                            <p className="text-sm font-medium text-foreground truncate">
                              {display.name}
                            </p>
                            {conv.last_message_at && (
                              <span className="text-xs text-muted-foreground whitespace-nowrap flex-shrink-0">
                                {formatDistanceToNow(new Date(conv.last_message_at), { addSuffix: true })}
                              </span>
                            )}
                          </div>
                          {conv.last_message_preview && (
                            <p className="text-xs text-muted-foreground truncate mb-1">
                              {conv.last_message_preview}
                            </p>
                          )}
                          {conv.unread_count > 0 && (
                            <div className="flex items-center justify-end mt-1">
                              <span className="bg-primary text-primary-foreground text-xs font-medium px-2 py-0.5 rounded-full min-w-[20px] text-center">
                                {conv.unread_count}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </ScrollArea>
        </div>

        <div className="flex-1 flex flex-col min-w-0">
          {selectedConversation ? (
            <>
              <div className="h-14 px-4 border-b border-border flex items-center justify-between shrink-0 bg-card">
                <div className="flex items-center gap-3 min-w-0 flex-1">
                  {selectedConv && (() => {
                    const display = getConversationDisplay(selectedConv);
                    return (
                      <>
                        <Avatar className="h-9 w-9 flex-shrink-0">
                          {display.avatar ? (
                            <AvatarImage src={display.avatar} alt={display.name} />
                          ) : null}
                          <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-xs font-semibold text-white">
                            {getInitials(display.name)}
                          </AvatarFallback>
                        </Avatar>
                        <div className="min-w-0 flex-1">
                          <p className="text-sm font-semibold text-foreground truncate">
                            {display.name}
                          </p>
                          <p className="text-xs text-muted-foreground truncate">
                            {display.subtitle}
                          </p>
                        </div>
                      </>
                    );
                  })()}
                </div>
                {selectedConv && selectedConv.conversation_type === 'dm' && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 w-8 p-0 hover:bg-secondary"
                    onClick={() => {
                      archiveMutation.mutate({
                        conversationType: selectedConv.conversation_type,
                        conversationId: selectedConv.conversation_id,
                        archive: !selectedConv.is_archived,
                      });
                    }}
                    title={selectedConv.is_archived ? 'Unarchive' : 'Archive'}
                  >
                    {selectedConv.is_archived ? (
                      <ArchiveRestore className="h-4 w-4" />
                    ) : (
                      <Archive className="h-4 w-4" />
                    )}
                  </Button>
                )}
              </div>

              <ScrollArea className="flex-1" ref={messagesContainerRef}>
                <div className="p-4">
                  {messagesLoading && messages.length === 0 ? (
                    <div className="text-center text-sm text-muted-foreground py-8">
                      Loading messages...
                    </div>
                  ) : messages.length === 0 ? (
                    <div className="text-center text-sm text-muted-foreground py-8">
                      <MessageSquare className="h-10 w-10 mx-auto mb-2 opacity-50" />
                      <p>No messages yet</p>
                    </div>
                  ) : (
                    <div>
                      {messages.map((message, index) => {
                        const isOwn = message.sender_id === user?.id;
                        const isDeleted = message.is_deleted;

                        
                        const currentMessageDate = new Date(message.created_at);
                        const prevMessage = index > 0 ? messages[index - 1] : null;
                        const prevMessageDate = prevMessage ? new Date(prevMessage.created_at) : null;
                        const showDateDivider = !prevMessageDate || !isSameDay(currentMessageDate, prevMessageDate);

                        
                        
                        const nextMessage = index < messages.length - 1 ? messages[index + 1] : null;
                        const showHeader =
                          index === 0 ||
                          !prevMessage ||
                          prevMessage.sender_id !== message.sender_id ||
                          new Date(message.created_at).getTime() - new Date(prevMessage.created_at).getTime() > 300000;
                        
                        
                        const nextMessageHasHeader = nextMessage && (
                          nextMessage.sender_id !== message.sender_id ||
                          new Date(nextMessage.created_at).getTime() - new Date(message.created_at).getTime() > 300000
                        );

                        
                        const senderProfile = isOwn 
                          ? currentUserProfile 
                          : message.sender_profile;
                        const senderName = isOwn
                          ? (currentUserProfile?.display_name || currentUserProfile?.username || user?.username || 'You')
                          : (message.sender_profile?.display_name || message.sender_profile?.username || 'Unknown');
                        const senderAvatar = isOwn
                          ? currentUserProfile?.profile_picture_url
                          : message.sender_profile?.profile_picture_url;

                        return (
                          <div key={message.id}>
                            {showDateDivider && (
                              <div className="flex items-center gap-3 my-6">
                                <div className="flex-1 h-px bg-border"></div>
                                <span className="text-xs font-medium text-muted-foreground px-3">
                                  {getDateDividerLabel(currentMessageDate)}
                                </span>
                                <div className="flex-1 h-px bg-border"></div>
                              </div>
                            )}

                            <ContextMenu>
                              <ContextMenuTrigger asChild>
                              <div
                                className={cn(
                                  index < messages.length - 1 ? (
                                    reactionsMap.get(message.id) && reactionsMap.get(message.id)!.length > 0
                                      ? 'mb-4'  
                                      : 'mb-3'
                                  ) : 'mb-2'
                                )}
                              >
                                <div className={cn(
                                  'flex gap-3 items-end',
                                  isOwn ? 'flex-row-reverse' : 'flex-row'
                                )}>
                                  <div className="flex flex-col flex-shrink-0">
                                    {showHeader && <div className="h-5 mb-1" />}
                                    <Avatar className="h-9 w-9 flex-shrink-0">
                                      {senderAvatar ? (
                                        <AvatarImage
                                          src={senderAvatar}
                                          alt={senderName}
                                        />
                                      ) : null}
                                      <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-xs font-semibold text-white">
                                        {getInitials(senderName)}
                                      </AvatarFallback>
                                    </Avatar>
                                  </div>

                                  <div className={cn(
                                    'flex flex-col max-w-[70%] min-w-0 flex-1',
                                    isOwn ? 'items-end' : 'items-start'
                                  )}>
                                    {showHeader && (
                                      <div className={cn(
                                        'flex items-center gap-2 mb-1 px-1',
                                        isOwn ? 'flex-row-reverse' : 'flex-row'
                                      )}>
                                        <p className="text-xs font-medium text-foreground">
                                          {senderName}
                                        </p>
                                        <span className="text-xs text-muted-foreground">
                                          {formatUserTime(new Date(message.created_at), timeFormat)}
                                        </span>
                                      </div>
                                    )}

                                    {message.message_type === 'pick_share' && message.metadata ? (
                                      <PickShareMessageCard
                                        metadata={message.metadata}
                                        isOwn={isOwn}
                                      />
                                    ) : shouldDisplayAsLargeEmoji(message.content) && !isDeleted ? (
                                      
                                      <div className="text-6xl leading-none">
                                        {message.content}
                                      </div>
                                    ) : (
                                      <div
                                        className={cn(
                                          'rounded-xl px-3 py-2 text-sm break-words',
                                          isDeleted
                                            ? 'bg-muted/50 text-muted-foreground italic'
                                            : isOwn
                                            ? 'bg-primary text-primary-foreground'
                                            : 'bg-secondary text-secondary-foreground'
                                        )}
                                      >
                                        {isDeleted ? (
                                          <p className="text-xs">Message deleted</p>
                                        ) : (
                                          <p className="whitespace-pre-wrap">{message.content}</p>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                </div>

                                {!isDeleted && reactionsMap.get(message.id) && reactionsMap.get(message.id)!.length > 0 && (
                                  <div className={cn(
                                    'flex gap-3',
                                    isOwn ? 'flex-row-reverse' : 'flex-row'
                                  )}>
                                    <div className="w-9 flex-shrink-0" />
                                    <div className={cn(
                                      'max-w-[70%] min-w-0 flex-1',
                                      isOwn ? 'flex justify-end' : 'flex justify-start'
                                    )}>
                                      <ReactionBar
                                        messageId={message.id}
                                        reactions={reactionsMap.get(message.id)!}
                                      />
                                    </div>
                                  </div>
                                )}
                              </div>
                            </ContextMenuTrigger>
                            <ContextMenuContent className="z-[200]">
                              {!isDeleted && (
                                <>
                                  <ContextMenuItem
                                    onClick={() => setReactionPickerMessageId(message.id)}
                                    className="cursor-pointer"
                                  >
                                    <SmilePlus className="h-4 w-4 mr-2" />
                                    Add Reaction
                                  </ContextMenuItem>
                                  {isOwn && <ContextMenuSeparator />}
                                </>
                              )}
                              {isOwn && !isDeleted && (
                                <ContextMenuItem
                                  onClick={() => handleDeleteMessage(message.id)}
                                  className="text-destructive focus:text-destructive"
                                >
                                  <Trash2 className="h-4 w-4 mr-2" />
                                  Delete Message
                                </ContextMenuItem>
                              )}
                            </ContextMenuContent>
                            </ContextMenu>
                          </div>
                        );
                      })}
                      <div ref={messagesEndRef} />
                    </div>
                  )}
                </div>
              </ScrollArea>

              <div className="h-auto px-4 py-3 border-t border-border shrink-0 bg-card">
                <div className="flex items-end gap-2">
                  <div className="relative flex-1">
                    <Textarea
                      ref={textareaRef}
                      value={messageContent}
                      onChange={(e) => setMessageContent(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' && !e.shiftKey) {
                          e.preventDefault();
                          handleSendMessage();
                        }
                      }}
                      placeholder="Type a message..."
                      className="min-h-[44px] max-h-[120px] resize-none text-sm bg-background pr-10"
                      maxLength={2000}
                      rows={1}
                    />
                    <Popover open={emojiPickerOpen} onOpenChange={setEmojiPickerOpen}>
                      <PopoverTrigger asChild>
                        <button
                          className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded hover:bg-accent transition-colors opacity-60 hover:opacity-100"
                          type="button"
                        >
                          <Smile className="h-5 w-5 text-muted-foreground" />
                        </button>
                      </PopoverTrigger>
                      <PopoverContent
                        className="w-auto p-0 border-0 shadow-none bg-transparent z-[110]"
                        side="top"
                        align="end"
                        sideOffset={8}
                      >
                        <EmojiPicker
                          onEmojiSelect={handleEmojiSelect}
                          onClose={() => setEmojiPickerOpen(false)}
                          mode="insert"
                        />
                      </PopoverContent>
                    </Popover>
                  </div>
                  <Button
                    onClick={handleSendMessage}
                    disabled={!messageContent.trim() || sendMessageMutation.isPending}
                    size="default"
                    className="h-[44px] px-4 shrink-0"
                  >
                    <Send className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
              <div className="text-center">
                <MessageSquare className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>Select a conversation to start messaging</p>
              </div>
            </div>
          )}
        </div>
      </div>

      <Dialog open={createDMOpen} onOpenChange={setCreateDMOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Start Conversation</DialogTitle>
            <DialogDescription>
              Select a friend or group to start chatting
            </DialogDescription>
          </DialogHeader>
          <ScrollArea className="max-h-[400px]">
            <div className="space-y-4">
              {friends.length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-muted-foreground mb-2 px-2">
                    Friends ({friends.length})
                  </h3>
                  <div className="space-y-1">
                    {friends.map((friendship) => {
                      const friend = (friendship as any).friend_profile;
                      if (!friend) return null;
                      
                      const displayName = friend.display_name || friend.username;
                      return (
                        <button
                          key={friend.user_id}
                          onClick={() => handleCreateDM(friend.user_id)}
                          className="w-full flex items-center gap-3 p-3 rounded-lg hover:bg-secondary transition-colors text-left"
                          disabled={createDMMutation.isPending}
                        >
                          <Avatar className="h-10 w-10">
                            {friend.profile_picture_url ? (
                              <AvatarImage src={friend.profile_picture_url} alt={displayName} />
                            ) : null}
                            <AvatarFallback>
                              {getInitials(displayName)}
                            </AvatarFallback>
                          </Avatar>
                          <div className="flex-1 min-w-0">
                            <p className="font-medium text-sm truncate">{displayName}</p>
                            <p className="text-xs text-muted-foreground truncate">@{friend.username}</p>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {groups.length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-muted-foreground mb-2 px-2">
                    Groups ({groups.length})
                  </h3>
                  <div className="space-y-1">
                    {groups.map((group) => (
                      <button
                        key={group.id}
                        onClick={() => handleOpenGroupChat(group.id)}
                        className="w-full flex items-center gap-3 p-3 rounded-lg hover:bg-secondary transition-colors text-left"
                        disabled={ensureGroupConvMutation.isPending}
                      >
                        <Avatar className="h-10 w-10">
                          {group.profile_picture_url ? (
                            <AvatarImage src={group.profile_picture_url} alt={group.name} />
                          ) : null}
                          <AvatarFallback>
                            <MessageSquare className="h-5 w-5" />
                          </AvatarFallback>
                        </Avatar>
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm truncate">{group.name}</p>
                          {group.description && (
                            <p className="text-xs text-muted-foreground truncate">{group.description}</p>
                          )}
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {friends.length === 0 && groups.length === 0 && (
                <div className="text-center text-muted-foreground py-8">
                  <MessageSquare className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>No friends or groups yet</p>
                </div>
              )}
            </div>
          </ScrollArea>
        </DialogContent>
      </Dialog>

      <Dialog open={!!reactionPickerMessageId} onOpenChange={() => setReactionPickerMessageId(null)}>
        <DialogContent className="max-w-md p-4">
          <DialogHeader>
            <DialogTitle>Add Reaction</DialogTitle>
            <DialogDescription>
              Choose an emoji to react to this message
            </DialogDescription>
          </DialogHeader>
          <div className="mt-4">
            <EmojiPicker
              mode="react"
              onEmojiSelect={async (emoji) => {
                if (reactionPickerMessageId) {
                  await addReaction.mutateAsync({
                    messageId: reactionPickerMessageId,
                    emoji,
                  });
                  setReactionPickerMessageId(null);
                }
              }}
              onClose={() => setReactionPickerMessageId(null)}
            />
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
