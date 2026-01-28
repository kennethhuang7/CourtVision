import React, { useState, useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { MessageSquare, Send, Trash2, Archive, ArchiveRestore, Plus, Smile, SmilePlus, ChevronDown, ChevronsDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { getInitials, cn } from '@/lib/utils';
import { Textarea } from '@/components/ui/textarea';
import { ScrollArea } from '@/components/ui/scroll-area';
import { PickShareMessageCard } from '@/components/messages/PickShareMessageCard';
import { EmojiPicker } from '@/components/chat/EmojiPicker';
import { ReactionBar } from '@/components/chat/ReactionBar';
import { EmojiAutocomplete } from '@/components/chat/EmojiAutocomplete';
import { ALL_EMOJIS } from '@/lib/emojiData';
import { applyDefaultSkinTone, loadCustomEmojis, addToRecentlyUsed } from '@/lib/emojiUtils';
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
import { useRemoveReaction } from '@/hooks/useRemoveReaction';
import { useReactionSubscription } from '@/hooks/useReactionSubscription';
import { format, formatDistanceToNow, isToday, isYesterday, isSameDay } from 'date-fns';
import { formatUserTime } from '@/lib/dateUtils';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { logger } from '@/lib/logger';
import { shouldDisplayAsLargeEmoji, getCustomEmojisMap, renderMessageWithCustomEmojis } from '@/lib/emojiUtils';
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
  const [archivedExpanded, setArchivedExpanded] = useState(false);
  const [isAtBottom, setIsAtBottom] = useState(true);
  const [newMessageCount, setNewMessageCount] = useState(0);
  const [cursorPosition, setCursorPosition] = useState(0);
  const [showEmojiAutocomplete, setShowEmojiAutocomplete] = useState(false);
  const [customEmojisMap, setCustomEmojisMap] = useState<Map<string, string>>(new Map());
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const editableRef = useRef<HTMLDivElement>(null);
  const emojiAutocompleteRef = useRef<{ handleKeyDown: (e: React.KeyboardEvent) => boolean } | null>(null);
  const prevMessagesLengthRef = useRef(0);

  useEffect(() => {
    getCustomEmojisMap().then(map => {
      setCustomEmojisMap(map);
    });
  }, []);

  useEffect(() => {
    if (editableRef.current) {
      const currentText = extractTextFromEditable(editableRef.current);
      if (currentText !== messageContent) {
        if (messageContent === '') {
          editableRef.current.innerHTML = '';
        } else {
          updateEditableContent(editableRef.current, messageContent);
        }
      }
    }
  }, [messageContent]);

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
  const removeReaction = useRemoveReaction();
  const { data: friends = [] } = useFriends();
  const { data: groups = [] } = useGroups();

  
  const messageIds = messages.map(m => m.id);
  const { data: reactionsMap = new Map(), isLoading: reactionsLoading } = useBatchMessageReactions(messageIds, !!selectedConversation);
  
  useEffect(() => {
    const pickShareMessages = messages.filter(m => m.message_type === 'pick_share');
    if (pickShareMessages.length > 0 && selectedConversation?.type === 'group') {
      logger.info('Pick share messages in group chat', {
        messageIds: pickShareMessages.map(m => m.id),
        allMessageIds: messageIds,
        reactionsMapSize: reactionsMap.size,
        reactionsLoading
      });
    }
  }, [reactionsMap, messages, selectedConversation, messageIds, reactionsLoading]);

  const scrollToBottom = useCallback((smooth = false) => {
    if (!messagesContainerRef.current) return;
    const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
    if (!viewport) return;
    
    if (smooth && messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth', block: 'end' });
      setTimeout(() => {
        viewport.scrollTop = viewport.scrollHeight;
        setIsAtBottom(true);
        setNewMessageCount(0);
      }, 300);
    } else {
      if (messagesEndRef.current) {
        messagesEndRef.current.scrollIntoView({ behavior: 'auto', block: 'end' });
      }
      
      const scroll = () => {
        const targetScroll = viewport.scrollHeight;
        viewport.scrollTop = targetScroll;
        
        requestAnimationFrame(() => {
          const currentScroll = viewport.scrollTop;
          const maxScroll = viewport.scrollHeight - viewport.clientHeight;
          
          if (currentScroll < maxScroll - 5) {
            viewport.scrollTop = viewport.scrollHeight;
          }
          
          setIsAtBottom(true);
          setNewMessageCount(0);
        });
      };
      
      requestAnimationFrame(scroll);
    }
  }, []);

  const checkIfAtBottom = useCallback(() => {
    if (!messagesContainerRef.current) return false;
    const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
    if (!viewport) return false;
    const threshold = 100;
    const isNearBottom = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < threshold;
    return isNearBottom;
  }, []);

  
  useReactionSubscription(messageIds, !!selectedConversation);

  const lastUnarchiveRef = useRef<string | null>(null);

  useEffect(() => {
    if (selectedConversation) {
      markReadMutation.mutate({
        conversationType: selectedConversation.type,
        conversationId: selectedConversation.id,
      });

      const convKey = `${selectedConversation.type}:${selectedConversation.id}`;
      const conv = conversations.find(
        c => c.conversation_type === selectedConversation.type &&
        c.conversation_id === selectedConversation.id
      );
      if (conv?.is_archived && lastUnarchiveRef.current !== convKey && !archiveMutation.isPending) {
        lastUnarchiveRef.current = convKey;
        archiveMutation.mutate({
          conversationType: selectedConversation.type,
          conversationId: selectedConversation.id,
          archive: false,
        });
      }
    }
  }, [selectedConversation?.type, selectedConversation?.id]);

  useEffect(() => {
    if (selectedConversation && !messagesLoading) {
      if (messages.length > 0) {
        const scrollAfterLoad = () => {
          if (!messagesContainerRef.current) return;
          const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
          if (!viewport) return;
          
          viewport.scrollTop = 0;
          
          const attemptScroll = (delay: number) => {
            setTimeout(() => {
              if (!messagesContainerRef.current) return;
              const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
              if (!viewport) return;
              
              if (messagesEndRef.current) {
                messagesEndRef.current.scrollIntoView({ behavior: 'auto', block: 'end' });
              }
              
              viewport.scrollTop = viewport.scrollHeight;
              
              requestAnimationFrame(() => {
                const maxScroll = viewport.scrollHeight - viewport.clientHeight;
                if (viewport.scrollTop < maxScroll - 5) {
                  viewport.scrollTop = viewport.scrollHeight;
                }
                setIsAtBottom(true);
                setNewMessageCount(0);
                prevMessagesLengthRef.current = messages.length;
              });
            }, delay);
          };
          
          attemptScroll(50);
          attemptScroll(150);
          attemptScroll(300);
          attemptScroll(500);
        };
        
        scrollAfterLoad();
      } else {
        setIsAtBottom(true);
        setNewMessageCount(0);
        prevMessagesLengthRef.current = 0;
      }
    }
  }, [selectedConversation?.id, messages.length, messagesLoading]);

  useEffect(() => {
    if (!selectedConversation || messages.length === 0) return;

    const currentLength = messages.length;
    const prevLength = prevMessagesLengthRef.current;

    if (currentLength > prevLength) {
      const newMessages = currentLength - prevLength;
      
      if (isAtBottom) {
        setTimeout(() => scrollToBottom(false), 100);
      } else {
        setNewMessageCount(prev => prev + newMessages);
      }
    }

    prevMessagesLengthRef.current = currentLength;
  }, [messages.length, isAtBottom, selectedConversation, scrollToBottom]);

  
  useEffect(() => {
    if (!messagesContainerRef.current || !selectedConversation) return;

    const viewport = messagesContainerRef.current.querySelector('[data-radix-scroll-area-viewport]') as HTMLElement;
    if (!viewport) return;

    const handleScroll = () => {
      const atBottom = checkIfAtBottom();
      setIsAtBottom(atBottom);
      if (atBottom) {
        setNewMessageCount(0);
      }
      
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
  }, [hasMore, messagesLoading, selectedConversation, loadMore, checkIfAtBottom]);

  const handleSendMessage = async () => {
    if (!selectedConversation) return;
    
    const textToSend = editableRef.current 
      ? extractTextFromEditable(editableRef.current)
      : messageContent;
    
    if (!textToSend.trim()) return;

    try {
      await sendMessageMutation.mutateAsync({
        conversationType: selectedConversation.type,
        conversationId: selectedConversation.id,
        content: textToSend,
      });
      setMessageContent('');
      if (editableRef.current) {
        editableRef.current.innerHTML = '';
      }
      setTimeout(() => scrollToBottom(false), 100);
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
    logger.debug('Opening group chat for group', { groupId });
    try {
      const conversationId = await ensureGroupConvMutation.mutateAsync(groupId);
      logger.debug('Group conversation ensured', { conversationId });
      
      setSelectedConversation({ type: 'group', id: conversationId });
      setCreateDMOpen(false);
    } catch (error: any) {
      logger.error('Error opening group chat', error as Error);
      
      const existingConv = conversations.find(
        c => c.conversation_type === 'group' && c.group_id === groupId
      );
      if (existingConv) {
        logger.debug('Found existing conversation, opening', { conversationId: existingConv.conversation_id });
        setSelectedConversation({ type: 'group', id: existingConv.conversation_id });
      } else {
        logger.debug('No existing conversation found, using group_id as fallback', { groupId });
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
    if (!editableRef.current) return;

    const text = extractTextFromEditable(editableRef.current);
    const cursorPos = getCursorPosition(editableRef.current);
    const newContent = text.substring(0, cursorPos) + emoji + text.substring(cursorPos);
    setMessageContent(newContent);

    setTimeout(() => {
      if (editableRef.current) {
        const newPosition = cursorPos + emoji.length;
        updateEditableContent(editableRef.current, newContent);
        setCursorPositionInEditable(editableRef.current, newPosition);
        editableRef.current.focus();
      }
    }, 0);
  };

  const extractTextFromEditable = (element: HTMLElement): string => {
    let text = '';
    const walker = document.createTreeWalker(
      element,
      NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT,
      null
    );
    
    let node;
    while (node = walker.nextNode()) {
      if (node.nodeType === Node.TEXT_NODE) {
        text += node.textContent;
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const el = node as HTMLElement;
        if (el.tagName === 'IMG' && el.getAttribute('data-emoji-name')) {
          text += `:${el.getAttribute('data-emoji-name')}:`;
        } else if (el.tagName === 'BR') {
          text += '\n';
        }
      }
    }
    return text;
  };

  const getCursorPosition = (element: HTMLElement): number => {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0) return 0;
    
    const range = selection.getRangeAt(0);
    const preCaretRange = range.cloneRange();
    preCaretRange.selectNodeContents(element);
    preCaretRange.setEnd(range.endContainer, range.endOffset);
    
    let position = 0;
    const walker = document.createTreeWalker(
      element,
      NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT,
      null
    );
    
    let node;
    while (node = walker.nextNode()) {
      if (node === range.endContainer) {
        if (node.nodeType === Node.TEXT_NODE) {
          position += range.endOffset;
        }
        break;
      }
      if (node.nodeType === Node.TEXT_NODE) {
        position += node.textContent?.length || 0;
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const el = node as HTMLElement;
        if (el.tagName === 'IMG' && el.getAttribute('data-emoji-name')) {
          position += `:${el.getAttribute('data-emoji-name')}:`.length;
        } else if (el.tagName === 'BR') {
          position += 1;
        }
      }
    }
    return position;
  };

  const setCursorPositionInEditable = (element: HTMLElement, position: number) => {
    const selection = window.getSelection();
    if (!selection) return;
    
    const range = document.createRange();
    let currentPos = 0;
    const walker = document.createTreeWalker(
      element,
      NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT,
      null
    );
    
    let node;
    while (node = walker.nextNode()) {
      if (node.nodeType === Node.TEXT_NODE) {
        const textNode = node as Text;
        const textLength = textNode.textContent?.length || 0;
        if (currentPos + textLength >= position) {
          range.setStart(textNode, position - currentPos);
          range.setEnd(textNode, position - currentPos);
          selection.removeAllRanges();
          selection.addRange(range);
          return;
        }
        currentPos += textLength;
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const el = node as HTMLElement;
        if (el.tagName === 'IMG' && el.getAttribute('data-emoji-name')) {
          const emojiLength = `:${el.getAttribute('data-emoji-name')}:`.length;
          if (currentPos + emojiLength >= position) {
            if (position === currentPos + emojiLength) {
              range.setStartAfter(el);
              range.setEndAfter(el);
            } else {
              range.setStartBefore(el);
              range.setEndBefore(el);
            }
            selection.removeAllRanges();
            selection.addRange(range);
            return;
          }
          currentPos += emojiLength;
        } else if (el.tagName === 'BR') {
          if (currentPos + 1 >= position) {
            range.setStartBefore(el);
            range.setEndBefore(el);
            selection.removeAllRanges();
            selection.addRange(range);
            return;
          }
          currentPos += 1;
        }
      }
    }
    
    range.selectNodeContents(element);
    range.collapse(false);
    selection.removeAllRanges();
    selection.addRange(range);
  };

  const handleEmojiAutocompleteSelect = (emoji: string, startPos: number, endPos: number) => {
    if (!editableRef.current) return;

    const text = extractTextFromEditable(editableRef.current);
    const newContent = text.substring(0, startPos) + emoji + text.substring(endPos);
    addToRecentlyUsed(emoji);
    setMessageContent(newContent);
    setShowEmojiAutocomplete(false);

    setTimeout(() => {
      if (editableRef.current) {
        const selection = window.getSelection();
        if (selection) {
          selection.removeAllRanges();
        }
        updateEditableContent(editableRef.current, newContent);
        const newPosition = startPos + emoji.length;
        setTimeout(() => {
          if (editableRef.current) {
            setCursorPositionInEditable(editableRef.current, newPosition);
            editableRef.current.focus();
            setCursorPosition(newPosition);
          }
        }, 0);
      }
    }, 0);
  };

  const updateEditableContent = (element: HTMLElement, text: string) => {
    const selection = window.getSelection();
    const range = selection && selection.rangeCount > 0 ? selection.getRangeAt(0).cloneRange() : null;
    const cursorOffset = range ? getCursorPosition(element) : null;
    
    const rendered = renderMessageWithCustomEmojis(text, customEmojisMap);
    element.innerHTML = '';
    rendered.forEach((node) => {
      if (typeof node === 'string') {
        const textNode = document.createTextNode(node);
        element.appendChild(textNode);
      } else if (React.isValidElement(node) && node.type === 'img') {
        const img = document.createElement('img');
        const src = (node.props as any).src;
        const alt = (node.props as any).alt || '';
        const className = (node.props as any).className || '';
        const style = (node.props as any).style || {};
        
        img.src = src;
        img.alt = alt;
        img.className = className;
        Object.assign(img.style, style);
        img.setAttribute('contenteditable', 'false');
        img.setAttribute('draggable', 'false');
        
        const emojiName = src.replace('/custom-emojis/', '').replace(/\.(png|gif|jpg|jpeg|webp)$/i, '');
        img.setAttribute('data-emoji-name', emojiName);
        
        element.appendChild(img);
      }
    });
    
    if (cursorOffset !== null) {
      setTimeout(() => {
        setCursorPositionInEditable(element, cursorOffset);
      }, 0);
    }
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
            ) : (() => {
              const activeConversations = conversations.filter(c => !c.is_archived);
              const archivedConversations = conversations.filter(c => c.is_archived);

              const renderConversation = (conv: Conversation, isArchived: boolean) => {
                const display = getConversationDisplay(conv);
                const isSelected =
                  selectedConversation?.type === conv.conversation_type &&
                  selectedConversation?.id === conv.conversation_id;

                return (
                  <motion.div
                    key={conv.id}
                    layout
                    initial={{ opacity: 0, x: isArchived ? -20 : 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: isArchived ? 20 : -20 }}
                    transition={{ duration: 0.3, ease: "easeInOut" }}
                  >
                    <button
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
                          <AvatarImage src={display.avatar} alt={display.name} />
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
                            <p className="text-xs text-muted-foreground truncate mb-1 flex items-center gap-1">
                              {renderMessageWithCustomEmojis(conv.last_message_preview, customEmojisMap)}
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
                  </motion.div>
                );
              };

              return (
                <div>
                  <AnimatePresence mode="popLayout">
                    {activeConversations.length === 0 && archivedConversations.length > 0 ? (
                      <motion.div
                        key="no-active"
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        exit={{ opacity: 0 }}
                        className="p-4 text-center text-sm text-muted-foreground"
                      >
                        <MessageSquare className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p>No active conversations</p>
                      </motion.div>
                    ) : (
                      activeConversations.map(conv => renderConversation(conv, false))
                    )}
                  </AnimatePresence>

                  <div className="border-t border-border/50 mt-2">
                    <button
                      onClick={() => setArchivedExpanded(!archivedExpanded)}
                      disabled={archivedConversations.length === 0}
                      className={cn(
                        'w-full px-4 py-2 flex items-center justify-between text-sm transition-colors',
                        archivedConversations.length === 0
                          ? 'text-muted-foreground/50 cursor-not-allowed'
                          : 'text-muted-foreground hover:text-foreground hover:bg-secondary/30'
                      )}
                    >
                      <div className="flex items-center gap-2">
                        <Archive className="h-4 w-4" />
                        <span>Archived</span>
                        {archivedConversations.length > 0 && (
                          <span className="text-xs bg-muted px-1.5 py-0.5 rounded-full">
                            {archivedConversations.length}
                          </span>
                        )}
                      </div>
                      {archivedConversations.length > 0 && (
                        <motion.div
                          animate={{ rotate: archivedExpanded ? 180 : 0 }}
                          transition={{ duration: 0.2 }}
                        >
                          <ChevronDown className="h-4 w-4" />
                        </motion.div>
                      )}
                    </button>

                    <AnimatePresence initial={false} mode="popLayout">
                      {archivedExpanded && archivedConversations.length > 0 && (
                        <motion.div
                          initial={{ height: 0, opacity: 0 }}
                          animate={{ height: "auto", opacity: 1 }}
                          exit={{ height: 0, opacity: 0 }}
                          transition={{ duration: 0.2, ease: "easeInOut" }}
                          className="overflow-hidden"
                        >
                          <div className="bg-muted/20">
                            <AnimatePresence mode="popLayout">
                              {archivedConversations.map(conv => renderConversation(conv, true))}
                            </AnimatePresence>
                          </div>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </div>
                </div>
              );
            })()}
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
                          <AvatarImage src={display.avatar} alt={display.name} />
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
                {selectedConv && (
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

              <ScrollArea className="flex-1 relative" ref={messagesContainerRef}>
                {!isAtBottom && newMessageCount > 0 && (
                  <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-50">
                    <div className="flex items-center gap-3 bg-card border border-border rounded-lg px-4 py-2 shadow-lg">
                      <span className="text-sm text-muted-foreground">
                        {newMessageCount} new {newMessageCount === 1 ? 'message' : 'messages'}
                      </span>
                      <Button
                        onClick={() => scrollToBottom(true)}
                        size="sm"
                        variant="default"
                      >
                        <ChevronsDown className="h-4 w-4 mr-1" />
                        Jump to bottom
                      </Button>
                    </div>
                  </div>
                )}
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
                      <AnimatePresence initial={false}>
                      {messages.map((message, index) => {
                        const isOwn = message.sender_id === user?.id;
                        const isDeleted = message.is_deleted === true;


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
                          <motion.div
                            key={message.id}
                            initial={{ opacity: 0, y: 10, scale: 0.98 }}
                            animate={{ opacity: 1, y: 0, scale: 1 }}
                            exit={{ opacity: 0, scale: 0.98 }}
                            transition={{ duration: 0.2, ease: "easeOut" }}
                          >
                            {showDateDivider && (
                              <div className="flex items-center gap-3 my-6">
                                <div className="flex-1 h-px bg-border"></div>
                                <span className="text-xs font-medium text-muted-foreground px-3">
                                  {getDateDividerLabel(currentMessageDate)}
                                </span>
                                <div className="flex-1 h-px bg-border"></div>
                              </div>
                            )}

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
                                    <AvatarImage
                                      src={senderAvatar}
                                      alt={senderName}
                                    />
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

                                  {(() => {
                                    const messageIdForMenu = message.id;
                                    return (
                                      <ContextMenu key={`context-menu-${messageIdForMenu}`}>
                                        <ContextMenuTrigger asChild>
                                          {message.message_type === 'pick_share' && message.metadata ? (
                                            <div>
                                              <PickShareMessageCard
                                                metadata={message.metadata}
                                                isOwn={isOwn}
                                                conversationType={selectedConversation?.type}
                                                groupId={selectedConv?.group_id}
                                                friendId={selectedConv?.other_user_id}
                                              />
                                            </div>
                                          ) : shouldDisplayAsLargeEmoji(message.content) && !isDeleted ? (
                                            <div className="text-6xl leading-none">
                                              {(() => {
                                                const hasCustomEmoji = /:([a-z0-9_+-]+):|\/custom-emojis\//i.test(message.content);
                                                if (!hasCustomEmoji) {
                                                  return message.content;
                                                }
                                                const rendered = renderMessageWithCustomEmojis(message.content, customEmojisMap);
                                                return rendered.map((node, i) => {
                                                  if (React.isValidElement(node) && node.type === 'img') {
                                                    return React.cloneElement(node as React.ReactElement<{ className?: string; style?: React.CSSProperties }>, { 
                                                      key: i,
                                                      className: 'inline-block w-[5rem] h-[5rem] object-contain',
                                                      style: {
                                                        imageRendering: 'crisp-edges' as any,
                                                      }
                                                    });
                                                  }
                                                  return <span key={i}>{node}</span>;
                                                });
                                              })()}
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
                                                <p className="whitespace-pre-wrap">
                                                  {renderMessageWithCustomEmojis(message.content, customEmojisMap)}
                                                </p>
                                              )}
                                            </div>
                                          )}
                                        </ContextMenuTrigger>
                                        <ContextMenuContent className="z-[200] min-w-[180px] w-auto" onCloseAutoFocus={(e) => e.preventDefault()}>
                                          {!isDeleted && (
                                            <ContextMenuItem
                                              onClick={(e) => {
                                                e.preventDefault();
                                                e.stopPropagation();
                                                setReactionPickerMessageId(messageIdForMenu);
                                              }}
                                              className="cursor-pointer"
                                            >
                                              <SmilePlus className="h-4 w-4 mr-2 shrink-0" />
                                              <span className="whitespace-nowrap">Add Reaction</span>
                                            </ContextMenuItem>
                                          )}
                                          {isOwn && (
                                            <>
                                              <ContextMenuSeparator />
                                              <ContextMenuItem
                                                onClick={message.message_type !== 'pick_share' && !isDeleted ? () => handleDeleteMessage(messageIdForMenu) : undefined}
                                                className={cn(
                                                  message.message_type === 'pick_share' || isDeleted
                                                    ? "text-muted-foreground/50 cursor-not-allowed"
                                                    : "text-destructive focus:text-destructive cursor-pointer"
                                                )}
                                                disabled={message.message_type === 'pick_share' || isDeleted}
                                              >
                                                <Trash2 className="h-4 w-4 mr-2 shrink-0" />
                                                <span className="whitespace-nowrap">Delete Message</span>
                                              </ContextMenuItem>
                                            </>
                                          )}
                                        </ContextMenuContent>
                                      </ContextMenu>
                                    );
                                  })()}

                                  {(() => {
                                    const messageReactions = reactionsMap.get(message.id);
                                    const shouldShowReactions = messageReactions && messageReactions.length > 0;
                                    
                                    if (shouldShowReactions) {
                                      return (
                                        <div 
                                          className={cn(
                                            'mt-1.5 w-full',
                                            isOwn ? 'flex justify-end' : 'flex justify-start'
                                          )}
                                          style={{ position: 'relative', zIndex: 10 }}
                                        >
                                          <ReactionBar
                                            messageId={message.id}
                                            reactions={messageReactions}
                                          />
                                        </div>
                                      );
                                    }
                                    return null;
                                  })()}
                                </div>
                              </div>
                            </div>
                          </motion.div>
                        );
                      })}
                      </AnimatePresence>
                      <div ref={messagesEndRef} />
                    </div>
                  )}
                </div>
              </ScrollArea>

              <div className="h-auto px-4 py-3 border-t border-border shrink-0 bg-card">
                <div className="flex items-end gap-2">
                  <div className="relative flex-1">
                    <div
                      ref={editableRef}
                      contentEditable
                      suppressContentEditableWarning
                      onInput={async (e) => {
                        const target = e.currentTarget;
                        const text = extractTextFromEditable(target);
                        const newCursorPos = getCursorPosition(target);
                        const beforeCursor = text.substring(0, newCursorPos);
                        
                        const shortcodePattern = /:([a-z0-9_+-]+):$/;
                        const match = beforeCursor.match(shortcodePattern);
                        
                        if (match) {
                          const shortcode = match[1].toLowerCase();
                          const exactMatch = ALL_EMOJIS.find(item => item.name.toLowerCase() === shortcode);
                          if (exactMatch) {
                            const emoji = applyDefaultSkinTone(exactMatch.emoji, exactMatch.supportsSkinTone || false);
                            const startPos = beforeCursor.lastIndexOf(':' + shortcode + ':');
                            const newContent = text.substring(0, startPos) + emoji + text.substring(newCursorPos);
                            addToRecentlyUsed(emoji);
                            setMessageContent(newContent);
                            setShowEmojiAutocomplete(false);
                            setTimeout(() => {
                              updateEditableContent(target, newContent);
                              const newPosition = startPos + emoji.length;
                              setCursorPositionInEditable(target, newPosition);
                              target.focus();
                              setCursorPosition(newPosition);
                            }, 0);
                            return;
                          }
                          
                          const customEmojis = await loadCustomEmojis();
                          const customMatch = customEmojis.find(item => item.name.toLowerCase() === shortcode);
                          if (customMatch) {
                            const startPos = beforeCursor.lastIndexOf(':' + shortcode + ':');
                            const shortcodeToken = `:${shortcode}:`;
                            const newContent = text.substring(0, startPos) + shortcodeToken + text.substring(newCursorPos);
                            addToRecentlyUsed(shortcodeToken);
                            setMessageContent(newContent);
                            setShowEmojiAutocomplete(false);
                            setTimeout(() => {
                              updateEditableContent(target, newContent);
                              const newPosition = startPos + shortcodeToken.length;
                              setCursorPositionInEditable(target, newPosition);
                              target.focus();
                              setCursorPosition(newPosition);
                            }, 0);
                            return;
                          }
                          
                          setMessageContent(text);
                          setCursorPosition(newCursorPos);
                          setShowEmojiAutocomplete(false);
                          return;
                        }
                        
                        const colonIndex = beforeCursor.lastIndexOf(':');
                        if (colonIndex !== -1) {
                          const afterColon = beforeCursor.substring(colonIndex + 1);
                          if (!afterColon.includes(' ') && !afterColon.includes('\n') && !afterColon.includes(':')) {
                            setMessageContent(text);
                            setCursorPosition(newCursorPos);
                            setShowEmojiAutocomplete(true);
                            return;
                          }
                        }
                        setMessageContent(text);
                        setCursorPosition(newCursorPos);
                        setShowEmojiAutocomplete(false);
                      }}
                      onKeyDown={(e) => {
                        if (showEmojiAutocomplete && emojiAutocompleteRef.current) {
                          const handled = emojiAutocompleteRef.current.handleKeyDown(e);
                          if (handled) return;
                        }
                        if (e.key === 'Enter' && !e.shiftKey) {
                          e.preventDefault();
                          handleSendMessage();
                        }
                      }}
                      onBlur={() => {
                        if (editableRef.current) {
                          const text = extractTextFromEditable(editableRef.current);
                          setMessageContent(text);
                        }
                      }}
                      className="min-h-[44px] max-h-[120px] resize-none text-sm bg-background pr-10 py-2 px-3 rounded-md border border-input focus:outline-none focus:border-ring focus:ring-2 focus:ring-ring focus:ring-offset-0 overflow-y-auto whitespace-pre-wrap break-words"
                      style={{ 
                        outline: 'none',
                        wordWrap: 'break-word',
                        overflowWrap: 'break-word'
                      }}
                      data-placeholder="Type a message..."
                    />
                    <style>{`
                      [contenteditable][data-placeholder]:empty:before {
                        content: attr(data-placeholder);
                        color: hsl(var(--muted-foreground));
                        pointer-events: none;
                      }
                    `}</style>
                    {showEmojiAutocomplete && (
                      <EmojiAutocomplete
                        ref={emojiAutocompleteRef}
                        text={messageContent}
                        cursorPosition={cursorPosition}
                        onSelect={handleEmojiAutocompleteSelect}
                        onClose={() => setShowEmojiAutocomplete(false)}
                      />
                    )}
                    <Popover open={emojiPickerOpen} onOpenChange={setEmojiPickerOpen}>
                      <PopoverTrigger asChild>
                        <button
                          className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded hover:bg-muted transition-colors opacity-60 hover:opacity-100"
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
                            <AvatarImage src={friend.profile_picture_url} alt={displayName} />
                            <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-sm font-semibold text-white">
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
                          <AvatarImage src={group.profile_picture_url} alt={group.name} />
                          <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-white">
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
              userReactions={
                reactionPickerMessageId && reactionsMap.get(reactionPickerMessageId)
                  ? reactionsMap.get(reactionPickerMessageId)!
                      .filter(r => r.user_ids.includes(user?.id || ''))
                      .map(r => r.emoji)
                  : []
              }
              onEmojiToggle={async (emoji, isRemoving) => {
                if (reactionPickerMessageId) {
                  try {
                    if (isRemoving) {
                      await removeReaction.mutateAsync({
                        messageId: reactionPickerMessageId,
                        emoji,
                      });
                    } else {
                      await addReaction.mutateAsync({
                        messageId: reactionPickerMessageId,
                        emoji,
                      });
                    }
                    setReactionPickerMessageId(null);
                  } catch (error: any) {
                    logger.error('Failed to toggle reaction', error as Error, { 
                      messageId: reactionPickerMessageId, 
                      emoji,
                      isRemoving,
                      errorCode: error?.code,
                      errorMessage: error?.message 
                    });
                  }
                }
              }}
              onEmojiSelect={async (emoji) => {
                if (reactionPickerMessageId) {
                  try {
                    await addReaction.mutateAsync({
                      messageId: reactionPickerMessageId,
                      emoji,
                    });
                    setReactionPickerMessageId(null);
                  } catch (error: any) {
                    logger.error('Failed to add reaction', error as Error, { 
                      messageId: reactionPickerMessageId, 
                      emoji,
                      errorCode: error?.code,
                      errorMessage: error?.message 
                    });
                  }
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
