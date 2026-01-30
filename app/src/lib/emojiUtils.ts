

import React from 'react';
import { RECENTLY_USED_KEY, MAX_RECENT_EMOJIS, ALL_EMOJIS, type SkinTone, applySkinTone } from './emojiData';
import { logger } from './logger';

const SKIN_TONE_PREFERENCE_KEY = 'courtvision-emoji-skin-tone';


const EMOJI_REGEX = /(\p{Emoji_Presentation}|\p{Emoji}\uFE0F)/gu;


export function isEmojiOnly(text: string): boolean {
  if (!text || text.trim().length === 0) return false;

  const customEmojiPattern = /:([a-z0-9_+-]+):|\.?\/custom-emojis\/[^\s]+/gi;
  const withoutEmojis = text
    .replace(EMOJI_REGEX, '')
    .replace(customEmojiPattern, '')
    .replace(/\s/g, '');

  return withoutEmojis.length === 0;
}


export function countEmojis(text: string): number {
  const unicodeMatches = text.match(EMOJI_REGEX) || [];
  const customEmojiPattern = /:([a-z0-9_+-]+):|\.?\/custom-emojis\/[^\s]+/gi;
  const customMatches = text.match(customEmojiPattern) || [];
  return unicodeMatches.length + (customMatches ? customMatches.length : 0);
}


export function extractEmojis(text: string): string[] {
  const matches = text.match(EMOJI_REGEX);
  return matches || [];
}


export function shouldDisplayAsLargeEmoji(text: string): boolean {
  if (!isEmojiOnly(text)) return false;

  const emojiCount = countEmojis(text);
  return emojiCount >= 1 && emojiCount <= 3;
}


export function getRecentlyUsedEmojis(): string[] {
  try {
    const stored = localStorage.getItem(RECENTLY_USED_KEY);
    if (!stored) return [];

    const recent = JSON.parse(stored);
    return Array.isArray(recent) ? recent.slice(0, MAX_RECENT_EMOJIS) : [];
  } catch (error) {
    logger.error('Failed to load recently used emojis', error as Error);
    return [];
  }
}

function getCustomEmojiNameFromToken(token: string): string | null {
  const shortcodeMatch = token.match(/^:([a-z0-9_+-]+):$/i);
  if (shortcodeMatch?.[1]) return shortcodeMatch[1].toLowerCase();

  const urlMatch = token.match(/\.?\/custom-emojis\/([a-z0-9_+-]+)\.(png|gif|jpg|jpeg|webp)$/i);
  if (urlMatch?.[1]) return urlMatch[1].toLowerCase();

  return null;
}

function normalizeRecentEmojiToken(token: string): string {
  const name = getCustomEmojiNameFromToken(token);
  if (name) return `:${name}:`;
  return token;
}

export async function pruneRecentlyUsedEmojis(): Promise<string[]> {
  try {
    const stored = localStorage.getItem(RECENTLY_USED_KEY);
    if (!stored) return [];

    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) return [];

    const custom = await loadCustomEmojis();
    const validCustomNames = new Set(custom.map(e => e.name.toLowerCase()));

    const seen = new Set<string>();
    const cleaned: string[] = [];

    for (const raw of parsed) {
      if (typeof raw !== 'string') continue;
      const token = normalizeRecentEmojiToken(raw);
      const customName = getCustomEmojiNameFromToken(token);

      if (customName && !validCustomNames.has(customName)) continue;
      if (seen.has(token)) continue;
      seen.add(token);
      cleaned.push(token);
      if (cleaned.length >= MAX_RECENT_EMOJIS) break;
    }

    localStorage.setItem(RECENTLY_USED_KEY, JSON.stringify(cleaned));
    return cleaned;
  } catch (error) {
    logger.error('Failed to prune recently used emojis', error as Error);
    return getRecentlyUsedEmojis();
  }
}


export function addToRecentlyUsed(emoji: string): void {
  try {
    const normalized = normalizeRecentEmojiToken(emoji);
    const recent = getRecentlyUsedEmojis().map(normalizeRecentEmojiToken);

    
    const filtered = recent.filter(e => e !== normalized);

    
    const updated = [normalized, ...filtered].slice(0, MAX_RECENT_EMOJIS);

    localStorage.setItem(RECENTLY_USED_KEY, JSON.stringify(updated));
  } catch (error) {
    logger.error('Failed to save recently used emoji', error as Error);
  }
}


export function searchEmojis(query: string): string[] {
  if (!query || query.trim().length === 0) return [];

  const lowerQuery = query.toLowerCase().trim();

  const matches = ALL_EMOJIS.filter(item => {
    if (item.name.toLowerCase().includes(lowerQuery)) return true;
    return item.keywords.some(keyword =>
      keyword.toLowerCase().includes(lowerQuery)
    );
  });

  return matches.map(item => item.emoji).slice(0, 50);
}


export async function loadCustomEmojis(): Promise<Array<{ name: string; url: string; emoji: string }>> {
  try {
    const basePath = import.meta.env.BASE_URL || '/';
    const manifestUrl = `${basePath}custom-emojis/manifest.json`;

    const response = await fetch(manifestUrl);

    if (!response.ok) {
      return [];
    }

    const manifest = await response.json();

    if (!Array.isArray(manifest) || manifest.length === 0) {
      return [];
    }

    const customEmojis = manifest
      .filter(item => item.name && item.filename)
      .map(item => ({
        name: item.name,
        url: `${basePath}custom-emojis/${item.filename}`,
        emoji: `${basePath}custom-emojis/${item.filename}`,
      }));

    return customEmojis;
  } catch (error) {
    logger.error('Failed to load custom emojis', error as Error);
    return [];
  }
}


export function getEmojiName(emoji: string): string {
  const found = ALL_EMOJIS.find(item => item.emoji === emoji);
  return found ? found.name : emoji;
}


export function getSkinTonePreference(): SkinTone {
  try {
    const stored = localStorage.getItem(SKIN_TONE_PREFERENCE_KEY);
    if (!stored) return 'default';

    const validTones: SkinTone[] = ['default', 'light', 'mediumLight', 'medium', 'mediumDark', 'dark'];
    return validTones.includes(stored as SkinTone) ? (stored as SkinTone) : 'default';
  } catch (error) {
    logger.error('Failed to load skin tone preference', error as Error);
    return 'default';
  }
}


export function setSkinTonePreference(skinTone: SkinTone): void {
  try {
    localStorage.setItem(SKIN_TONE_PREFERENCE_KEY, skinTone);
  } catch (error) {
    logger.error('Failed to save skin tone preference', error as Error);
  }
}


export function applyDefaultSkinTone(emoji: string, supportsSkinTone: boolean): string {
  if (!supportsSkinTone) return emoji;

  const preference = getSkinTonePreference();
  return applySkinTone(emoji, preference);
}

let customEmojisCache: Array<{ name: string; url: string; emoji: string }> | null = null;

export async function getCustomEmojisMap(forceReload = false): Promise<Map<string, string>> {
  if (!customEmojisCache || forceReload) {
    customEmojisCache = await loadCustomEmojis();
  }
  const map = new Map<string, string>();
  customEmojisCache.forEach(emoji => {
    map.set(emoji.name.toLowerCase(), emoji.url);
  });
  return map;
}

export function renderMessageWithCustomEmojis(content: string, customEmojisMap: Map<string, string>): React.ReactNode[] {
  if (!content) return [content];

  const parts: React.ReactNode[] = [];
  const customEmojiPattern = /:([a-z0-9_+-]+):|\.?\/custom-emojis\/([a-z0-9_+-]+\.(png|gif|jpg|jpeg|webp))/gi;
  let lastIndex = 0;
  let key = 0;
  const matches: Array<{ index: number; length: number; name: string | null; url: string }> = [];
  
  let match;
  const pattern = new RegExp(customEmojiPattern.source, customEmojiPattern.flags);
  while ((match = pattern.exec(content)) !== null) {
    if (match.index === undefined) break;
    
    let emojiName: string | null = null;
    let emojiUrl: string | undefined;
    
    if (match[1]) {
      emojiName = match[1].toLowerCase();
      emojiUrl = customEmojisMap.get(emojiName);
      if (!emojiUrl) continue;
    } else if (match[2]) {
      const filename = match[2];
      const nameWithoutExt = filename.replace(/\.(png|gif|jpg|jpeg|webp)$/i, '');
      emojiName = nameWithoutExt.toLowerCase();
      emojiUrl = customEmojisMap.get(emojiName);
      if (!emojiUrl) continue;
    } else {
      continue;
    }
    
    matches.push({
      index: match.index,
      length: match[0].length,
      name: emojiName,
      url: emojiUrl
    });
  }

  if (matches.length === 0) {
    return [content];
  }

  matches.forEach((emojiMatch, idx) => {
    if (emojiMatch.index > lastIndex) {
      const textPart = content.substring(lastIndex, emojiMatch.index);
      if (textPart) parts.push(textPart);
    }
    
    parts.push(
      React.createElement('img', {
        key: key++,
        src: emojiMatch.url,
        alt: emojiMatch.name || 'custom emoji',
        className: 'inline-block w-[1.2em] h-[1.2em] align-middle mx-0.5',
        style: {
          imageRendering: 'crisp-edges' as any,
        }
      })
    );
    
    lastIndex = emojiMatch.index + emojiMatch.length;
  });

  if (lastIndex < content.length) {
    const remaining = content.substring(lastIndex);
    if (remaining) parts.push(remaining);
  }

  return parts.length > 0 ? parts : [content];
}

export function stripUnknownCustomEmojis(content: string, customEmojisMap: Map<string, string>): string {
  if (!content) return content;

  const pattern = /:([a-z0-9_+-]+):|\.?\/custom-emojis\/([a-z0-9_+-]+\.(png|gif|jpg|jpeg|webp))/gi;

  const stripped = content.replace(pattern, (full, shortcodeName, filename) => {
    let name: string | null = null;
    if (shortcodeName) {
      name = String(shortcodeName).toLowerCase();
    } else if (filename) {
      name = String(filename).replace(/\.(png|gif|jpg|jpeg|webp)$/i, '').toLowerCase();
    }

    if (!name) return full;
    if (customEmojisMap.has(name)) return full;
    return '';
  });

  return stripped.replace(/\s+/g, ' ').trim();
}
