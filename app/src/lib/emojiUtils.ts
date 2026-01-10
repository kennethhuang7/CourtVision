

import { RECENTLY_USED_KEY, MAX_RECENT_EMOJIS, ALL_EMOJIS, type SkinTone, applySkinTone } from './emojiData';
import { logger } from './logger';

const SKIN_TONE_PREFERENCE_KEY = 'courtvision-emoji-skin-tone';


const EMOJI_REGEX = /(\p{Emoji_Presentation}|\p{Emoji}\uFE0F)/gu;


export function isEmojiOnly(text: string): boolean {
  if (!text || text.trim().length === 0) return false;

  
  const withoutEmojis = text.replace(EMOJI_REGEX, '').replace(/\s/g, '');

  
  return withoutEmojis.length === 0;
}


export function countEmojis(text: string): number {
  const matches = text.match(EMOJI_REGEX);
  return matches ? matches.length : 0;
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


export function addToRecentlyUsed(emoji: string): void {
  try {
    const recent = getRecentlyUsedEmojis();

    
    const filtered = recent.filter(e => e !== emoji);

    
    const updated = [emoji, ...filtered].slice(0, MAX_RECENT_EMOJIS);

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
    const response = await fetch('/custom-emojis/manifest.json');

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
        url: `/custom-emojis/${item.filename}`,
        emoji: `/custom-emojis/${item.filename}`,
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
