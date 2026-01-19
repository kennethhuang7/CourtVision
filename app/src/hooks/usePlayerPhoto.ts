

import { useState, useEffect } from 'react';
import { cacheManager } from '@/lib/cache';
import { fetchImageAsBase64 } from '@/lib/imageUtils';
import { logger } from '@/lib/logger';


export function usePlayerPhoto(playerId: string | number): string {
  const id = String(playerId);
  const cdnUrl = `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${id}.png`;
  const [photoUrl, setPhotoUrl] = useState<string>(cdnUrl);

  useEffect(() => {
    let mounted = true;

    async function loadPhoto() {
      try {
        
        const cached = await cacheManager.getPlayerPhoto(id);

        if (cached && mounted) {
          
          if (cached.startsWith('data:')) {
            setPhotoUrl(cached);
            return;
          }
          
        }

        
        
        fetchImageAsBase64(cdnUrl).then(async (base64) => {
          if (base64 && mounted) {
            
            await cacheManager.savePlayerPhoto(id, cdnUrl, base64);
            
            setPhotoUrl(base64);
            logger.info(`Cached player photo for ${id}`);
          }
        }).catch((error) => {
          logger.warn(`Failed to cache player photo for ${id}`, error);
        });

      } catch (error) {
        logger.error(`Error loading player photo for ${id}`, error as Error);
      }
    }

    loadPhoto();

    return () => {
      mounted = false;
    };
  }, [id, cdnUrl]);

  return photoUrl;
}
