

import { logger } from './logger';


export async function fetchImageAsBase64(url: string): Promise<string | null> {
  return new Promise((resolve) => {
    const img = new Image();

    
    img.crossOrigin = 'anonymous';

    img.onload = () => {
      try {
        
        const canvas = document.createElement('canvas');
        canvas.width = img.width;
        canvas.height = img.height;

        const ctx = canvas.getContext('2d');
        if (!ctx) {
          logger.warn('Failed to get canvas context');
          resolve(null);
          return;
        }

        ctx.drawImage(img, 0, 0);

        
        const base64 = canvas.toDataURL('image/png');
        resolve(base64);
      } catch (error) {
        logger.error('Failed to convert image to base64 via canvas', error as Error);
        resolve(null);
      }
    };

    img.onerror = () => {
      logger.warn(`Failed to load image: ${url}`);
      resolve(null);
    };

    
    img.src = url;
  });
}


export async function isImageUrlReachable(url: string): Promise<boolean> {
  try {
    const response = await fetch(url, { method: 'HEAD' });
    return response.ok;
  } catch {
    return false;
  }
}
