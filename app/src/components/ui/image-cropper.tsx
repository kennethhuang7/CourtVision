import { useState, useCallback, useRef } from 'react';
import ReactCrop, { Crop, PixelCrop } from 'react-image-crop';
import 'react-image-crop/dist/ReactCrop.css';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { logger } from '@/lib/logger';

interface ImageCropperProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  imageSrc: string;
  onCropComplete: (croppedImageBlob: Blob) => void;
  aspectRatio: number;
  cropShape?: 'rect' | 'round';
  title?: string;
}


async function getCroppedImg(
  image: HTMLImageElement,
  crop: PixelCrop,
  aspectRatio: number
): Promise<Blob> {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');

  if (!ctx) {
    throw new Error('No 2d context');
  }

  
  const outputWidth = aspectRatio === 3 ? 1920 : 512;
  const outputHeight = aspectRatio === 3 ? 640 : 512;

  canvas.width = outputWidth;
  canvas.height = outputHeight;

  
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  
  ctx.drawImage(
    image,
    crop.x,
    crop.y,
    crop.width,
    crop.height,
    0,
    0,
    outputWidth,
    outputHeight
  );

  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (!blob) {
        reject(new Error('Canvas is empty'));
        return;
      }
      resolve(blob);
    }, 'image/jpeg', 0.95);
  });
}

export function ImageCropper({
  open,
  onOpenChange,
  imageSrc,
  onCropComplete,
  aspectRatio,
  cropShape = 'rect',
  title = 'Crop Image',
}: ImageCropperProps) {
  const [crop, setCrop] = useState<Crop>();
  const [completedCrop, setCompletedCrop] = useState<PixelCrop>();
  const [isProcessing, setIsProcessing] = useState(false);
  const imgRef = useRef<HTMLImageElement>(null);

  const onImageLoad = useCallback((e: React.SyntheticEvent<HTMLImageElement>) => {
    const { width, height } = e.currentTarget;

    
    let cropWidth: number;
    let cropHeight: number;

    if (aspectRatio === 1) {
      
      const size = Math.min(width, height);
      cropWidth = size;
      cropHeight = size;
    } else {
      
      cropWidth = width;
      cropHeight = width / aspectRatio;

      
      if (cropHeight > height) {
        cropHeight = height;
        cropWidth = height * aspectRatio;
      }
    }

    
    const x = (width - cropWidth) / 2;
    const y = (height - cropHeight) / 2;

    setCrop({
      unit: 'px',
      width: cropWidth,
      height: cropHeight,
      x: Math.max(0, x),
      y: Math.max(0, y),
    });
  }, [aspectRatio]);

  const handleSave = async () => {
    const image = imgRef.current;
    if (!completedCrop || !image) return;

    setIsProcessing(true);
    try {
      const croppedImageBlob = await getCroppedImg(image, completedCrop, aspectRatio);
      onCropComplete(croppedImageBlob);
      onOpenChange(false);

      
      setCrop(undefined);
      setCompletedCrop(undefined);
    } catch (error) {
      logger.error('Error cropping image', error as Error);
      alert('Error cropping image. Please try again.');
    } finally {
      setIsProcessing(false);
    }
  };

  const handleCancel = () => {
    onOpenChange(false);
    setCrop(undefined);
    setCompletedCrop(undefined);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-5xl w-full max-h-[95vh] flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4 flex-shrink-0">
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>

        <div className="flex-1 relative bg-black/50 min-h-0 flex items-center justify-center p-4">
          <ReactCrop
            crop={crop}
            onChange={(c) => setCrop(c)}
            onComplete={(c) => setCompletedCrop(c)}
            aspect={aspectRatio}
            circularCrop={cropShape === 'round'}
            style={{ maxHeight: '100%', maxWidth: '100%' }}
          >
            <img
              ref={imgRef}
              src={imageSrc}
              onLoad={onImageLoad}
              alt="Crop preview"
              style={{
                maxHeight: 'calc(95vh - 200px)',
                maxWidth: '100%',
                display: 'block'
              }}
            />
          </ReactCrop>
        </div>

        <div className="px-6 py-4 border-t flex-shrink-0">
          <DialogFooter className="gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={handleCancel}
              disabled={isProcessing}
            >
              Cancel
            </Button>
            <Button
              type="button"
              onClick={handleSave}
              disabled={isProcessing || !completedCrop}
            >
              {isProcessing ? 'Processing...' : 'Save'}
            </Button>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}
