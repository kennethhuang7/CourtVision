import { useState } from 'react';
import { cn } from '@/lib/utils';

interface PlayerAvatarProps {
  src?: string | null;
  name: string;
  className?: string;
  imgClassName?: string;
}

function getInitials(name: string): string {
  if (!name) return '?';
  const parts = name.trim().split(/\s+/);
  if (parts.length === 1) {
    return parts[0].substring(0, 2).toUpperCase();
  }
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

export function PlayerAvatar({ src, name, className, imgClassName }: PlayerAvatarProps) {
  const [imageError, setImageError] = useState(false);
  const initials = getInitials(name);

  const showFallback = !src || imageError;

  return (
    <div
      className={cn(
        'rounded-full overflow-hidden bg-gradient-to-br from-secondary via-muted to-secondary',
        className
      )}
    >
      {!showFallback ? (
        <img
          src={src}
          alt={name}
          className={cn('w-full h-full object-cover object-top', imgClassName)}
          onError={() => setImageError(true)}
        />
      ) : (
        <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-primary/20 via-primary/10 to-secondary text-primary font-semibold">
          <span className="text-[0.4em]">{initials}</span>
        </div>
      )}
    </div>
  );
}
