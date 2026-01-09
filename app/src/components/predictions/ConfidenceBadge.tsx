import { cn } from '@/lib/utils';
import { ConfidenceLevel } from '@/types/nba';

interface ConfidenceBadgeProps {
  confidence: number;
  showValue?: boolean;
  size?: 'sm' | 'md' | 'lg';
}

export function getConfidenceLevel(confidence: number): ConfidenceLevel {
  if (confidence >= 80) return 'high';
  if (confidence >= 60) return 'medium';
  return 'low';
}

export function ConfidenceBadge({ confidence, showValue = true, size = 'md' }: ConfidenceBadgeProps) {
  const level = getConfidenceLevel(confidence);
  
  const sizeClasses = {
    sm: 'px-3 py-1 text-xs',
    md: 'px-3 py-1 text-sm',
    lg: 'px-4 py-1.5 text-base',
  };

  return (
    <span className={cn(
      'inline-flex items-center gap-1.5 rounded-full font-medium border',
      sizeClasses[size],
      level === 'high' && 'confidence-high',
      level === 'medium' && 'confidence-medium',
      level === 'low' && 'confidence-low',
    )}>
      <span className={cn(
        'h-2 w-2 rounded-full',
        level === 'high' && 'bg-green-400',
        level === 'medium' && 'bg-amber-400',
        level === 'low' && 'bg-red-400',
      )} />
      {showValue && `${confidence}%`}
      {!showValue && (
        <span className="capitalize">{level}</span>
      )}
    </span>
  );
}
