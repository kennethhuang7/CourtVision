import { cn } from '@/lib/utils';

interface StatBadgeProps {
  label: string;
  value: number;
  actual?: number;
  className?: string;
}

export function StatBadge({ label, value, actual, className }: StatBadgeProps) {
  const diff = actual !== undefined ? value - actual : undefined;
  
  return (
    <div className={cn('flex flex-col items-center', className)}>
      <span className="data-label mb-1">{label}</span>
      <span className="text-lg font-bold text-foreground">{value}</span>
      {diff !== undefined && (
        <span className={cn(
          'text-xs font-medium mt-0.5',
          diff > 0 ? 'text-destructive' : diff < 0 ? 'text-success' : 'text-muted-foreground'
        )}>
          {diff > 0 ? '+' : ''}{diff.toFixed(1)}
        </span>
      )}
    </div>
  );
}
