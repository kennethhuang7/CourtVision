import { cn } from '@/lib/utils';
import { LucideIcon } from 'lucide-react';

interface SummaryCardProps {
  title: string;
  value: string | number;
  icon: LucideIcon;
  trend?: 'up' | 'down' | 'neutral';
  className?: string;
}

export function SummaryCard({ title, value, icon: Icon, trend, className }: SummaryCardProps) {
  return (
    <div className={cn('stat-card flex items-center density-gap group', className)}>
      <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 via-primary/10 to-accent/10 text-primary group-hover:from-primary/30 group-hover:to-accent/20 transition-all duration-300">
        <Icon className="h-6 w-6 shrink-0" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="data-label truncate leading-tight">{title}</p>
        <p className="text-2xl font-bold bg-gradient-to-r from-foreground to-foreground/80 bg-clip-text text-transparent truncate leading-tight">
          {value}
        </p>
      </div>
      {trend && (
        <div className={cn(
          'ml-auto text-sm font-semibold px-2 py-1 rounded-lg shrink-0 whitespace-nowrap',
          trend === 'up' && 'bg-gradient-to-r from-success/20 to-success/10 text-success',
          trend === 'down' && 'bg-gradient-to-r from-destructive/20 to-destructive/10 text-destructive',
          trend === 'neutral' && 'bg-gradient-to-r from-muted to-muted/50 text-muted-foreground'
        )}>
          {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'}
        </div>
      )}
    </div>
  );
}
