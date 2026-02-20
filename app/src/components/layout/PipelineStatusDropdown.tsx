import { formatDistanceToNow, format } from 'date-fns';
import { motion } from 'framer-motion';
import { CheckCircle2, XCircle, Loader2, AlertCircle, ExternalLink, RefreshCw, GitBranch } from 'lucide-react';
import { usePipelineStatus, type PipelineRun } from '@/hooks/usePipelineStatus';
import { cn } from '@/lib/utils';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

function StatusIcon({ status, className }: { status: PipelineRun['status']; className?: string }) {
  switch (status) {
    case 'success':
      return <CheckCircle2 className={cn('text-success', className)} />;
    case 'failure':
      return <XCircle className={cn('text-destructive', className)} />;
    case 'in_progress':
      return <Loader2 className={cn('text-warning animate-spin', className)} />;
    case 'cancelled':
      return <AlertCircle className={cn('text-muted-foreground', className)} />;
    default:
      return <AlertCircle className={cn('text-muted-foreground', className)} />;
  }
}

function StatusDot({ status }: { status: PipelineRun['status'] }) {
  const colorClass = {
    success: 'bg-success',
    failure: 'bg-destructive',
    in_progress: 'bg-warning',
    cancelled: 'bg-muted-foreground',
    unknown: 'bg-muted-foreground',
  }[status];

  return (
    <span className="relative flex h-2 w-2">
      {status === 'in_progress' && (
        <span className={cn('absolute inline-flex h-full w-full animate-ping rounded-full opacity-75', colorClass)} />
      )}
      <span className={cn('relative inline-flex h-2 w-2 rounded-full', colorClass)} />
    </span>
  );
}

function formatTimestamp(date: Date): string {
  const now = new Date();
  const isToday = date.toDateString() === now.toDateString();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  const isYesterday = date.toDateString() === yesterday.toDateString();

  if (isToday) {
    return `Today ${format(date, 'h:mm a')}`;
  } else if (isYesterday) {
    return `Yesterday ${format(date, 'h:mm a')}`;
  } else {
    return format(date, 'MMM d h:mm a');
  }
}

function getStatusLabel(status: PipelineRun['status']): string {
  switch (status) {
    case 'success':
      return 'Success';
    case 'failure':
      return 'Failed';
    case 'in_progress':
      return 'Running';
    case 'cancelled':
      return 'Cancelled';
    default:
      return 'Unknown';
  }
}

export function PipelineStatusDropdown() {
  const { data: runs, isLoading, isError, refetch, isFetching } = usePipelineStatus();

  const latestRun = runs?.[0];

  const handleOpenInGitHub = (url: string) => {
    if (window.electron?.openExternal) {
      window.electron.openExternal(url);
    } else {
      window.open(url, '_blank');
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center gap-1.5 px-2 py-1 rounded-md text-muted-foreground">
        <Loader2 className="h-3 w-3 animate-spin" />
        <span className="text-xs">Loading...</span>
      </div>
    );
  }

  if (isError || !latestRun) {
    return (
      <button
        onClick={() => refetch()}
        className="flex items-center gap-1.5 px-2 py-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
        title="Failed to load pipeline status. Click to retry."
      >
        <AlertCircle className="h-3 w-3" />
        <span className="text-xs">Pipeline</span>
      </button>
    );
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <motion.button
          className="flex items-center gap-1.5 px-2 py-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-secondary/50 transition-colors"
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          title={`Pipeline: ${getStatusLabel(latestRun.status)}`}
        >
          <GitBranch className="h-3 w-3" />
          <StatusDot status={latestRun.status} />
          <span className="text-xs font-medium">
            {formatTimestamp(latestRun.timestamp)}
          </span>
        </motion.button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className="w-72 bg-card/95 backdrop-blur-md border-border/40 shadow-xl shadow-black/20"
        sideOffset={8}
      >
        <div className="flex items-center justify-between px-3 py-2 border-b border-border/30">
          <h3 className="text-sm font-semibold text-foreground">Pipeline Runs</h3>
          <motion.button
            onClick={() => refetch()}
            disabled={isFetching}
            className="text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
            title="Refresh"
            animate={isFetching ? { rotate: 360 } : { rotate: 0 }}
            transition={isFetching ? { duration: 1, repeat: Infinity, ease: "linear" } : { duration: 0.3 }}
          >
            <RefreshCw className="h-3.5 w-3.5" />
          </motion.button>
        </div>

        <div className="py-1 max-h-80 overflow-y-auto">
          {runs?.map((run, index) => (
            <DropdownMenuItem
              key={run.id}
              className="flex items-center gap-3 px-3 py-2.5 cursor-pointer"
              onClick={() => handleOpenInGitHub(run.url)}
            >
              <StatusIcon status={run.status} className="h-4 w-4 shrink-0" />
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium text-foreground truncate">
                    {formatTimestamp(run.timestamp)}
                  </span>
                  <span className={cn(
                    'text-xs font-medium shrink-0',
                    run.status === 'success' && 'text-success',
                    run.status === 'failure' && 'text-destructive',
                    run.status === 'in_progress' && 'text-warning',
                    run.status === 'cancelled' && 'text-muted-foreground'
                  )}>
                    {getStatusLabel(run.status)}
                  </span>
                </div>
                <p className="text-xs text-muted-foreground truncate mt-0.5">
                  {run.name}
                </p>
              </div>
              <ExternalLink className="h-3 w-3 text-muted-foreground shrink-0 opacity-0 group-hover:opacity-100" />
            </DropdownMenuItem>
          ))}
        </div>

        <DropdownMenuSeparator className="bg-border/50" />
        <DropdownMenuItem
          className="justify-center text-sm text-primary hover:text-primary/80 cursor-pointer py-2"
          onClick={() => handleOpenInGitHub(`https://github.com/kennethhuang7/CourtVision/actions`)}
        >
          View all runs
          <ExternalLink className="h-3 w-3 ml-1.5" />
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
