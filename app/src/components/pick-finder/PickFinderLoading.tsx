import { useState, useEffect } from 'react';
import { Check, Loader2, TrendingUp, Users, Target, Sparkles, BarChart3, Trophy, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import type { PickFinderFilters, LoadingStage } from '@/types/pickFinder';

interface PickFinderLoadingProps {
  filters: PickFinderFilters;
  currentStage?: string;
  progress?: number;
  onCancel?: () => void;
}

const stages: LoadingStage[] = [
  { id: 'games', label: 'Loading today\'s games & matchups', status: 'pending' },
  { id: 'players', label: 'Fetching active players & season stats', status: 'pending' },
  { id: 'predictions', label: 'Getting AI predictions & confidence scores', status: 'pending' },
  { id: 'history', label: 'Analyzing historical performance data', status: 'pending' },
  { id: 'filtering', label: 'Applying your filter criteria', status: 'pending' },
  { id: 'scoring', label: 'Finalizing strength scores & preparing results...', status: 'pending' },
];

const stageIcons = [
  Target,
  Users,
  Sparkles,
  TrendingUp,
  BarChart3,
  Trophy,
];

export function PickFinderLoading({ filters, currentStage = '', progress = 0, onCancel }: PickFinderLoadingProps) {
  const [currentStages, setCurrentStages] = useState<LoadingStage[]>(stages);
  const [currentStageIndex, setCurrentStageIndex] = useState(-1);

  useEffect(() => {
    
    if (!currentStage) {
      
      setCurrentStages(stages);
      setCurrentStageIndex(-1);
      return;
    }

    
    const stageIndex = stages.findIndex(s => s.id === currentStage);
    if (stageIndex === -1) return;

    setCurrentStageIndex(stageIndex);
    setCurrentStages(prev =>
      prev.map((stage, index) => {
        if (index < stageIndex) {
          return { ...stage, status: 'completed' };
        } else if (index === stageIndex) {
          return { ...stage, status: 'in-progress' };
        }
        return stage;
      })
    );
  }, [currentStage]);

  
  const activeFiltersCount = [
    filters.enableHitRateThreshold,
    filters.enableConsecutiveHits,
    filters.enableContextSplit,
    filters.enableH2H,
    filters.enablePositionDefense,
    filters.enableTeamDefense,
    filters.enablePace,
    filters.enableMinConfidence,
    filters.enableMinMinutes,
    filters.aiAgreement !== 'disabled',
  ].filter(Boolean).length;

  return (
    <div className="flex flex-col min-h-full p-6 sm:p-8 bg-gradient-to-b from-background to-background/90 overflow-y-auto">
      <div className="flex-1 flex items-center justify-center">
        <div className="w-full max-w-2xl space-y-6">
          <div className="text-center space-y-4">
            <div className="flex justify-center">
              <div className="relative">
                <div className="absolute inset-0 rounded-full bg-primary/20 animate-ping" />
                <div className="relative rounded-full bg-gradient-to-br from-primary to-primary/80 p-5 shadow-lg shadow-primary/50">
                  <Loader2 className="h-10 w-10 text-foreground animate-spin" />
                </div>
              </div>
            </div>
            <div>
              <h2 className="text-2xl sm:text-3xl font-bold text-foreground mb-2 bg-gradient-to-r from-primary/80 to-primary bg-clip-text text-transparent">
                Finding Your Best Picks
              </h2>
              <p className="text-muted-foreground text-sm sm:text-base">
                Analyzing <span className="text-foreground font-medium">{filters.statType === 'all' ? 'all stats' : filters.statType}</span>
                {' '}with <span className="text-foreground font-medium">{activeFiltersCount} active filter{activeFiltersCount !== 1 ? 's' : ''}</span>
              </p>
            </div>
          </div>

          <div className="space-y-3">
            <div className="h-2 bg-secondary rounded-full overflow-hidden border border-border/80">
              <div
                className="h-full bg-gradient-to-r from-primary via-primary/80 to-primary transition-all duration-500 ease-out relative"
                style={{ width: `${Math.min(progress, 100)}%` }}
              >
                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/30 to-transparent animate-shimmer" />
              </div>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground/80">Progress</span>
              <span className="text-primary font-medium">{Math.min(Math.round(progress), 100)}%</span>
            </div>
          </div>

          <div className="space-y-2 bg-card/50 rounded-xl border border-border/60 p-6 backdrop-blur-sm">
            {currentStages.map((stage, index) => {
              const Icon = stageIcons[index];
              const isActive = stage.status === 'in-progress';
              const isCompleted = stage.status === 'completed';

              return (
                <div
                  key={stage.id}
                  className={cn(
                    'flex items-center gap-4 p-3 rounded-lg transition-all duration-300',
                    isActive && 'bg-primary/10 scale-105',
                    isCompleted && 'opacity-60'
                  )}
                >
                  <div
                    className={cn(
                      'flex h-10 w-10 items-center justify-center rounded-full transition-all duration-500 flex-shrink-0',
                      isCompleted && 'bg-primary text-foreground shadow-lg shadow-primary/50 scale-110',
                      isActive && 'bg-primary/20 text-primary animate-pulse',
                      stage.status === 'pending' && 'bg-secondary text-muted-foreground/60'
                    )}
                  >
                    {isCompleted ? (
                      <Check className="h-5 w-5 animate-in zoom-in-50 duration-300" />
                    ) : isActive ? (
                      <Icon className="h-5 w-5 animate-pulse" />
                    ) : (
                      <Icon className="h-5 w-5" />
                    )}
                  </div>
                  <div className="flex-1">
                    <span
                      className={cn(
                        'text-sm font-medium transition-colors duration-300',
                        isCompleted && 'text-muted-foreground/80',
                        isActive && 'text-foreground',
                        stage.status === 'pending' && 'text-muted-foreground/60'
                      )}
                    >
                      {stage.label}
                    </span>
                    {isActive && (
                      <div className="flex gap-1 mt-1">
                        <div className="h-1 w-1 rounded-full bg-primary animate-bounce [animation-delay:0ms]" />
                        <div className="h-1 w-1 rounded-full bg-primary animate-bounce [animation-delay:150ms]" />
                        <div className="h-1 w-1 rounded-full bg-primary animate-bounce [animation-delay:300ms]" />
                      </div>
                    )}
                  </div>
                  {isCompleted && (
                    <div className="text-xs text-muted-foreground/80 font-medium">✓ Done</div>
                  )}
                </div>
              );
            })}
          </div>

          <div className="grid grid-cols-3 gap-4">
            <div className="bg-card/50 border border-border/60 rounded-lg p-4 text-center backdrop-blur-sm">
              <div className="text-xs text-muted-foreground/80 mb-1">Time Window</div>
              <div className="text-xl font-bold text-foreground">{filters.timeWindow}</div>
              <div className="text-xs text-muted-foreground/60">games</div>
            </div>
            <div className="bg-card/50 border border-border/60 rounded-lg p-4 text-center backdrop-blur-sm">
              <div className="text-xs text-muted-foreground/80 mb-1">Line Method</div>
              <div className="text-sm font-bold text-foreground">
                {filters.lineMethod === 'player-average' ? 'Player Avg' : 'AI Prediction'}
              </div>
              <div className="text-xs text-muted-foreground/60 mt-1">
                {filters.lineAdjustment === 'standard' ? 'Standard' :
                 filters.lineAdjustment === 'favorable' ? 'Favorable' : 'Custom'}
              </div>
            </div>
            <div className="bg-card/50 border border-border/60 rounded-lg p-4 text-center backdrop-blur-sm">
              <div className="text-xs text-muted-foreground/80 mb-1">Filters Active</div>
              <div className="text-xl font-bold text-primary">{activeFiltersCount}</div>
              <div className="text-xs text-muted-foreground/60 mt-1">
                {filters.overUnder === 'over' ? 'Over' : filters.overUnder === 'under' ? 'Under' : 'Both'}
              </div>
            </div>
          </div>

          <p className="text-xs text-muted-foreground/60 text-center">
            Tip: Stricter filters = fewer picks but higher confidence
          </p>
        </div>
      </div>

      {/* Cancel button - always visible at bottom */}
      {onCancel && (
        <div className="flex justify-center pt-4 pb-2">
          <Button onClick={onCancel} variant="outline" size="lg" className="gap-2">
            <X className="h-4 w-4" />
            Cancel
          </Button>
        </div>
      )}

      <style>{`
        @keyframes shimmer {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(100%);
          }
        }
        .animate-shimmer {
          animation: shimmer 2s infinite;
        }
      `}</style>
    </div>
  );
}
