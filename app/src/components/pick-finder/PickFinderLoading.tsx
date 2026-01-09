import { useState, useEffect } from 'react';
import { Check, Loader2, TrendingUp, Users, Target, Sparkles, BarChart3, Trophy } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { PickFinderFilters, LoadingStage } from '@/types/pickFinder';

interface PickFinderLoadingProps {
  filters: PickFinderFilters;
  currentStage?: string;
  progress?: number;
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

export function PickFinderLoading({ filters, currentStage = '', progress = 0 }: PickFinderLoadingProps) {
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
    <div className="flex min-h-[80vh] items-center justify-center p-8 bg-gradient-to-b from-zinc-950 to-zinc-900">
      <div className="w-full max-w-2xl space-y-8">
        <div className="text-center space-y-4">
          <div className="flex justify-center">
            <div className="relative">
              <div className="absolute inset-0 rounded-full bg-blue-500/20 animate-ping" />
              <div className="relative rounded-full bg-gradient-to-br from-blue-500 to-blue-600 p-5 shadow-lg shadow-blue-500/50">
                <Loader2 className="h-10 w-10 text-white animate-spin" />
              </div>
            </div>
          </div>
          <div>
            <h2 className="text-3xl font-bold text-white mb-2 bg-gradient-to-r from-blue-400 to-blue-600 bg-clip-text text-transparent">
              Finding Your Best Picks
            </h2>
            <p className="text-zinc-400">
              Analyzing <span className="text-white font-medium">{filters.statType === 'all' ? 'all stats' : filters.statType}</span>
              {' '}with <span className="text-white font-medium">{activeFiltersCount} active filter{activeFiltersCount !== 1 ? 's' : ''}</span>
            </p>
          </div>
        </div>

        <div className="space-y-3">
          <div className="h-2 bg-zinc-800 rounded-full overflow-hidden border border-zinc-700">
            <div
              className="h-full bg-gradient-to-r from-blue-500 via-blue-600 to-blue-500 transition-all duration-500 ease-out relative"
              style={{ width: `${Math.min(progress, 100)}%` }}
            >
              <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/30 to-transparent animate-shimmer" />
            </div>
          </div>
          <div className="flex justify-between text-sm">
            <span className="text-zinc-500">Progress</span>
            <span className="text-blue-400 font-medium">{Math.min(Math.round(progress), 100)}%</span>
          </div>
        </div>

        <div className="space-y-2 bg-zinc-900/50 rounded-xl border border-zinc-800 p-6 backdrop-blur-sm">
          {currentStages.map((stage, index) => {
            const Icon = stageIcons[index];
            const isActive = stage.status === 'in-progress';
            const isCompleted = stage.status === 'completed';

            return (
              <div
                key={stage.id}
                className={cn(
                  'flex items-center gap-4 p-3 rounded-lg transition-all duration-300',
                  isActive && 'bg-blue-500/10 scale-105',
                  isCompleted && 'opacity-60'
                )}
              >
                <div
                  className={cn(
                    'flex h-10 w-10 items-center justify-center rounded-full transition-all duration-500 flex-shrink-0',
                    isCompleted && 'bg-blue-500 text-white shadow-lg shadow-blue-500/50 scale-110',
                    isActive && 'bg-blue-500/20 text-blue-400 animate-pulse',
                    stage.status === 'pending' && 'bg-zinc-800 text-zinc-600'
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
                      isCompleted && 'text-zinc-500',
                      isActive && 'text-white',
                      stage.status === 'pending' && 'text-zinc-600'
                    )}
                  >
                    {stage.label}
                  </span>
                  {isActive && (
                    <div className="flex gap-1 mt-1">
                      <div className="h-1 w-1 rounded-full bg-blue-400 animate-bounce [animation-delay:0ms]" />
                      <div className="h-1 w-1 rounded-full bg-blue-400 animate-bounce [animation-delay:150ms]" />
                      <div className="h-1 w-1 rounded-full bg-blue-400 animate-bounce [animation-delay:300ms]" />
                    </div>
                  )}
                </div>
                {isCompleted && (
                  <div className="text-xs text-zinc-500 font-medium">✓ Done</div>
                )}
              </div>
            );
          })}
        </div>

        <div className="grid grid-cols-3 gap-4">
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4 text-center backdrop-blur-sm">
            <div className="text-xs text-zinc-500 mb-1">Time Window</div>
            <div className="text-xl font-bold text-white">{filters.timeWindow}</div>
            <div className="text-xs text-zinc-600">games</div>
          </div>
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4 text-center backdrop-blur-sm">
            <div className="text-xs text-zinc-500 mb-1">Line Method</div>
            <div className="text-sm font-bold text-white">
              {filters.lineMethod === 'player-average' ? 'Player Avg' : 'AI Prediction'}
            </div>
            <div className="text-xs text-zinc-600 mt-1">
              {filters.lineAdjustment === 'standard' ? 'Standard' :
               filters.lineAdjustment === 'favorable' ? 'Favorable' : 'Custom'}
            </div>
          </div>
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4 text-center backdrop-blur-sm">
            <div className="text-xs text-zinc-500 mb-1">Filters Active</div>
            <div className="text-xl font-bold text-blue-400">{activeFiltersCount}</div>
            <div className="text-xs text-zinc-600 mt-1">
              {filters.overUnder === 'over' ? 'Over' : filters.overUnder === 'under' ? 'Under' : 'Both'}
            </div>
          </div>
        </div>

        <div className="text-center">
          <p className="text-xs text-zinc-600">
            Tip: Stricter filters = fewer picks but higher confidence
          </p>
        </div>
      </div>

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
