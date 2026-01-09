import { useState, useEffect } from 'react';
import { Search, ArrowLeft, X, Target, TrendingUp, Shield, Zap, Clock, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Slider } from '@/components/ui/slider';
import { Checkbox } from '@/components/ui/checkbox';
import { InfoTooltip } from '@/components/ui/InfoTooltip';
import { PickFinderLoading } from '@/components/pick-finder/PickFinderLoading';
import { PickResultCard } from '@/components/pick-finder/PickResultCard';
import { usePickFinder } from '@/hooks/usePickFinder';
import { useEnsemble } from '@/contexts/EnsembleContext';
import { logger } from '@/lib/logger';
import type { PickFinderFilters, PickResult } from '@/types/pickFinder';

type PageState = 'constructor' | 'loading' | 'results';

const statOptions = [
  { value: 'all', label: 'All Stats' },
  { value: 'points', label: 'Points' },
  { value: 'rebounds', label: 'Rebounds' },
  { value: 'assists', label: 'Assists' },
  { value: 'steals', label: 'Steals' },
  { value: 'blocks', label: 'Blocks' },
  { value: 'turnovers', label: 'Turnovers' },
  { value: 'threePointersMade', label: '3-Pointers Made' },
] as const;

const defaultFilters: PickFinderFilters = {
  
  statType: 'all',
  overUnder: 'over',
  lineMethod: 'player-average',
  lineAdjustment: 'standard',

  
  customModifiers: {
    points: 2.0,
    rebounds: 1.0,
    assists: 1.0,
    steals: 0.5,
    blocks: 0.5,
    turnovers: 0.5,
    threePointersMade: 0.5,
  },

  
  timeWindow: 5,
  enableHitRateThreshold: false,
  hitRateMode: 'percentage',
  hitRateThreshold: 60,
  hitRateCount: 3,
  enableConsecutiveHits: false,
  consecutiveHits: 0,

  
  enableContextSplit: false,
  contextTimeWindow: 5,
  enableContextHitRate: false,
  contextHitRateMode: 'percentage',
  contextHitRateThreshold: 60,
  contextHitRateCount: 3,
  enableContextConsecutiveHits: false,
  contextConsecutiveHits: 0,

  
  enableH2H: false,
  h2hTimeWindow: 10,
  enableH2hHitRate: false,
  h2hHitRateMode: 'percentage',
  h2hHitRateThreshold: 60,
  h2hHitRateCount: 6,
  enableH2hConsecutiveHits: false,
  h2hConsecutiveHits: 0,

  
  separatePlayoffStats: true,

  
  enablePositionDefense: false, 
  positionDefenseRank: 10,
  enableTeamDefense: false,
  teamDefenseRank: 10,
  enablePace: false, 
  paceRequirement: 'any',

  
  excludeTiredVsRested: false,

  
  aiAgreement: 'disabled', 
  enableMinConfidence: false, 
  minConfidence: 60,

  
  enableMinMinutes: false, 
  minMinutes: 25,
  minMinutesWindow: 5,
  playerRole: 'any',
};

export default function PickFinder() {
  const [page, setPage] = useState<PageState>('constructor');
  const [filters, setFilters] = useState<PickFinderFilters>(() => {
    
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem('courtvision-pick-finder-filters');
      if (stored) {
        try {
          const parsed = JSON.parse(stored);
          
          return {
            ...defaultFilters,
            ...parsed,
            
            customModifiers: parsed.customModifiers ?? defaultFilters.customModifiers,
            hitRateMode: parsed.hitRateMode ?? defaultFilters.hitRateMode,
            hitRateCount: parsed.hitRateCount ?? defaultFilters.hitRateCount,
            contextTimeWindow: parsed.contextTimeWindow ?? defaultFilters.contextTimeWindow,
            enableContextHitRate: parsed.enableContextHitRate ?? defaultFilters.enableContextHitRate,
            contextHitRateMode: parsed.contextHitRateMode ?? defaultFilters.contextHitRateMode,
            contextHitRateThreshold: parsed.contextHitRateThreshold ?? parsed.contextSplitThreshold ?? defaultFilters.contextHitRateThreshold,
            contextHitRateCount: parsed.contextHitRateCount ?? defaultFilters.contextHitRateCount,
            contextConsecutiveHits: parsed.contextConsecutiveHits ?? defaultFilters.contextConsecutiveHits,
            enableContextConsecutiveHits: parsed.enableContextConsecutiveHits ?? defaultFilters.enableContextConsecutiveHits,
            h2hTimeWindow: parsed.h2hTimeWindow ?? defaultFilters.h2hTimeWindow,
            enableH2hHitRate: parsed.enableH2hHitRate ?? defaultFilters.enableH2hHitRate,
            h2hHitRateMode: parsed.h2hHitRateMode ?? defaultFilters.h2hHitRateMode,
            h2hHitRateCount: parsed.h2hHitRateCount ?? defaultFilters.h2hHitRateCount,
            h2hConsecutiveHits: parsed.h2hConsecutiveHits ?? defaultFilters.h2hConsecutiveHits,
            enableH2hConsecutiveHits: parsed.enableH2hConsecutiveHits ?? defaultFilters.enableH2hConsecutiveHits,
          };
        } catch (e) {
          logger.error('Error loading Pick Finder filters from localStorage', e as Error);
        }
      }
    }
    return defaultFilters;
  });
  const [results, setResults] = useState<PickResult[]>([]);
  const [expandedSections, setExpandedSections] = useState({
    performance: true,
    matchup: true,
    ai: true,
    rest: true,
    reliability: true,
  });
  const [performanceTab, setPerformanceTab] = useState<'overall' | 'homeaway' | 'h2h'>('overall');

  
  useEffect(() => {
    if (typeof window !== 'undefined') {
      localStorage.setItem('courtvision-pick-finder-filters', JSON.stringify(filters));
    }
  }, [filters]);

  const updateFilter = <K extends keyof PickFinderFilters>(
    key: K,
    value: PickFinderFilters[K]
  ) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const toggleSection = (section: keyof typeof expandedSections) => {
    setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }));
  };

  const { selectedModels } = useEnsemble();
  const { findPicks } = usePickFinder();
  const [loadingStage, setLoadingStage] = useState<string>('');
  const [loadingProgress, setLoadingProgress] = useState<number>(0);

  const handleSearch = async () => {
    setPage('loading');
    setLoadingStage('');
    setLoadingProgress(0);

    try {
      const picks = await findPicks(filters, selectedModels, (stage, progress) => {
        setLoadingStage(stage);
        setLoadingProgress(progress);
      });
      setResults(picks);
      setPage('results');
    } catch (error) {
      setResults([]);
      setPage('constructor');
    }
  };

  const handleCancel = () => {
    setPage('constructor');
  };

  const handleBackToConstructor = () => {
    setPage('constructor');
  };

  const handleReset = () => {
    setFilters(defaultFilters);
    
    if (typeof window !== 'undefined') {
      localStorage.removeItem('courtvision-pick-finder-filters');
    }
  };

  
  if (page === 'loading') {
    return (
      <div className="flex h-full flex-col bg-background">
        <PickFinderLoading filters={filters} currentStage={loadingStage} progress={loadingProgress} />
        <div className="absolute bottom-8 left-1/2 -translate-x-1/2">
          <Button onClick={handleCancel} variant="outline" size="lg">
            <X className="mr-2 h-4 w-4 shrink-0" />
            <span className="whitespace-nowrap">Cancel</span>
          </Button>
        </div>
      </div>
    );
  }

  
  if (page === 'results') {
    return (
      <div className="flex h-full flex-col bg-background">
        <div className="border-b border-border bg-card px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Button onClick={handleBackToConstructor} variant="ghost" size="sm">
                <ArrowLeft className="mr-2 h-4 w-4 shrink-0" />
                <span className="whitespace-nowrap">Back to Filters</span>
              </Button>
              <div className="h-6 w-px bg-border" />
              <div>
                <h1 className="text-xl font-bold text-foreground">
                  Found {results.length} {results.length === 1 ? 'Pick' : 'Picks'}
                </h1>
                <p className="text-sm text-muted-foreground">
                  {filters.statType} {filters.overUnder}s • L{filters.timeWindow} window • Sorted by strength
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6">
          {results.length > 0 ? (
            <div className="max-w-5xl mx-auto space-y-3">
              {results.map((result, index) => (
                <PickResultCard key={`${result.playerId}-${result.gameId}-${index}`} result={result} />
              ))}
            </div>
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-center space-y-3">
                <div className="flex justify-center">
                  <div className="rounded-full bg-primary/10 p-6">
                    <Target className="h-12 w-12 text-primary shrink-0" />
                  </div>
                </div>
                <h3 className="text-lg font-medium text-foreground leading-tight">No picks found</h3>
                <p className="text-sm text-muted-foreground max-w-sm leading-tight">
                  No picks matched your criteria. Try adjusting your filters.
                </p>
                <Button onClick={handleBackToConstructor} variant="outline">
                  <span className="whitespace-nowrap">Back to Filters</span>
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }

  
  const TabButton = ({ id, label, enabled, isActive, onClick, showIndicator = false }: {
    id: string;
    label: string;
    enabled: boolean;
    isActive: boolean;
    onClick: () => void;
    showIndicator?: boolean;
  }) => (
    <button
      onClick={onClick}
      className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors relative ${
        isActive ? 'text-blue-400' : 'text-zinc-400 hover:text-white'
      }`}
    >
      <span className="flex items-center justify-center gap-2">
        {label}
        {showIndicator && (
          enabled ? (
            <span className="bg-blue-500/20 text-blue-400 text-xs px-1.5 py-0.5 rounded font-medium">ON</span>
          ) : (
            <span className="text-zinc-500 text-xs px-1.5 py-0.5 rounded bg-zinc-800">OFF</span>
          )
        )}
      </span>
      {isActive && (
        <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-500" />
      )}
    </button>
  );

  const HitRateControls = ({
    enabled,
    onEnabledChange,
    mode,
    onModeChange,
    threshold,
    onThresholdChange,
    count,
    onCountChange,
    timeWindow,
    enableConsecutive,
    onEnableConsecutiveChange,
    consecutiveHits,
    onConsecutiveHitsChange,
  }: {
    enabled: boolean;
    onEnabledChange: (value: boolean) => void;
    mode: 'percentage' | 'count';
    onModeChange: (value: 'percentage' | 'count') => void;
    threshold: number;
    onThresholdChange: (value: number) => void;
    count: number;
    onCountChange: (value: number) => void;
    timeWindow: number;
    enableConsecutive: boolean;
    onEnableConsecutiveChange: (value: boolean) => void;
    consecutiveHits: number;
    onConsecutiveHitsChange: (value: number) => void;
  }) => (
    <div className="space-y-4">
      <label className="flex items-center gap-2 cursor-pointer group">
        <div className="relative">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => onEnabledChange(e.target.checked)}
            className="sr-only"
          />
          <div className={`w-10 h-6 rounded-full transition-colors ${enabled ? 'bg-blue-500' : 'bg-zinc-700'}`}>
            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${enabled ? 'translate-x-5' : 'translate-x-1'}`} />
          </div>
        </div>
        <span className="text-white text-sm">Minimum hit rate</span>
        <InfoTooltip content="Percentage of games where player hit this line." />
      </label>

      {enabled && (
        <div className="ml-13 space-y-4">
          <div>
            <label className="block text-zinc-400 text-sm mb-2">Mode</label>
            <Select value={mode} onValueChange={(v) => onModeChange(v as 'percentage' | 'count')}>
              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="percentage">Percentage</SelectItem>
                <SelectItem value="count">Count</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {mode === 'percentage' ? (
            <div>
              <div className="flex justify-between items-center mb-2">
                <label className="text-zinc-400 text-sm">Threshold</label>
                <span className="text-white text-sm font-medium">{threshold}%</span>
              </div>
              <input
                type="range"
                min={40}
                max={100}
                value={threshold}
                onChange={(e) => onThresholdChange(Number(e.target.value))}
                className="w-full h-2 bg-zinc-700 rounded-lg appearance-none cursor-pointer accent-blue-500"
              />
              <div className="flex justify-between text-xs text-zinc-500 mt-1">
                <span>40%</span>
                <span>100%</span>
              </div>
            </div>
          ) : (
            <div>
              <label className="block text-zinc-400 text-sm mb-2">Minimum Hits</label>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={timeWindow}
                  value={count}
                  onChange={(e) => {
                    const val = Math.min(timeWindow, Math.max(0, Number(e.target.value)));
                    onCountChange(val);
                  }}
                  className="flex-1 bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white"
                />
                <span className="text-zinc-400 text-sm">out of {timeWindow}</span>
              </div>
            </div>
          )}
        </div>
      )}

      <label className="flex items-center gap-2 cursor-pointer group">
        <div className="relative">
          <input
            type="checkbox"
            checked={enableConsecutive}
            onChange={(e) => {
              onEnableConsecutiveChange(e.target.checked);
              if (!e.target.checked) onConsecutiveHitsChange(0);
            }}
            className="sr-only"
          />
          <div className={`w-10 h-6 rounded-full transition-colors ${enableConsecutive ? 'bg-blue-500' : 'bg-zinc-700'}`}>
            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${enableConsecutive ? 'translate-x-5' : 'translate-x-1'}`} />
          </div>
        </div>
        <span className="text-white text-sm">Consecutive hits</span>
        <InfoTooltip content="Require N consecutive hits in recent games." />
      </label>

      {enableConsecutive && (
        <div className="ml-13">
          <label className="block text-zinc-400 text-sm mb-2">Consecutive</label>
          <input
            type="number"
            min={1}
            max={timeWindow}
            value={consecutiveHits}
            onChange={(e) => {
              const val = Number(e.target.value);
              if (isNaN(val) || val < 1) onConsecutiveHitsChange(1);
              else if (val > timeWindow) onConsecutiveHitsChange(timeWindow);
              else onConsecutiveHitsChange(val);
            }}
            className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-blue-500"
          />
        </div>
      )}
    </div>
  );

  
  return (
    <div className="min-h-screen bg-zinc-950 text-white p-6">
      <div className="max-w-6xl">
        <div className="mb-8 min-w-0">
          <h1 className="text-2xl font-bold mb-1 leading-tight truncate">Pick Finder</h1>
          <p className="text-zinc-500 leading-tight truncate">Configure filters to find high-confidence picks</p>
        </div>

        <div className="space-y-6">
          <div className="border border-zinc-800 rounded-xl p-5 bg-zinc-900/50">
            <div className="grid grid-cols-4 gap-4">
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <label className="block text-zinc-400 text-sm">Stat Type</label>
                  <InfoTooltip content="Choose which player stat to analyze. Select 'All Stats' to find picks across all categories." />
                </div>
                <Select value={filters.statType} onValueChange={(v) => updateFilter('statType', v as any)}>
                  <SelectTrigger className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white h-11">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {statOptions.map(opt => (
                      <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <div className="flex items-center gap-2 mb-2">
                  <label className="block text-zinc-400 text-sm">Direction</label>
                  <InfoTooltip content="Choose whether to find Over picks, Under picks, or both." />
                </div>
                <div className="flex gap-1 bg-zinc-800 p-1 rounded-lg">
                  <button
                    onClick={() => updateFilter('overUnder', 'over')}
                    className={`flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      filters.overUnder === 'over'
                        ? 'bg-blue-500 text-white'
                        : 'text-zinc-400 hover:text-white hover:bg-zinc-700'
                    }`}
                  >
                    Over
                  </button>
                  <button
                    onClick={() => updateFilter('overUnder', 'both')}
                    className={`flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      filters.overUnder === 'both'
                        ? 'bg-blue-500 text-white'
                        : 'text-zinc-400 hover:text-white hover:bg-zinc-700'
                    }`}
                  >
                    Both
                  </button>
                  <button
                    onClick={() => updateFilter('overUnder', 'under')}
                    className={`flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      filters.overUnder === 'under'
                        ? 'bg-blue-500 text-white'
                        : 'text-zinc-400 hover:text-white hover:bg-zinc-700'
                    }`}
                  >
                    Under
                  </button>
                </div>
              </div>

              <div>
                <div className="flex items-center gap-2 mb-2">
                  <label className="block text-zinc-400 text-sm">Line Method</label>
                  <InfoTooltip content="Player Average uses the player's season average. AI Prediction uses machine learning to predict their performance." />
                </div>
                <Select value={filters.lineMethod} onValueChange={(v) => updateFilter('lineMethod', v as any)}>
                  <SelectTrigger className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white h-11">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="player-average">Player Average</SelectItem>
                    <SelectItem value="ai-prediction">AI Prediction</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div>
                <div className="flex items-center gap-2 mb-2">
                  <label className="block text-zinc-400 text-sm">Line Adjustment</label>
                  <InfoTooltip content="Standard: use the line as-is. Favorable: adjust the line to make picks easier to hit. Custom: set your own adjustments per stat." />
                </div>
                <Select value={filters.lineAdjustment} onValueChange={(v) => updateFilter('lineAdjustment', v as any)}>
                  <SelectTrigger className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white h-11">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="standard">Standard</SelectItem>
                    <SelectItem value="favorable">Favorable</SelectItem>
                    <SelectItem value="custom">Custom</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className={`overflow-hidden transition-all duration-300 ease-in-out ${
              filters.lineAdjustment === 'custom' ? 'max-h-[500px] opacity-100 mt-4' : 'max-h-0 opacity-0'
            }`}>
              <div className="pt-4 border-t border-zinc-800">
                <p className="text-zinc-400 text-xs mb-3">Custom line modifiers by stat type</p>
                <div className="grid grid-cols-7 gap-3">
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Points</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.points}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, points: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Rebounds</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.rebounds}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, rebounds: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Assists</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.assists}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, assists: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Steals</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.steals}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, steals: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Blocks</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.blocks}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, blocks: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">Turnovers</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.turnovers}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, turnovers: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-zinc-400 text-xs mb-1">3PM</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="10"
                      value={filters.customModifiers.threePointersMade}
                      onChange={(e) => updateFilter('customModifiers', { ...filters.customModifiers, threePointersMade: Number(e.target.value) })}
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="grid lg:grid-cols-2 gap-3">
            <div className="space-y-3">
              <div className="border border-zinc-800 rounded-xl overflow-hidden bg-zinc-900/50">
                <div className="px-5 py-4 border-b border-zinc-800">
                  <h3 className="text-white font-medium">Recent Performance</h3>
                  <p className="text-zinc-500 text-sm">Historical hit rates and trends</p>
                </div>

                <div className="flex border-b border-zinc-800">
                  <TabButton
                    id="overall"
                    label="Overall"
                    enabled={true}
                    isActive={performanceTab === 'overall'}
                    onClick={() => setPerformanceTab('overall')}
                    showIndicator={false}
                  />
                  <TabButton
                    id="homeaway"
                    label="Home/Away"
                    enabled={filters.enableContextSplit}
                    isActive={performanceTab === 'homeaway'}
                    onClick={() => setPerformanceTab('homeaway')}
                    showIndicator={true}
                  />
                  <TabButton
                    id="h2h"
                    label="H2H"
                    enabled={filters.enableH2H}
                    isActive={performanceTab === 'h2h'}
                    onClick={() => setPerformanceTab('h2h')}
                    showIndicator={true}
                  />
                </div>

                <div className="p-5" style={{ minHeight: '340px' }}>
                  {performanceTab === 'overall' && (
                    <div className="space-y-4">
                        <div>
                          <div className="flex items-center gap-2 mb-2">
                            <label className="block text-zinc-400 text-sm">Time Window</label>
                            <InfoTooltip content="Number of recent games to analyze for hit rate and trends." />
                          </div>
                          <Select value={filters.timeWindow.toString()} onValueChange={(v) => {
                            const newWindow = parseInt(v) as 3 | 5 | 10 | 15 | 20;
                            updateFilter('timeWindow', newWindow);
                            if (filters.consecutiveHits > newWindow) {
                              updateFilter('consecutiveHits', 0);
                            }
                          }}>
                            <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="3">Last 3</SelectItem>
                              <SelectItem value="5">Last 5</SelectItem>
                              <SelectItem value="10">Last 10</SelectItem>
                              <SelectItem value="15">Last 15</SelectItem>
                              <SelectItem value="20">Last 20</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>

                        <HitRateControls
                          enabled={filters.enableHitRateThreshold}
                          onEnabledChange={(v) => updateFilter('enableHitRateThreshold', v)}
                          mode={filters.hitRateMode}
                          onModeChange={(v) => updateFilter('hitRateMode', v)}
                          threshold={filters.hitRateThreshold}
                          onThresholdChange={(v) => updateFilter('hitRateThreshold', v)}
                          count={filters.hitRateCount}
                          onCountChange={(v) => updateFilter('hitRateCount', v)}
                          timeWindow={filters.timeWindow}
                          enableConsecutive={filters.enableConsecutiveHits}
                          onEnableConsecutiveChange={(v) => updateFilter('enableConsecutiveHits', v)}
                          consecutiveHits={filters.consecutiveHits}
                          onConsecutiveHitsChange={(v) => updateFilter('consecutiveHits', v)}
                        />
                      </div>
                  )}

                  {performanceTab === 'homeaway' && (
                    <div className="space-y-4">
                      <label className="flex items-center gap-2 cursor-pointer group">
                        <div className="relative">
                          <input
                            type="checkbox"
                            checked={filters.enableContextSplit}
                            onChange={(e) => updateFilter('enableContextSplit', e.target.checked)}
                            className="sr-only"
                          />
                          <div className={`w-10 h-6 rounded-full transition-colors ${filters.enableContextSplit ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enableContextSplit ? 'translate-x-5' : 'translate-x-1'}`} />
                          </div>
                        </div>
                        <span className="text-white text-sm font-medium">Enable Home/Away Split</span>
                        <InfoTooltip content="Checks last X home games if playing at home today, or last X away games if playing away." />
                      </label>

                      {filters.enableContextSplit && (
                        <div className="ml-13 space-y-4">
                          <div>
                            <label className="block text-zinc-400 text-sm mb-2">Time Window</label>
                            <Select value={filters.contextTimeWindow.toString()} onValueChange={(v) => {
                              const newWindow = parseInt(v) as 3 | 5 | 10 | 15 | 20;
                              updateFilter('contextTimeWindow', newWindow);
                              if (filters.contextConsecutiveHits > newWindow) {
                                updateFilter('contextConsecutiveHits', 0);
                              }
                            }}>
                              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="3">Last 3</SelectItem>
                                <SelectItem value="5">Last 5</SelectItem>
                                <SelectItem value="10">Last 10</SelectItem>
                                <SelectItem value="15">Last 15</SelectItem>
                                <SelectItem value="20">Last 20</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>

                          <HitRateControls
                            enabled={filters.enableContextHitRate}
                            onEnabledChange={(v) => updateFilter('enableContextHitRate', v)}
                            mode={filters.contextHitRateMode}
                            onModeChange={(v) => updateFilter('contextHitRateMode', v)}
                            threshold={filters.contextHitRateThreshold}
                            onThresholdChange={(v) => updateFilter('contextHitRateThreshold', v)}
                            count={filters.contextHitRateCount}
                            onCountChange={(v) => updateFilter('contextHitRateCount', v)}
                            timeWindow={filters.contextTimeWindow}
                            enableConsecutive={filters.enableContextConsecutiveHits}
                            onEnableConsecutiveChange={(v) => updateFilter('enableContextConsecutiveHits', v)}
                            consecutiveHits={filters.contextConsecutiveHits}
                            onConsecutiveHitsChange={(v) => updateFilter('contextConsecutiveHits', v)}
                          />
                        </div>
                      )}
                    </div>
                  )}

                  {performanceTab === 'h2h' && (
                      <div className="space-y-4">
                      <label className="flex items-center gap-2 cursor-pointer group">
                        <div className="relative">
                          <input
                            type="checkbox"
                            checked={filters.enableH2H}
                            onChange={(e) => updateFilter('enableH2H', e.target.checked)}
                            className="sr-only"
                          />
                          <div className={`w-10 h-6 rounded-full transition-colors ${filters.enableH2H ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enableH2H ? 'translate-x-5' : 'translate-x-1'}`} />
                          </div>
                        </div>
                        <span className="text-white text-sm font-medium">Enable Head-to-Head</span>
                        <InfoTooltip content="Analyzes last X games vs today's specific opponent only." />
                      </label>

                      {filters.enableH2H && (
                        <div className="ml-13 space-y-4">
                          <div>
                            <label className="block text-zinc-400 text-sm mb-2">Time Window</label>
                            <Select value={filters.h2hTimeWindow.toString()} onValueChange={(v) => {
                              const newWindow = parseInt(v) as 3 | 5 | 10 | 15 | 20;
                              updateFilter('h2hTimeWindow', newWindow);
                              if (filters.h2hConsecutiveHits > newWindow) {
                                updateFilter('h2hConsecutiveHits', 0);
                              }
                            }}>
                              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="3">Last 3</SelectItem>
                                <SelectItem value="5">Last 5</SelectItem>
                                <SelectItem value="10">Last 10</SelectItem>
                                <SelectItem value="15">Last 15</SelectItem>
                                <SelectItem value="20">Last 20</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>

                          <HitRateControls
                            enabled={filters.enableH2hHitRate}
                            onEnabledChange={(v) => updateFilter('enableH2hHitRate', v)}
                            mode={filters.h2hHitRateMode}
                            onModeChange={(v) => updateFilter('h2hHitRateMode', v)}
                            threshold={filters.h2hHitRateThreshold}
                            onThresholdChange={(v) => updateFilter('h2hHitRateThreshold', v)}
                            count={filters.h2hHitRateCount}
                            onCountChange={(v) => updateFilter('h2hHitRateCount', v)}
                            timeWindow={filters.h2hTimeWindow}
                            enableConsecutive={filters.enableH2hConsecutiveHits}
                            onEnableConsecutiveChange={(v) => updateFilter('enableH2hConsecutiveHits', v)}
                            consecutiveHits={filters.h2hConsecutiveHits}
                            onConsecutiveHitsChange={(v) => updateFilter('h2hConsecutiveHits', v)}
                          />
                        </div>
                      )}
                    </div>
                  )}
                </div>

                <div className="px-5 py-4 border-t border-zinc-800 bg-zinc-900/30">
                  <label className="flex items-center gap-2 cursor-pointer group">
                    <div className="relative">
                      <input
                        type="checkbox"
                        checked={filters.separatePlayoffStats}
                        onChange={(e) => updateFilter('separatePlayoffStats', e.target.checked)}
                        className="sr-only"
                      />
                      <div className={`w-10 h-6 rounded-full transition-colors ${filters.separatePlayoffStats ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                        <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.separatePlayoffStats ? 'translate-x-5' : 'translate-x-1'}`} />
                      </div>
                    </div>
                    <div>
                      <span className="text-white text-sm">Separate playoff and regular season</span>
                      <p className="text-zinc-500 text-xs mt-0.5">Only use matching game type in historical analysis</p>
                    </div>
                  </label>
                </div>
              </div>

            <div className="border border-zinc-800 rounded-xl overflow-hidden bg-zinc-900/50">
                <button
                  onClick={() => toggleSection('ai')}
                  className="w-full px-5 py-4 flex items-center justify-between hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="text-left">
                    <h3 className="text-white font-medium">AI & Confidence</h3>
                    <p className="text-zinc-500 text-sm">Model prediction settings</p>
                  </div>
                  <svg
                    className={`w-5 h-5 text-zinc-400 transition-transform ${expandedSections.ai ? 'rotate-180' : ''}`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {expandedSections.ai && (
                  <div className="px-5 pb-5 pt-2 border-t border-zinc-800">
                    <div className="space-y-5">
                      <div>
                        <div className="flex items-center gap-2 mb-2">
                          <label className="block text-zinc-400 text-sm">AI Agreement</label>
                          <InfoTooltip content="Require the AI prediction to agree with your pick direction. Higher levels require stronger agreement with larger margins." />
                        </div>
                        <Select value={filters.aiAgreement} onValueChange={(v) => updateFilter('aiAgreement', v as any)}>
                          <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="disabled">No Requirement</SelectItem>
                            <SelectItem value="simple">Simple (AI agrees with direction)</SelectItem>
                            <SelectItem value="strong">Strong (≥2 pts above/below)</SelectItem>
                            <SelectItem value="very-strong">Very Strong (≥4 pts above/below)</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="space-y-4">
                        <label className="flex items-start gap-3 cursor-pointer group">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.enableMinConfidence}
                              onChange={(e) => updateFilter('enableMinConfidence', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.enableMinConfidence ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enableMinConfidence ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Minimum confidence requirement</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Require AI predictions to meet confidence threshold</p>
                          </div>
                        </label>

                        {filters.enableMinConfidence && (
                          <div className="ml-13">
                            <div className="flex justify-between items-center mb-2">
                              <label className="text-zinc-400 text-sm">AI Prediction Confidence</label>
                              <span className="text-white text-sm font-medium">{filters.minConfidence}%</span>
                            </div>
                            <input
                              type="range"
                              min={0}
                              max={100}
                              value={filters.minConfidence}
                              onChange={(e) => updateFilter('minConfidence', Number(e.target.value))}
                              className="w-full h-2 bg-zinc-700 rounded-lg appearance-none cursor-pointer accent-blue-500"
                            />
                            <div className="flex justify-between text-xs text-zinc-500 mt-1">
                              <span>0%</span>
                              <span>100%</span>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="space-y-3">
              <div className="border border-zinc-800 rounded-xl overflow-hidden bg-zinc-900/50">
                <button
                  onClick={() => toggleSection('matchup')}
                  className="w-full px-5 py-4 flex items-center justify-between hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="text-left">
                    <h3 className="text-white font-medium">Matchup Advantage</h3>
                    <p className="text-zinc-500 text-sm">Defense rankings and pace factors</p>
                  </div>
                  <svg
                    className={`w-5 h-5 text-zinc-400 transition-transform ${expandedSections.matchup ? 'rotate-180' : ''}`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {expandedSections.matchup && (
                  <div className="px-5 pb-5 pt-2 border-t border-zinc-800">
                    <div className="space-y-4">
                      <div className="flex items-center justify-between gap-3">
                        <label className="flex items-start gap-3 cursor-pointer group flex-1">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.enablePositionDefense}
                              onChange={(e) => updateFilter('enablePositionDefense', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.enablePositionDefense ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enablePositionDefense ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Positional Defense Filter</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Target weak defenses at the player's position</p>
                          </div>
                        </label>
                        {filters.enablePositionDefense && (
                          <div className="w-[140px]">
                            <Select value={filters.positionDefenseRank.toString()} onValueChange={(v) => updateFilter('positionDefenseRank', parseInt(v))}>
                              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="5">Bottom 5</SelectItem>
                                <SelectItem value="10">Bottom 10</SelectItem>
                                <SelectItem value="15">Bottom 15</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                        )}
                      </div>

                      <div className="flex items-center justify-between gap-3">
                        <label className="flex items-start gap-3 cursor-pointer group flex-1">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.enableTeamDefense}
                              onChange={(e) => updateFilter('enableTeamDefense', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.enableTeamDefense ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enableTeamDefense ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Overall Team Defense Filter</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Consider overall team defensive rating</p>
                          </div>
                        </label>
                        {filters.enableTeamDefense && (
                          <div className="w-[140px]">
                            <Select value={filters.teamDefenseRank.toString()} onValueChange={(v) => updateFilter('teamDefenseRank', parseInt(v))}>
                              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="5">Bottom 5</SelectItem>
                                <SelectItem value="10">Bottom 10</SelectItem>
                                <SelectItem value="15">Bottom 15</SelectItem>
                                <SelectItem value="30">Any</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                        )}
                      </div>

                      <div className="flex items-center justify-between gap-3">
                        <label className="flex items-start gap-3 cursor-pointer group flex-1">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.enablePace}
                              onChange={(e) => updateFilter('enablePace', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.enablePace ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enablePace ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Pace Filter</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Filter by game pace (recommended)</p>
                          </div>
                        </label>
                        {filters.enablePace && (
                          <div className="w-[140px]">
                            <Select value={filters.paceRequirement} onValueChange={(v) => updateFilter('paceRequirement', v as any)}>
                              <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="any">Any</SelectItem>
                                <SelectItem value="above-average">Above Average</SelectItem>
                                <SelectItem value="fast">Fast</SelectItem>
                                <SelectItem value="very-fast">Very Fast</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>

              
              <div className="border border-zinc-800 rounded-xl overflow-hidden bg-zinc-900/50">
                <button
                  onClick={() => toggleSection('reliability')}
                  className="w-full px-5 py-4 flex items-center justify-between hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="text-left">
                    <h3 className="text-white font-medium">Player & Schedule</h3>
                    <p className="text-zinc-500 text-sm">Minutes, role, and rest considerations</p>
                  </div>
                  <svg
                    className={`w-5 h-5 text-zinc-400 transition-transform ${expandedSections.reliability ? 'rotate-180' : ''}`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {expandedSections.reliability && (
                  <div className="px-5 pb-5 pt-2 border-t border-zinc-800">
                    <div className="space-y-5">
                      <div className="space-y-4">
                        <label className="flex items-start gap-3 cursor-pointer group">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.enableMinMinutes}
                              onChange={(e) => updateFilter('enableMinMinutes', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.enableMinMinutes ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.enableMinMinutes ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Minimum minutes requirement</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Require players to average minimum minutes played</p>
                          </div>
                        </label>

                        {filters.enableMinMinutes && (
                          <div className="ml-13 grid grid-cols-2 gap-4">
                            <div>
                              <label className="block text-zinc-400 text-sm mb-2">Min. Minutes</label>
                              <Select value={filters.minMinutes.toString()} onValueChange={(v) => updateFilter('minMinutes', parseInt(v))}>
                                <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="20">20 minutes</SelectItem>
                                  <SelectItem value="25">25 minutes</SelectItem>
                                  <SelectItem value="28">28 minutes</SelectItem>
                                  <SelectItem value="30">30 minutes</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div>
                              <label className="block text-zinc-400 text-sm mb-2">Over Last</label>
                              <Select value={filters.minMinutesWindow.toString()} onValueChange={(v) => updateFilter('minMinutesWindow', parseInt(v) as any)}>
                                <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="3">3 games</SelectItem>
                                  <SelectItem value="5">5 games</SelectItem>
                                  <SelectItem value="10">10 games</SelectItem>
                                  <SelectItem value="15">15 games</SelectItem>
                                  <SelectItem value="20">20 games</SelectItem>
                                  <SelectItem value="999">Entire Season</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                          </div>
                        )}
                      </div>

                      <div>
                        <div className="flex items-center gap-2 mb-2">
                          <label className="block text-zinc-400 text-sm">Usage Rate</label>
                          <InfoTooltip content="Filter by how much the player is involved in their team's plays. High usage players touch the ball more often." />
                        </div>
                        <Select value={filters.playerRole} onValueChange={(v) => updateFilter('playerRole', v as any)}>
                          <SelectTrigger className="bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="any">Any</SelectItem>
                            <SelectItem value="high-usage">High Usage (≥25%)</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="pt-3 border-t border-zinc-800">
                        <label className="flex items-start gap-3 cursor-pointer group">
                          <div className="relative mt-0.5">
                            <input
                              type="checkbox"
                              checked={filters.excludeTiredVsRested}
                              onChange={(e) => updateFilter('excludeTiredVsRested', e.target.checked)}
                              className="sr-only"
                            />
                            <div className={`w-10 h-6 rounded-full transition-colors ${filters.excludeTiredVsRested ? 'bg-blue-500' : 'bg-zinc-700'}`}>
                              <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${filters.excludeTiredVsRested ? 'translate-x-5' : 'translate-x-1'}`} />
                            </div>
                          </div>
                          <div>
                            <span className="text-white text-sm">Exclude tired vs well-rested matchups</span>
                            <p className="text-zinc-500 text-xs mt-0.5">Skip picks where player is on back-to-back and opponent is rested</p>
                          </div>
                        </label>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          
          <div className="mt-6 flex gap-3">
            <button
              onClick={handleSearch}
              className="flex-1 bg-blue-500 hover:bg-blue-600 text-white font-medium py-3 px-6 rounded-xl transition-colors flex items-center justify-center gap-2"
            >
              <svg className="w-5 h-5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <span className="whitespace-nowrap">Find Picks</span>
            </button>
            <button
              onClick={handleReset}
              className="px-6 py-3 text-zinc-400 hover:text-white border border-zinc-700 hover:border-zinc-600 rounded-xl transition-colors shrink-0"
            >
              <span className="whitespace-nowrap">Reset</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
