import { useMemo, useState } from 'react';
import { TrendingUp, TrendingDown, Eye, Plus, Check, AlertCircle, ChevronDown, Shield, Target, CheckCircle2, Flame } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { getTeamLogoUrl } from '@/utils/teamLogos';
import type { PickResult } from '@/types/pickFinder';
import { useNavigate } from 'react-router-dom';
import { useSavePick } from '@/hooks/useSavePick';
import { toast } from 'sonner';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';

interface PickResultCardProps {
  result: PickResult;
}

const statLabels: Record<string, string> = {
  points: 'Points',
  rebounds: 'Rebounds',
  assists: 'Assists',
  steals: 'Steals',
  blocks: 'Blocks',
  turnovers: 'Turnovers',
  threePointersMade: '3PM',
};

export function PickResultCard({ result }: PickResultCardProps) {
  const navigate = useNavigate();
  const { mutate: savePick, isPending: isSavingPick } = useSavePick();
  const [expanded, setExpanded] = useState(false);

  const handleViewAnalysis = () => {
    sessionStorage.setItem('shared-selected-date', result.gameDate);
    localStorage.setItem('player-analysis-selected-game', result.gameId);
    localStorage.setItem('player-analysis-selected-player', result.playerId);
    localStorage.setItem('player-analysis-selected-stat', result.statType);
    localStorage.setItem('player-analysis-line-value', result.line.toString());
    localStorage.setItem('player-analysis-nav-timestamp', Date.now().toString());
    navigate('/dashboard/player-analysis');
  };

  const handleAddToPicks = () => {
    savePick(
      {
        playerId: result.playerId,
        gameId: result.gameId,
        statName: result.statType,
        lineValue: result.line,
        overUnder: result.overUnder,
      },
      {
        onSuccess: () => {
          toast.success('Pick added to My Picks!');
        },
        onError: (error: Error) => {
          
          if (error.message.includes('logged in')) {
            toast.error('Please log in to save picks');
          } else if (error.message.includes('duplicate') || error.message.includes('already exists')) {
            toast.error('You already have this pick saved');
          } else {
            toast.error('Failed to add pick. Please try again.');
          }
        },
      }
    );
  };

  
  const getStrengthLabel = (score: number) => {
    if (score >= 90) return 'Excellent';
    if (score >= 70) return 'Strong';
    if (score >= 50) return 'Good';
    return 'Fair';
  };

  const strengthTier = useMemo(() => {
    if (result.strengthScore >= 90) {
      return { color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' };
    }
    if (result.strengthScore >= 70) {
      return { color: 'text-primary', bg: 'bg-primary/10 border-primary/20' };
    }
    if (result.strengthScore >= 50) {
      return { color: 'text-amber-400', bg: 'bg-amber-500/10 border-amber-500/20' };
    }
    return { color: 'text-orange-400', bg: 'bg-orange-500/10 border-orange-500/20' };
  }, [result.strengthScore]);

  const strengthBreakdownChips = useMemo(() => {
    return Object.entries(result.strengthBreakdown)
      .filter(([, value]) => value !== undefined && value !== 0)
      .map(([key, value]) => ({ key, value: Math.round(value as number) }))
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));
  }, [result.strengthBreakdown]);

  const strengthBreakdownLabel = useMemo(() => {
    return {
      hitRate: 'Hit rate',
      contextHitRate: 'Context',
      aiMargin: 'AI edge',
      confidence: 'Confidence',
      defenseRank: 'Defense',
      pace: 'Pace',
    } as Record<string, string>;
  }, []);

  const strengthBreakdownHelp = useMemo(() => {
    return {
      hitRate: 'Bonus points from recent hit rate performance.',
      contextHitRate: 'Bonus points from the selected context split (home/away, etc.).',
      aiMargin: 'Bonus points from AI prediction margin vs your line.',
      confidence: 'Bonus points from model confidence.',
      defenseRank: 'Bonus points from matchup defense filters.',
      pace: 'Bonus points from pace-related filters.',
    } as Record<string, string>;
  }, []);

  const topIndicatorBadges = useMemo(() => {
    const reasons = result.reasons || [];
    const picked: { key: string; icon: JSX.Element; text: string; className: string }[] = [];

    const ai = reasons.find((r) => r.toLowerCase().includes('ai predicts'));
    if (ai) {
      const deltaMatch = ai.match(/\(([-\d.]+)\s+(above|below)\s+line\)/i);
      const delta =
        deltaMatch
          ? `${deltaMatch[2].toLowerCase() === 'below' ? '-' : '+'}${Math.abs(Number(deltaMatch[1])).toFixed(1)} vs line`
          : null;
      const isPositive = deltaMatch ? deltaMatch[2].toLowerCase() === 'above' : true;
      picked.push({
        key: 'ai',
        icon: isPositive ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />,
        text: delta || 'AI edge',
        className: 'text-primary/80 border-primary/20 bg-primary/10',
      });
    }

    const conf = reasons.find((r) => r.toLowerCase().includes('confidence'));
    if (conf) {
      const pct = conf.match(/(\d+)\s*%/);
      picked.push({
        key: 'conf',
        icon: <Target className="h-3.5 w-3.5" />,
        text: pct ? `${pct[1]}% confidence` : 'Confidence',
        className: 'text-muted-foreground border-border bg-secondary/10',
      });
    }

    const hit = reasons.find((r) => r.toLowerCase().includes('hit in'));
    if (hit) {
      const pct = hit.match(/\((\d+)%\)/);
      picked.push({
        key: 'hit',
        icon: <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400" />,
        text: pct ? `${pct[1]}% hit rate` : 'Hit rate',
        className: 'text-emerald-300 border-emerald-500/20 bg-emerald-500/10',
      });
    }

    const streak = reasons.find((r) => r.toLowerCase().includes('consecutive'));
    if (streak) {
      const n = streak.match(/last\s+(\d+)\s+consecutive/i);
      picked.push({
        key: 'streak',
        icon: <Flame className="h-3.5 w-3.5 text-orange-400" />,
        text: n ? `${n[1]} game streak` : 'Streak',
        className: 'text-orange-300 border-orange-500/20 bg-orange-500/10',
      });
    }

    return picked.slice(0, 3);
  }, [result.reasons]);

  return (
    <div className="group relative overflow-hidden rounded-2xl border border-border/40 bg-card transition-all duration-200 hover:border-border/70 hover:bg-secondary/50 hover:shadow-sm hover:-translate-y-0.5 animate-in fade-in slide-in-from-bottom-2">
      {/* Shine effect */}
      <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none">
        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
      </div>
      <div className="relative flex flex-col gap-4 px-4 pt-4 pb-3 sm:flex-row sm:items-center sm:px-6 sm:pt-6 sm:pb-3">
        <div className="flex flex-1 gap-3 min-w-0">
          <div className="relative shrink-0">
            <img
              src={result.playerPhotoUrl || '/player-placeholder.png'}
              alt={result.playerName}
              className="h-14 w-14 rounded-xl object-cover bg-secondary ring-1 ring-border sm:h-16 sm:w-16"
              onError={(e) => {
                e.currentTarget.src = '/player-placeholder.png';
              }}
            />
            <div className="absolute -bottom-1 -right-1">
              <img
                src={getTeamLogoUrl(result.teamAbbr)}
                alt={result.teamAbbr}
                className="h-6 w-6 rounded bg-background border border-border shadow-sm"
                onError={(e) => {
                  e.currentTarget.style.display = 'none';
                }}
              />
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <h3 className="text-xl font-semibold leading-tight text-foreground truncate sm:text-2xl">
              {result.playerName}
            </h3>
            <p className="mt-0.5 flex flex-wrap items-center gap-x-1.5 gap-y-0.5 text-sm text-muted-foreground leading-tight sm:text-base">
              <span className="whitespace-nowrap">{result.position}</span>
              <span className="text-muted-foreground/60">•</span>
              <span className="whitespace-nowrap">{result.team}</span>
              <span className="whitespace-nowrap">
                {result.isHome ? 'vs' : '@'} {result.opponent}
              </span>
            </p>
          </div>
        </div>

        <div className="shrink-0 text-center sm:ml-auto">
          <div className={cn('text-4xl font-bold leading-none tabular-nums sm:text-5xl', strengthTier.color)}>
            {result.strengthScore}
          </div>
          <Badge
            variant="outline"
            className={cn('mt-1 text-xs uppercase tracking-wide', strengthTier.bg)}
          >
            {getStrengthLabel(result.strengthScore)}
          </Badge>
        </div>
      </div>

      <div className="relative -mx-4 border-y border-primary/10 border-l-4 border-primary bg-secondary/50 px-4 py-3.5 sm:-mx-6 sm:px-6">
        <div className="flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="text-sm font-medium">
            {statLabels[result.statType]}
          </Badge>
          <Badge variant="outline" className="whitespace-nowrap">
            {result.overUnder === 'over' ? 'Over' : 'Under'} {result.line}
          </Badge>
          <span className="h-4 w-px bg-border/60 hidden sm:inline" />
          <span className="text-base font-semibold whitespace-nowrap">
            AI predicts <span className="text-primary font-semibold tabular-nums">{result.aiPrediction.toFixed(1)}</span>
          </span>
          <span className="h-4 w-px bg-border/60 hidden sm:inline" />
          <span className="text-base font-semibold whitespace-nowrap">
            Confidence <span className="text-primary font-semibold tabular-nums">{result.confidence}</span>
          </span>
        </div>
      </div>

      <div className="border-t border-border/40 px-4 py-3 sm:px-6">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
          className="group/expand flex w-full items-center justify-between gap-3 text-left transition-colors hover:text-foreground"
        >
          <span className="text-sm text-muted-foreground transition-colors group-hover/expand:text-foreground">
            {result.reasons.length} strength indicators
            {result.warnings && result.warnings.length > 0 ? ` • ${result.warnings.length} warning${result.warnings.length === 1 ? '' : 's'}` : ''}
          </span>
          <ChevronDown
            className={cn(
              'h-4 w-4 shrink-0 transition-transform text-muted-foreground group-hover/expand:translate-y-0.5',
              expanded && 'rotate-180'
            )}
          />
        </button>

        {!expanded && (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {topIndicatorBadges.map((b) => (
              <span
                key={b.key}
                className={cn('inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs', b.className)}
              >
                {b.icon}
                <span className="truncate">{b.text}</span>
              </span>
            ))}
            {topIndicatorBadges.length === 0 ? (
              <span className="text-xs text-muted-foreground">Tap to view details</span>
            ) : null}
          </div>
        )}

        {expanded && (
          <div className="mt-3 space-y-3 animate-in fade-in slide-in-from-top-1 duration-200">
            {strengthBreakdownChips.length > 0 ? (
              <TooltipProvider delayDuration={150}>
                <div className="flex flex-wrap gap-1.5">
                  {strengthBreakdownChips.map((c) => (
                    <Tooltip key={c.key}>
                      <TooltipTrigger asChild>
                        <span className="inline-flex h-6 cursor-help items-center rounded-full border border-border/60 bg-secondary/10 px-2 text-xs text-muted-foreground hover:bg-secondary/20">
                          {strengthBreakdownLabel[c.key] ?? c.key} +{c.value}
                        </span>
                      </TooltipTrigger>
                      <TooltipContent side="top" align="center" collisionPadding={12} className="max-w-xs text-xs">
                        {strengthBreakdownHelp[c.key] ?? 'Bonus points for this component.'}
                      </TooltipContent>
                    </Tooltip>
                  ))}
                </div>
              </TooltipProvider>
            ) : null}

            <div className="grid grid-cols-1 gap-2 lg:grid-cols-2">
              {result.reasons.map((reason, idx) => (
                <div
                  key={idx}
                  className="flex items-start gap-2 rounded-md p-2 text-sm transition-colors hover:bg-secondary/70"
                >
                  <Check className="mt-0.5 h-4 w-4 shrink-0 text-emerald-500" />
                  <span className="leading-relaxed text-foreground/90">{reason}</span>
                </div>
              ))}
              {result.warnings && result.warnings.length > 0 ? (
                <div className="col-span-full mt-1 rounded-lg border border-orange-500/20 bg-orange-500/5 p-3">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <AlertCircle className="h-4 w-4 text-orange-500 shrink-0" />
                    Warnings
                  </div>
                  <div className="mt-2 space-y-1.5">
                    {result.warnings.map((w, i) => (
                      <div key={i} className="text-sm text-muted-foreground leading-relaxed">
                        {w}
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        )}
      </div>

      <div className="flex flex-col gap-2 border-t border-border/40 p-4 sm:flex-row sm:gap-3 sm:p-6">
        <Button
          size="default"
          onClick={handleAddToPicks}
          className="flex-1 sm:flex-none sm:min-w-[180px]"
          disabled={isSavingPick}
        >
          <Plus className="mr-2 h-4 w-4 shrink-0" />
          <span className="whitespace-nowrap">{isSavingPick ? 'Saving...' : 'Add to My Picks'}</span>
        </Button>
        <Button
          variant="ghost"
          size="default"
          onClick={handleViewAnalysis}
          className="flex-1 sm:flex-none"
        >
          <Eye className="mr-2 h-4 w-4 shrink-0" />
          <span className="whitespace-nowrap">View in Analysis</span>
        </Button>
      </div>
    </div>
  );
}
