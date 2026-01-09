import { TrendingUp, Eye, Plus, Check, AlertCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { getTeamLogoUrl } from '@/utils/teamLogos';
import type { PickResult } from '@/types/pickFinder';
import { useNavigate } from 'react-router-dom';
import { useSavePick } from '@/hooks/useSavePick';
import { toast } from 'sonner';

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

  const handleViewAnalysis = () => {
    
    
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

  
  const getStrengthColor = (score: number) => {
    if (score >= 80) return 'text-green-500';
    if (score >= 65) return 'text-blue-500';
    if (score >= 50) return 'text-yellow-500';
    return 'text-orange-500';
  };

  const getStrengthLabel = (score: number) => {
    if (score >= 80) return 'Excellent';
    if (score >= 65) return 'Strong';
    if (score >= 50) return 'Good';
    return 'Fair';
  };

  return (
    <div className="rounded-lg border border-border bg-card p-5 hover:border-primary/50 transition-colors">
      <div className="flex items-start gap-4 mb-4">
        <div className="relative flex-shrink-0">
          <img
            src={result.playerPhotoUrl || '/player-placeholder.png'}
            alt={result.playerName}
            className="h-16 w-16 shrink-0 rounded-lg object-cover bg-accent"
            onError={(e) => {
              e.currentTarget.src = '/player-placeholder.png';
            }}
          />
          <div className="absolute -bottom-1 -right-1">
            <img
              src={getTeamLogoUrl(result.teamAbbr)}
              alt={result.teamAbbr}
              className="h-6 w-6 shrink-0 rounded bg-background border border-border"
              onError={(e) => {
                e.currentTarget.style.display = 'none';
              }}
            />
          </div>
        </div>

        <div className="flex-1 min-w-0">
          <h3 className="text-lg font-semibold text-foreground truncate leading-tight">
            {result.playerName}
          </h3>
          <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap leading-tight">
            <span className="whitespace-nowrap">{result.position}</span>
            <span>•</span>
            <span className="whitespace-nowrap">{result.team}</span>
            <span>{result.isHome ? 'vs' : '@'}</span>
            <span className="whitespace-nowrap">{result.opponent}</span>
          </div>
        </div>

        <div className="flex-shrink-0 text-right">
          <div className={cn('text-3xl font-bold whitespace-nowrap', getStrengthColor(result.strengthScore))}>
            {result.strengthScore}
          </div>
          <div className="text-xs text-muted-foreground whitespace-nowrap">{getStrengthLabel(result.strengthScore)}</div>
        </div>
      </div>

      <div className="mb-4 p-3 rounded-lg bg-accent/50 border border-border/50">
        <div className="flex items-center justify-between flex-wrap gap-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-foreground whitespace-nowrap">
              {statLabels[result.statType]}
            </span>
            <Badge variant={result.overUnder === 'over' ? 'default' : 'secondary'} className="font-semibold shrink-0 whitespace-nowrap">
              {result.overUnder === 'over' ? '▲' : '▼'} {result.line}
            </Badge>
          </div>
          <div className="text-sm text-muted-foreground whitespace-nowrap">
            AI predicts: <span className="font-semibold text-foreground">{result.aiPrediction.toFixed(1)}</span>
            {' • '}
            Confidence: <span className="font-semibold text-foreground">{result.confidence}</span>
          </div>
        </div>
      </div>

      <div className="mb-4 space-y-2">
        <div className="flex items-center justify-between text-xs">
          <span className="text-muted-foreground">Strength Indicators</span>
          <div className="flex gap-1">
            {Object.entries(result.strengthBreakdown).map(([key, value]) => {
              if (value === undefined || value === 0) return null;
              return (
                <div key={key} className="h-1 w-8 bg-accent rounded-full overflow-hidden">
                  <div
                    className="h-full bg-primary"
                    style={{ width: `${Math.min((value / 30) * 100, 100)}%` }}
                  />
                </div>
              );
            })}
          </div>
        </div>
        <div className="h-2 bg-accent rounded-full overflow-hidden">
          <div
            className="h-full bg-gradient-to-r from-primary to-primary/80 transition-all"
            style={{ width: `${result.strengthScore}%` }}
          />
        </div>
      </div>

      <div className="mb-4 space-y-1.5">
        {result.reasons.map((reason, index) => (
          <div key={index} className="flex items-start gap-2 text-sm">
            <Check className="h-4 w-4 text-green-500 flex-shrink-0 mt-0.5 shrink-0" />
            <span className="text-foreground leading-tight">{reason}</span>
          </div>
        ))}
      </div>

      {result.warnings && result.warnings.length > 0 && (
        <div className="mb-4 space-y-1.5">
          {result.warnings.map((warning, index) => (
            <div key={index} className="flex items-start gap-2 text-sm">
              <AlertCircle className="h-4 w-4 text-orange-500 flex-shrink-0 mt-0.5 shrink-0" />
              <span className="text-muted-foreground leading-tight">{warning}</span>
            </div>
          ))}
        </div>
      )}

      <div className="flex gap-2 pt-3 border-t border-border">
        <Button variant="outline" size="sm" onClick={handleViewAnalysis} className="flex-1">
          <Eye className="mr-2 h-4 w-4 shrink-0" />
          <span className="whitespace-nowrap">View in Analysis</span>
        </Button>
        <Button size="sm" onClick={handleAddToPicks} className="flex-1" disabled={isSavingPick}>
          <Plus className="mr-2 h-4 w-4 shrink-0" />
          <span className="whitespace-nowrap">{isSavingPick ? 'Saving...' : 'Add to My Picks'}</span>
        </Button>
      </div>
    </div>
  );
}
