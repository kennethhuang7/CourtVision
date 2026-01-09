import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { Game } from '@/types/nba';
import { PlayerCard } from './PlayerCard';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface GameSectionProps {
  game: Game;
  showCompare?: boolean;
}

export function GameSection({ game, showCompare = false }: GameSectionProps) {
  const [isOpen, setIsOpen] = useState(true);
  const [teamFilter, setTeamFilter] = useState<'all' | 'home' | 'away'>('all');

  
  const sortedPredictions = [...game.predictions].sort((a, b) => b.predictedStats.points - a.predictedStats.points);

  const filteredPredictions = sortedPredictions.filter(p => {
    if (teamFilter === 'all') return true;
    if (teamFilter === 'home') return p.isHome;
    return !p.isHome;
  });

  const homePredictions = sortedPredictions.filter(p => p.isHome);
  const awayPredictions = sortedPredictions.filter(p => !p.isHome);

  return (
    <div className="section-gradient">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex w-full items-center justify-between p-3 hover:bg-gradient-to-r hover:from-primary/5 hover:to-transparent rounded-lg transition-all duration-200"
      >
        <div className="flex items-center gap-3">
          <div className={cn(
            'flex items-center justify-center w-8 h-8 rounded-lg transition-all duration-200',
            'bg-gradient-to-br from-secondary to-muted'
          )}>
            {isOpen ? (
              <ChevronDown className="h-5 w-5 text-primary" />
            ) : (
              <ChevronRight className="h-5 w-5 text-muted-foreground" />
            )}
          </div>
          <h2 className="text-lg font-semibold bg-gradient-to-r from-foreground to-foreground/80 bg-clip-text text-transparent">
            {game.awayTeamAbbr} @ {game.homeTeamAbbr}
          </h2>
          <span className="text-sm text-muted-foreground px-2 py-0.5 rounded-full bg-gradient-to-r from-muted/50 to-transparent">
            {game.predictions.length} predictions
          </span>
        </div>
      </button>

      {isOpen && (
        <div className="mt-4 animate-fade-in">
          <div className="mb-4 flex gap-2 p-1 bg-gradient-to-r from-secondary/50 via-muted/30 to-secondary/50 rounded-lg w-fit">
            {['all', 'home', 'away'].map((filter) => (
              <Button
                key={filter}
                variant={teamFilter === filter ? 'default' : 'ghost'}
                size="sm"
                onClick={() => setTeamFilter(filter as typeof teamFilter)}
                className={cn(
                  'transition-all duration-200',
                  teamFilter === filter 
                    ? 'shadow-lg bg-gradient-to-r from-primary to-accent text-primary-foreground' 
                    : 'text-muted-foreground hover:text-foreground'
                )}
              >
                {filter === 'all' ? 'All' : filter === 'home' ? game.homeTeamAbbr : game.awayTeamAbbr}
              </Button>
            ))}
          </div>

          {teamFilter === 'all' ? (
            <>
              {awayPredictions.length > 0 && (
                <div style={{ marginBottom: 'var(--density-section-gap, 1.5rem)' }}>
                  <h3 className="flex items-center density-gap text-sm font-medium text-muted-foreground" style={{ marginBottom: 'var(--density-gap, 1rem)' }}>
                    <span className="h-2 w-2 rounded-full bg-gradient-to-r from-warning to-warning/50" />
                    {game.awayTeamAbbr} (Away)
                  </h3>
                  <div className="flex flex-col density-gap">
                    {awayPredictions.map(prediction => (
                      <PlayerCard key={prediction.id} prediction={prediction} showCompare={showCompare} />
                    ))}
                  </div>
                </div>
              )}

              {homePredictions.length > 0 && (
                <div>
                  <h3 className="flex items-center density-gap text-sm font-medium text-muted-foreground" style={{ marginBottom: 'var(--density-gap, 1rem)' }}>
                    <span className="h-2 w-2 rounded-full bg-gradient-to-r from-accent to-primary" />
                    {game.homeTeamAbbr} (Home)
                  </h3>
                  <div className="flex flex-col density-gap">
                    {homePredictions.map(prediction => (
                      <PlayerCard key={prediction.id} prediction={prediction} showCompare={showCompare} />
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="flex flex-col density-gap">
              {filteredPredictions.map(prediction => (
                <PlayerCard key={prediction.id} prediction={prediction} showCompare={showCompare} />
              ))}
            </div>
          )}

          {filteredPredictions.length === 0 && (
            <p className="py-8 text-center text-muted-foreground">
              No predictions for this filter
            </p>
          )}
        </div>
      )}
    </div>
  );
}
