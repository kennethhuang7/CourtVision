import { useState } from 'react';
import { MapPin, ChevronDown, ChevronUp, TrendingUp } from 'lucide-react';
import { Prediction } from '@/types/nba';
import { ConfidenceBadge } from './ConfidenceBadge';
import { PlayerDetailModal } from './PlayerDetailModal';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface PlayerCardProps {
  prediction: Prediction;
  showCompare?: boolean;
}

export function PlayerCard({ prediction, showCompare = false }: PlayerCardProps) {
  const [compareOpen, setCompareOpen] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const { player, predictedStats, actualStats, confidence, isHome } = prediction;

  const handleCardClick = (e: React.MouseEvent<HTMLDivElement>) => {
    e.stopPropagation();
    
    
    setModalOpen(true);
  };

  const stats = [
    { label: 'PTS', key: 'points' },
    { label: 'REB', key: 'rebounds' },
    { label: 'AST', key: 'assists' },
    { label: 'STL', key: 'steals' },
    { label: 'BLK', key: 'blocks' },
    { label: 'TO', key: 'turnovers' },
    { label: '3PM', key: 'threePointersMade' },
  ] as const;

  return (
    <>
      <div
        onClick={handleCardClick}
        className="player-card-horizontal group cursor-pointer relative overflow-hidden transition-all duration-300 hover:shadow-lg hover:shadow-primary/10 hover:scale-[1.01]"
        style={{ 
          cursor: 'pointer', 
          userSelect: 'none'
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/5 to-primary/0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 pointer-events-none" />
        
        <div className="relative flex-shrink-0 z-10 pointer-events-none">
          <div className="w-16 h-16 md:w-20 md:h-20 rounded-full overflow-hidden ring-2 ring-primary/30 group-hover:ring-primary/60 transition-all duration-300 bg-gradient-to-br from-secondary via-muted to-secondary shadow-lg group-hover:shadow-xl group-hover:shadow-primary/20">
            <img
              src={player.photoUrl}
              alt={player.name}
              className="w-full h-full object-cover object-top"
              onError={(e) => {
                e.currentTarget.src = '/player-placeholder.png';
              }}
            />
          </div>
          <div className="absolute inset-0 rounded-full bg-primary/30 blur-xl opacity-0 group-hover:opacity-100 transition-opacity duration-300 -z-10" />
        </div>

        <div className="flex flex-col justify-center w-[180px] md:w-[220px] flex-shrink-0 z-10 pointer-events-none min-w-0">
          <div className="flex items-center gap-2 mb-1 min-w-0">
            <h3 className="text-base md:text-lg font-bold text-foreground truncate leading-tight group-hover:text-primary transition-colors flex-1 min-w-0">{player.name}</h3>
            <TrendingUp className="h-4 w-4 shrink-0 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <ConfidenceBadge confidence={confidence} size="sm" />
            <span className={cn(
              'inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-medium border transition-all shrink-0',
              isHome
                ? 'bg-gradient-to-r from-accent/20 to-primary/20 text-accent border-accent/30 group-hover:from-accent/30 group-hover:to-primary/30'
                : 'bg-gradient-to-r from-warning/20 to-warning/10 text-warning border-warning/30 group-hover:from-warning/30 group-hover:to-warning/20'
            )}>
              <MapPin className="h-3 w-3 shrink-0" />
              <span className="whitespace-nowrap">{isHome ? 'HOME' : 'AWAY'}</span>
            </span>
          </div>
          <div className="text-xs text-muted-foreground mt-1 truncate leading-tight">{player.position} • {player.teamAbbr}</div>
        </div>

        <div className="flex-1 grid grid-cols-7 gap-2 md:gap-4 z-10 pointer-events-none">
          {stats.map(({ label, key }) => (
            <div 
              key={key} 
              className="stat-box flex flex-col items-center justify-center p-2 rounded-lg pointer-events-none"
              style={{
                border: 'none',
                outline: 'none',
                boxShadow: 'none',
                borderWidth: '0',
                borderStyle: 'none',
                borderColor: 'transparent'
              }}
            >
              <span className="text-xs font-medium text-muted-foreground mb-1 whitespace-nowrap">{label}</span>
              <span className="text-sm md:text-xl font-bold bg-gradient-to-b from-foreground to-foreground/70 bg-clip-text text-transparent group-hover:from-primary group-hover:to-primary/80 transition-all whitespace-nowrap">
                {typeof predictedStats[key] === 'number'
                  ? predictedStats[key].toFixed(1)
                  : predictedStats[key]}
              </span>
              {compareOpen && actualStats && (
                <span className={cn(
                  'text-xs mt-1 font-medium whitespace-nowrap',
                  Math.abs(predictedStats[key] - actualStats[key]) < 2
                    ? 'text-success'
                    : Math.abs(predictedStats[key] - actualStats[key]) < 4
                      ? 'text-warning'
                      : 'text-destructive'
                )}>
                  {typeof actualStats[key] === 'number'
                    ? actualStats[key].toFixed(1)
                    : actualStats[key]}
                </span>
              )}
            </div>
          ))}
        </div>

        {showCompare && actualStats && (
          <Button
            variant="ghost"
            size="sm"
            className="flex-shrink-0 text-muted-foreground hover:text-foreground hover:bg-primary/10 z-10"
            onClick={(e) => {
              e.stopPropagation();
              setCompareOpen(!compareOpen);
            }}
          >
            {compareOpen ? (
              <ChevronUp className="h-4 w-4 shrink-0" />
            ) : (
              <ChevronDown className="h-4 w-4 shrink-0" />
            )}
          </Button>
        )}
      </div>

      <PlayerDetailModal 
        prediction={prediction} 
        open={modalOpen} 
        onOpenChange={setModalOpen} 
      />
    </>
  );
}
