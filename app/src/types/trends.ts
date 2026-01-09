import type { StatType } from './nba';

export type TrendType = 'recent-form' | 'h2h' | 'home-away';

export interface TrendFilters {
  statType: StatType | 'all';
  overUnder: 'over' | 'under' | 'both';
  trendTypes: TrendType[]; 
  minStreak: 3 | 4 | 5; 

  
  lineMethod: 'player-average' | 'ai-prediction';
  lineAdjustment: 'standard' | 'favorable' | 'custom';
  customModifiers?: { [key: string]: number };

  
  requireAiAgreement?: boolean;

  
  playerSearch?: string;
  teams?: string[];
  opponents?: string[];
}

export interface Trend {
  
  playerId: string;
  playerName: string;
  playerPhotoUrl: string;
  position: string;
  team: string;
  teamAbbr: string;
  gameId: string;
  opponent: string;
  opponentAbbr: string;
  isHome: boolean;
  gameDate: string;

  
  statType: StatType;
  overUnder: 'over' | 'under';
  line: number;
  lineMethod: 'player-average' | 'ai-prediction';
  lineAdjustment: 'standard' | 'favorable' | 'custom';
  aiPrediction?: number;
  confidence?: number;

  
  hitRate: number; 
  hitCount: number;
  totalGames: number;
  consecutiveHits: number;

  
  contextHitRate?: number;
  contextHitCount?: number;
  contextTotalGames?: number;

  
  trendScore: number; 

  
  trendLabel: string; 
}
