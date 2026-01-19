import type { StatType } from './nba';

export interface PickFinderFilters {
  
  statType: StatType | 'all';
  overUnder: 'over' | 'under' | 'both';
  lineMethod: 'player-average' | 'ai-prediction';
  lineAdjustment: 'standard' | 'favorable' | 'custom';

  
  customModifiers: {
    points: number;
    rebounds: number;
    assists: number;
    steals: number;
    blocks: number;
    turnovers: number;
    threePointersMade: number;
  };

  
  timeWindow: 3 | 5 | 10 | 15 | 20;
  enableHitRateThreshold: boolean; 
  hitRateMode: 'percentage' | 'count'; 
  hitRateThreshold: number; 
  hitRateCount: number; 
  enableConsecutiveHits: boolean; 
  consecutiveHits: number; 

  
  enableContextSplit: boolean;
  contextTimeWindow: 3 | 5 | 10 | 15 | 20;
  enableContextHitRate: boolean; 
  contextHitRateMode: 'percentage' | 'count'; 
  contextHitRateThreshold: number; 
  contextHitRateCount: number; 
  enableContextConsecutiveHits: boolean;
  contextConsecutiveHits: number;

  
  enableH2H: boolean; 
  h2hTimeWindow: 3 | 5 | 10 | 15 | 20;
  enableH2hHitRate: boolean; 
  h2hHitRateMode: 'percentage' | 'count'; 
  h2hHitRateThreshold: number; 
  h2hHitRateCount: number; 
  enableH2hConsecutiveHits: boolean;
  h2hConsecutiveHits: number;

  
  separatePlayoffStats: boolean; 

  
  enablePositionDefense: boolean;
  positionDefenseRank: number; 
  enableTeamDefense: boolean;
  teamDefenseRank: number; 
  enablePace: boolean;
  paceRequirement: 'any' | 'above-average' | 'fast' | 'very-fast';

  
  aiAgreement: 'disabled' | 'simple' | 'strong' | 'very-strong';
  enableMinConfidence: boolean; 
  minConfidence: number; 

  
  excludeTiredVsRested: boolean;

  
  enableMinMinutes: boolean; 
  minMinutes: number; 
  minMinutesWindow: 3 | 5 | 10 | 15 | 20 | 999; 
  playerRole: 'any' | 'high-usage'; 
}

export interface PickResult {
  
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
  aiPrediction: number;
  confidence: number;

  
  hitRate: number; 
  hitCount: number;
  totalGames: number;
  contextHitRate?: number; 
  contextHitCount?: number;
  contextTotalGames?: number;
  h2hHitRate?: number; 
  h2hHitCount?: number;
  h2hTotalGames?: number;
  consecutiveHits: number;

  
  positionDefenseRank?: number;
  teamDefenseRank?: number;
  paceBonus?: number; 

  
  strengthScore: number; 
  strengthBreakdown: {
    hitRate: number;
    contextHitRate?: number;
    aiMargin: number;
    confidence: number;
    defenseRank: number;
    pace?: number;
  };

  
  reasons: string[]; 
  warnings?: string[]; 
}

export interface LoadingStage {
  id: string;
  label: string;
  status: 'pending' | 'in-progress' | 'completed';
}
