export interface Player {
  id: string;
  name: string;
  team: string;
  teamAbbr: string;
  position: string;
  photoUrl: string;
}

export interface FeatureExplanation {
  feature_name: string;
  description: string;
  value: number;
  context: string;
  impact_tier: 'strong_positive' | 'moderate_positive' | 'slight_positive' | 'neutral' | 'slight_negative' | 'moderate_negative' | 'strong_negative';
  impact_symbol: '+++' | '++' | '+' | '=' | '-' | '--' | '---';
  importance_rank: number;
}

export interface FeatureExplanations {
  points?: FeatureExplanation[];
  rebounds?: FeatureExplanation[];
  assists?: FeatureExplanation[];
  steals?: FeatureExplanation[];
  blocks?: FeatureExplanation[];
  turnovers?: FeatureExplanation[];
  threePointersMade?: FeatureExplanation[];
  three_pointers_made?: FeatureExplanation[]; 
}

export interface Prediction {
  id: string;
  predictionId?: number; 
  playerId: string;
  player: Player;
  gameId: string;
  gameDate: string;
  opponent: string;
  opponentAbbr: string;
  isHome: boolean;
  confidence: number;
  predictedStats: PlayerStats;
  actualStats?: PlayerStats;
  predictionError?: number;
  featureExplanations?: FeatureExplanations;
}

export interface PlayerStats {
  points: number;
  rebounds: number;
  assists: number;
  steals: number;
  blocks: number;
  turnovers: number;
  threePointersMade: number;
}

export interface Game {
  id: string;
  date: string;
  homeTeam: string;
  homeTeamAbbr: string;
  homeTeamCity?: string;
  awayTeam: string;
  awayTeamAbbr: string;
  awayTeamCity?: string;
  predictions: Prediction[];
}

export interface HistoricalGame {
  id: string;
  date: string;
  opponent: string;
  opponentAbbr: string;
  isHome: boolean;
  result: 'W' | 'L';
  score: string;
  stats: PlayerStats;
  minutesPlayed: number;
  season?: string;
  teamId?: number; 
  opponentTeamId?: number; 
}

export interface ModelPerformance {
  stat: string;
  mae: number;
  predictions: number;
}

export interface ContextInfo {
  opponentDefense?: string;
  starPlayersOut?: string;
  restDays?: string;
  playoffExperience?: string;
  paceComparison?: string;
}

export type ConfidenceLevel = 'high' | 'medium' | 'low';
export type TimeWindow = 'L5' | 'L10' | 'L20' | 'L50' | 'All';
export type StatType = 'points' | 'rebounds' | 'assists' | 'steals' | 'blocks' | 'turnovers' | 'threePointersMade';

export interface UserSettings {
  fontSize: 'small' | 'medium' | 'large' | 'extra-large';
  dateFormat: 'MM/DD/YYYY' | 'DD/MM/YYYY' | 'YYYY-MM-DD';
  defaultTimeWindow: TimeWindow;
  defaultStat: StatType;
  defaultConfidenceFilter: 'all' | 'high' | 'medium' | 'low';
  notificationsEnabled: boolean;
  emailNotifications: boolean;
  desktopNotifications: boolean;
}
