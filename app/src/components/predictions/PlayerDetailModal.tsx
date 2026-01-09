import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronDown, ChevronUp, MapPin, TrendingUp, TrendingDown, Minus, HelpCircle, Eye, BarChart3 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Prediction, FeatureExplanation } from '@/types/nba';
import { ConfidenceBadge } from './ConfidenceBadge';
import { cn } from '@/lib/utils';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useConfidenceComponents } from '@/hooks/useConfidenceComponents';
import { useEnsemble } from '@/contexts/EnsembleContext';

interface PlayerDetailModalProps {
  prediction: Prediction;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

interface StatFactor {
  name: string;
  impact: 'positive' | 'negative' | 'neutral';
  impactSymbol?: string; 
  value: string;
  weight: number;
  tooltip?: string; 
}


function convertImpactSymbol(symbol: FeatureExplanation['impact']): 'positive' | 'negative' | 'neutral' {
  if (symbol === '+++' || symbol === '++' || symbol === '+') return 'positive';
  if (symbol === '---' || symbol === '--' || symbol === '-') return 'negative';
  return 'neutral';
}


function getFeatureLabel(featureName: string): string {
  const featureMap: Record<string, string> = {
    
    'points_l5': 'Last 5 Games Average',
    'points_l10': 'Last 10 Games Average',
    'points_l20': 'Last 20 Games Average',
    'points_l5_weighted': 'Recent Form (L5 Weighted)',
    'points_l10_weighted': 'Recent Form (L10 Weighted)',
    'points_l20_weighted': 'Recent Form (L20 Weighted)',
    'points_120_weighted': 'Recent Form (L20 Weighted)',
    
    
    'rebounds_l5': 'Last 5 Games Average',
    'rebounds_l10': 'Last 10 Games Average',
    'rebounds_l20': 'Last 20 Games Average',
    'rebounds_l5_weighted': 'Recent Form (L5 Weighted)',
    'rebounds_l10_weighted': 'Recent Form (L10 Weighted)',
    'rebounds_l20_weighted': 'Recent Form (L20 Weighted)',
    'assists_l5': 'Last 5 Games Average',
    'assists_l10': 'Last 10 Games Average',
    'assists_l20': 'Last 20 Games Average',
    'assists_l5_weighted': 'Recent Form (L5 Weighted)',
    'assists_l10_weighted': 'Recent Form (L10 Weighted)',
    'assists_l20_weighted': 'Recent Form (L20 Weighted)',
    'steals_l5': 'Last 5 Games Average',
    'steals_l10': 'Last 10 Games Average',
    'steals_l20': 'Last 20 Games Average',
    'steals_l5_weighted': 'Recent Form (L5 Weighted)',
    'steals_l10_weighted': 'Recent Form (L10 Weighted)',
    'steals_l20_weighted': 'Recent Form (L20 Weighted)',
    'blocks_l5': 'Last 5 Games Average',
    'blocks_l10': 'Last 10 Games Average',
    'blocks_l20': 'Last 20 Games Average',
    'blocks_l5_weighted': 'Recent Form (L5 Weighted)',
    'blocks_l10_weighted': 'Recent Form (L10 Weighted)',
    'blocks_l20_weighted': 'Recent Form (L20 Weighted)',
    'turnovers_l5': 'Last 5 Games Average',
    'turnovers_l10': 'Last 10 Games Average',
    'turnovers_l20': 'Last 20 Games Average',
    'turnovers_l5_weighted': 'Recent Form (L5 Weighted)',
    'turnovers_l10_weighted': 'Recent Form (L10 Weighted)',
    'turnovers_l20_weighted': 'Recent Form (L20 Weighted)',
    'three_pointers_made_l5': 'Last 5 Games Average',
    'three_pointers_made_l10': 'Last 10 Games Average',
    'three_pointers_made_l20': 'Last 20 Games Average',
    'three_pointers_made_l5_weighted': 'Recent Form (L5 Weighted)',
    'three_pointers_made_l10_weighted': 'Recent Form (L10 Weighted)',
    'three_pointers_made_l20_weighted': 'Recent Form (L20 Weighted)',
    
    
    'points_per_36_l5': 'Per-36 Minute Rate (Last 5)',
    'points_per_36_l10': 'Per-36 Minute Rate (Last 10)',
    'points_per_36_l20': 'Per-36 Minute Rate (Last 20)',
    'rebounds_per_36_l5': 'Per-36 Minute Rate (Last 5)',
    'rebounds_per_36_l10': 'Per-36 Minute Rate (Last 10)',
    'rebounds_per_36_l20': 'Per-36 Minute Rate (Last 20)',
    'assists_per_36_l5': 'Per-36 Minute Rate (Last 5)',
    'assists_per_36_l10': 'Per-36 Minute Rate (Last 10)',
    'assists_per_36_l20': 'Per-36 Minute Rate (Last 20)',
    
    
    'minutes_played_l5': 'Average Minutes (Last 5)',
    'minutes_played_l10': 'Average Minutes (Last 10)',
    'minutes_played_l20': 'Average Minutes (Last 20)',
    'minutes_played_l5_weighted': 'Average Minutes (L5 Weighted)',
    'minutes_played_l10_weighted': 'Average Minutes (L10 Weighted)',
    'minutes_played_l20_weighted': 'Average Minutes (L20 Weighted)',
    'minutes_trend': 'Minutes Trend',
    
    
    'usage_rate_l5': 'Usage Rate (Last 5)',
    'usage_rate_l10': 'Usage Rate (Last 10)',
    'usage_rate_l20': 'Usage Rate (Last 20)',
    'usage_rate_l5_weighted': 'Usage Rate (L5 Weighted)',
    'usage_rate_l10_weighted': 'Usage Rate (L10 Weighted)',
    'usage_rate_l20_weighted': 'Usage Rate (L20 Weighted)',
    
    
    'fg_pct_l5': 'Field Goal % (Last 5)',
    'fg_pct_l10': 'Field Goal % (Last 10)',
    'fg_pct_l20': 'Field Goal % (Last 20)',
    'three_pct_l5': 'Three-Point % (Last 5)',
    'three_pct_l10': 'Three-Point % (Last 10)',
    'three_pct_l20': 'Three-Point % (Last 20)',
    'ft_pct_l5': 'Free Throw % (Last 5)',
    'ft_pct_l10': 'Free Throw % (Last 10)',
    'ft_pct_l20': 'Free Throw % (Last 20)',
    'true_shooting_pct_l5': 'True Shooting % (Last 5)',
    'true_shooting_pct_l10': 'True Shooting % (Last 10)',
    'true_shooting_pct_l20': 'True Shooting % (Last 20)',
    
    
    'is_home': 'Home Game',
    'days_rest': 'Days Rest',
    'is_back_to_back': 'Back-to-Back Game',
    'is_well_rested': 'Well Rested (3+ days)',
    'games_in_last_3_days': 'Games in Last 3 Days',
    'games_in_last_7_days': 'Games in Last 7 Days',
    'is_heavy_schedule': 'Heavy Schedule',
    'consecutive_games': 'Consecutive Games Streak',
    'games_played_season': 'Games Played This Season',
    'season_progress': 'Season Progress',
    'is_early_season': 'Early Season',
    'is_mid_season': 'Mid Season',
    'is_late_season': 'Late Season',
    'games_remaining': 'Games Remaining',
    
    
    'offensive_rating_team': 'Team Offensive Rating',
    'defensive_rating_team': 'Team Defensive Rating',
    'pace_team': 'Team Pace',
    'offensive_rating_opp': 'Opponent Offensive Rating',
    'defensive_rating_opp': 'Opponent Defensive Rating',
    'pace_opp': 'Opponent Pace',
    
    
    'opp_field_goal_pct': 'Opponent FG% Allowed',
    'opp_three_point_pct': 'Opponent 3PT% Allowed',
    'opp_team_turnovers_per_game': 'Opponent Turnovers Forced',
    'opp_team_steals_per_game': 'Opponent Steals Per Game',
    'opp_points_allowed_to_position': 'Opponent Points Allowed to Position',
    'opp_rebounds_allowed_to_position': 'Opponent Rebounds Allowed to Position',
    'opp_assists_allowed_to_position': 'Opponent Assists Allowed to Position',
    'opp_blocks_allowed_to_position': 'Opponent Blocks Allowed to Position',
    'opp_three_pointers_allowed_to_position': 'Opponent 3PM Allowed to Position',
    
    
    'star_teammate_out': 'Star Teammate Out',
    'star_teammate_ppg': 'Star Teammate PPG',
    'games_without_star': 'Games Without Star Teammate',
    
    
    'is_starter_l5': 'Starting Status (Last 5)',
    'is_starter_l10': 'Starting Status (Last 10)',
    
    
    'offensive_rating_l5': 'Offensive Rating (Last 5)',
    'offensive_rating_l10': 'Offensive Rating (Last 10)',
    'offensive_rating_l20': 'Offensive Rating (Last 20)',
    'defensive_rating_l5': 'Defensive Rating (Last 5)',
    'defensive_rating_l10': 'Defensive Rating (Last 10)',
    'defensive_rating_l20': 'Defensive Rating (Last 20)',
    'net_rating_l5': 'Net Rating (Last 5)',
    'net_rating_l10': 'Net Rating (Last 10)',
    'net_rating_l20': 'Net Rating (Last 20)',
    
    
    'ast_to_ratio_l5': 'Assist-to-Turnover Ratio (Last 5)',
    'ast_to_ratio_l10': 'Assist-to-Turnover Ratio (Last 10)',
    'ast_to_ratio_l20': 'Assist-to-Turnover Ratio (Last 20)',
    'pts_per_fga_l5': 'Points Per FGA (Last 5)',
    'pts_per_fga_l10': 'Points Per FGA (Last 10)',
    'pts_per_fga_l20': 'Points Per FGA (Last 20)',
    
    
    'tz_difference': 'Timezone Difference',
    'west_to_east': 'Traveling West to East',
    'east_to_west': 'Traveling East to West',
    'arena_altitude': 'Arena Altitude',
    'altitude_away': 'High Altitude Away Game',
    
    
    'is_playoff': 'Playoff Game',
    'playoff_games_career': 'Career Playoff Games',
    'playoff_performance_boost': 'Playoff Performance Boost',
    
    
    'days_since_asb': 'Days Since All-Star Break',
    'post_asb_bounce': 'Post All-Star Break Bounce',
  };

  
  if (featureMap[featureName]) {
    return featureMap[featureName];
  }

  
  const lowerName = featureName.toLowerCase();
  for (const [key, value] of Object.entries(featureMap)) {
    if (key.toLowerCase() === lowerName) {
      return value;
    }
  }

  
  if (lowerName.includes('points') && lowerName.includes('l5') && !lowerName.includes('weighted')) return 'Last 5 Games Average';
  if (lowerName.includes('points') && lowerName.includes('l10') && !lowerName.includes('weighted')) return 'Last 10 Games Average';
  if (lowerName.includes('points') && lowerName.includes('l20') && !lowerName.includes('weighted')) return 'Last 20 Games Average';
  if (lowerName.includes('points') && lowerName.includes('l5') && lowerName.includes('weighted')) return 'Recent Form (L5 Weighted)';
  if (lowerName.includes('points') && lowerName.includes('l10') && lowerName.includes('weighted')) return 'Recent Form (L10 Weighted)';
  if (lowerName.includes('points') && (lowerName.includes('l20') || lowerName.includes('120')) && lowerName.includes('weighted')) return 'Recent Form (L20 Weighted)';
  if (lowerName.includes('rebounds') && lowerName.includes('l5') && !lowerName.includes('weighted')) return 'Last 5 Games Average';
  if (lowerName.includes('rebounds') && lowerName.includes('l10') && !lowerName.includes('weighted')) return 'Last 10 Games Average';
  if (lowerName.includes('rebounds') && lowerName.includes('l20') && !lowerName.includes('weighted')) return 'Last 20 Games Average';
  if (lowerName.includes('rebounds') && lowerName.includes('l5') && lowerName.includes('weighted')) return 'Recent Form (L5 Weighted)';
  if (lowerName.includes('rebounds') && lowerName.includes('l10') && lowerName.includes('weighted')) return 'Recent Form (L10 Weighted)';
  if (lowerName.includes('rebounds') && lowerName.includes('l20') && lowerName.includes('weighted')) return 'Recent Form (L20 Weighted)';
  if (lowerName.includes('assists') && lowerName.includes('l5') && !lowerName.includes('weighted')) return 'Last 5 Games Average';
  if (lowerName.includes('assists') && lowerName.includes('l10') && !lowerName.includes('weighted')) return 'Last 10 Games Average';
  if (lowerName.includes('assists') && lowerName.includes('l20') && !lowerName.includes('weighted')) return 'Last 20 Games Average';
  if (lowerName.includes('assists') && lowerName.includes('l5') && lowerName.includes('weighted')) return 'Recent Form (L5 Weighted)';
  if (lowerName.includes('assists') && lowerName.includes('l10') && lowerName.includes('weighted')) return 'Recent Form (L10 Weighted)';
  if (lowerName.includes('assists') && lowerName.includes('l20') && lowerName.includes('weighted')) return 'Recent Form (L20 Weighted)';
  if (lowerName.includes('usage_rate') && lowerName.includes('l5') && lowerName.includes('weighted')) return 'Usage Rate (L5 Weighted)';
  if (lowerName.includes('usage_rate') && lowerName.includes('l10') && lowerName.includes('weighted')) return 'Usage Rate (L10 Weighted)';
  if (lowerName.includes('usage_rate') && lowerName.includes('l20') && lowerName.includes('weighted')) return 'Usage Rate (L20 Weighted)';
  if (lowerName.includes('usage_rate')) return 'Usage Rate';
  if (lowerName.includes('minutes') && lowerName.includes('l5') && lowerName.includes('weighted')) return 'Average Minutes (L5 Weighted)';
  if (lowerName.includes('minutes') && lowerName.includes('l10') && lowerName.includes('weighted')) return 'Average Minutes (L10 Weighted)';
  if (lowerName.includes('minutes') && lowerName.includes('l20') && lowerName.includes('weighted')) return 'Average Minutes (L20 Weighted)';
  if (lowerName.includes('minutes')) return 'Average Minutes';
  if (lowerName.includes('pace')) return 'Pace';
  if (lowerName.includes('defensive_rating')) return 'Defensive Rating';
  if (lowerName.includes('offensive_rating')) return 'Offensive Rating';
  if (lowerName.includes('days_rest')) return 'Days Rest';
  if (lowerName.includes('star_teammate')) return 'Star Teammate Impact';
  if (lowerName.includes('is_starter')) return 'Starting Status';
  if (lowerName.includes('is_home')) return 'Home Game';
  if (lowerName.includes('is_playoff')) return 'Playoff Game';

  
  return featureName
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}


function detectStatTypeFromFeatureName(featureName: string): string | null {
  const lowerName = featureName.toLowerCase();
  
  
  if (lowerName.includes('pts_per_') || lowerName.includes('ast_to_ratio')) {
    return null;
  }
  
  
  if (lowerName.includes('points') || lowerName.includes('pts') || lowerName.includes('ppg')) {
    return 'points';
  }
  if (lowerName.includes('rebounds') || lowerName.includes('reb') || lowerName.includes('rpg')) {
    return 'rebounds';
  }
  if (lowerName.includes('assists') || lowerName.includes('ast') || lowerName.includes('apg')) {
    return 'assists';
  }
  if (lowerName.includes('steals') || lowerName.includes('stl') || lowerName.includes('spg')) {
    return 'steals';
  }
  if (lowerName.includes('blocks') || lowerName.includes('blk') || lowerName.includes('bpg')) {
    return 'blocks';
  }
  if (lowerName.includes('turnovers') || lowerName.includes('tov') || lowerName.includes('turnover')) {
    return 'turnovers';
  }
  if (lowerName.includes('three') || lowerName.includes('3pm') || lowerName.includes('threepointers')) {
    return 'threepointersmade';
  }
  
  return null; 
}


function formatFeatureValue(featureName: string, statKey: string, rawValue: any): string {
  if (rawValue === null || rawValue === undefined) return '';
  
  const numValue = typeof rawValue === 'number' ? rawValue : parseFloat(String(rawValue));
  if (isNaN(numValue)) return String(rawValue);

  const lowerName = featureName.toLowerCase();
  const lowerStat = statKey.toLowerCase();
  
  
  const detectedStatType = detectStatTypeFromFeatureName(featureName);
  const statTypeForFormatting = detectedStatType || lowerStat;

  
  if (lowerName.includes('is_') || lowerName.includes('_out') || lowerName.includes('_away')) {
    if (numValue === 1 || numValue === 1.0) {
      if (lowerName.includes('is_home')) return 'Home';
      if (lowerName.includes('is_playoff')) return 'Playoff';
      if (lowerName.includes('is_back_to_back')) return 'Yes';
      if (lowerName.includes('is_well_rested')) return 'Yes';
      if (lowerName.includes('is_heavy_schedule')) return 'Yes';
      if (lowerName.includes('is_early_season')) return 'Early';
      if (lowerName.includes('is_mid_season')) return 'Mid';
      if (lowerName.includes('is_late_season')) return 'Late';
      if (lowerName.includes('star_teammate_out')) return 'Yes';
      if (lowerName.includes('altitude_away')) return 'High Altitude';
      return 'Yes';
    }
    if (numValue === 0 || numValue === 0.0) {
      if (lowerName.includes('is_home')) return 'Away';
      return 'No';
    }
  }

  
  if (lowerName.includes('days_rest')) {
    const days = Math.round(numValue);
    return `${days} ${days === 1 ? 'day' : 'days'} rest`;
  }

  
  if (lowerName.includes('games_played') || lowerName.includes('games_without') || lowerName.includes('games_in_last')) {
    const games = Math.round(numValue);
    return `${games} ${games === 1 ? 'game' : 'games'}`;
  }

  
  if (lowerName.includes('_pct') || lowerName.includes('usage_rate') || lowerName.includes('_rate') || lowerName.includes('ratio')) {
    return `${numValue.toFixed(1)}%`;
  }

  
  if (lowerName.includes('rating') || lowerName.includes('pace')) {
    return `${numValue.toFixed(1)}`;
  }

  
  if (lowerName.includes('minutes')) {
    return `${numValue.toFixed(1)} mins`;
  }

  
  if (lowerName.includes('per_36')) {
    const statAbbr = statTypeForFormatting.includes('points') ? 'PPG' :
                     statTypeForFormatting.includes('rebounds') ? 'RPG' :
                     statTypeForFormatting.includes('assists') ? 'APG' :
                     statTypeForFormatting.includes('steals') ? 'SPG' :
                     statTypeForFormatting.includes('blocks') ? 'BPG' :
                     statTypeForFormatting.includes('turnovers') ? 'TOV/G' :
                     statTypeForFormatting.includes('three') ? '3PM' : '';
    return `${numValue.toFixed(1)} ${statAbbr}`;
  }

  
  if (lowerName.includes('star_teammate_ppg')) {
    return `${numValue.toFixed(1)} PPG`;
  }

  
  if (lowerName.includes('altitude')) {
    return `${Math.round(numValue)} ft`;
  }

  
  if (lowerName.includes('tz_difference')) {
    return `${numValue > 0 ? '+' : ''}${Math.round(numValue)} hrs`;
  }

  
  
  if (lowerName.includes('season_progress')) {
    return numValue.toFixed(2);
  }

  
  if (lowerName.includes('days_since_asb')) {
    return `${Math.round(numValue)}`;
  }

  
  if (lowerName.includes('is_starter')) {
    return numValue.toFixed(1);
  }

  
  if (lowerName.includes('consecutive_games')) {
    const games = Math.round(numValue);
    return `${games} ${games === 1 ? 'game' : 'games'}`;
  }

  
  if (lowerName.includes('games_remaining')) {
    const games = Math.round(numValue);
    return `${games} ${games === 1 ? 'game' : 'games'}`;
  }

  
  if (lowerName.includes('position_')) {
    return numValue.toFixed(1);
  }

  
  if (lowerName.includes('post_asb_bounce')) {
    return numValue.toFixed(1);
  }

  
  if (lowerName.includes('west_to_east') || lowerName.includes('east_to_west')) {
    return numValue.toFixed(1);
  }

  
  if (lowerName.includes('playoff_games_career')) {
    const games = Math.round(numValue);
    return `${games} ${games === 1 ? 'game' : 'games'}`;
  }

  
  if (lowerName.includes('playoff_performance_boost')) {
    return numValue.toFixed(2);
  }

  
  if (lowerName.includes('pts_per_fga') || lowerName.includes('pts_per_ast')) {
    return numValue.toFixed(2);
  }

  
  
  if (detectedStatType) {
    const statFormats: Record<string, (v: number) => string> = {
      points: (v) => `${v.toFixed(1)} PPG`,
      rebounds: (v) => `${v.toFixed(1)} RPG`,
      assists: (v) => `${v.toFixed(1)} APG`,
      steals: (v) => `${v.toFixed(1)} SPG`,
      blocks: (v) => `${v.toFixed(1)} BPG`,
      turnovers: (v) => `${v.toFixed(1)} TOV/G`,
      threepointersmade: (v) => `${v.toFixed(1)} 3PM`,
    };

    const formatter = statFormats[detectedStatType];
    if (formatter) {
      return formatter(numValue);
    }
  }

  
  if (numValue % 1 === 0) {
    return String(Math.round(numValue));
  }
  return numValue.toFixed(2);
}


const getFeatureTooltip = (featureName: string, impactTier: string): string | undefined => {
  const tooltips: Record<string, string | ((impact: string) => string)> = {
    'star_teammate_out': () => 'When a star teammate is injured, this player typically sees increased shot opportunities and usage rate',
    'star_teammate_ppg': () => 'The scoring output of the missing star teammate affects how many additional opportunities this player receives',
    'usage_rate_l5': () => 'Percentage of team possessions used by this player while on the court - higher usage means more involved in offense',
    'usage_rate_l10': () => 'Percentage of team possessions used by this player while on the court - higher usage means more involved in offense',
    'usage_rate_l20': () => 'Percentage of team possessions used by this player while on the court - higher usage means more involved in offense',
    'usage_rate_l5_weighted': () => 'Percentage of team possessions used by this player (recent games weighted more heavily)',
    'usage_rate_l10_weighted': () => 'Percentage of team possessions used by this player (recent games weighted more heavily)',
    'usage_rate_l20_weighted': () => 'Percentage of team possessions used by this player (recent games weighted more heavily)',
    'offensive_rating_team': () => 'Points scored per 100 possessions by player\'s team - measures team offensive efficiency',
    'defensive_rating_team': () => 'Points allowed per 100 possessions by player\'s team - measures team defensive efficiency',
    'offensive_rating_opp': (impact) => impact.includes('positive')
      ? 'Opponent has weak offense - creates more transition opportunities and faster pace'
      : 'Opponent has strong offense - may lead to higher scoring game',
    'defensive_rating_opp': (impact) => impact.includes('positive')
      ? 'Opponent has weak defense - easier scoring opportunities for this player'
      : 'Opponent has strong defense - tougher to score against',
    'pace_team': () => 'Number of possessions per 48 minutes for player\'s team - faster pace means more opportunities',
    'pace_opp': () => 'Number of possessions per 48 minutes for opponent - faster pace means more opportunities',
    'true_shooting_pct_l5': () => 'Shooting efficiency accounting for 2-pointers, 3-pointers, and free throws',
    'true_shooting_pct_l10': () => 'Shooting efficiency accounting for 2-pointers, 3-pointers, and free throws',
    'true_shooting_pct_l20': () => 'Shooting efficiency accounting for 2-pointers, 3-pointers, and free throws',
    'offensive_rating_l5': () => 'Points produced per 100 possessions - measures individual offensive impact',
    'offensive_rating_l10': () => 'Points produced per 100 possessions - measures individual offensive impact',
    'offensive_rating_l20': () => 'Points produced per 100 possessions - measures individual offensive impact',
    'defensive_rating_l5': () => 'Points allowed per 100 possessions when player is on court',
    'defensive_rating_l10': () => 'Points allowed per 100 possessions when player is on court',
    'defensive_rating_l20': () => 'Points allowed per 100 possessions when player is on court',
    'reb_rate_l5': () => 'Percentage of available rebounds grabbed while on court',
    'reb_rate_l10': () => 'Percentage of available rebounds grabbed while on court',
    'reb_rate_l20': () => 'Percentage of available rebounds grabbed while on court',
    'ast_to_ratio_l5': () => 'Assists divided by turnovers - measures playmaking efficiency',
    'ast_to_ratio_l10': () => 'Assists divided by turnovers - measures playmaking efficiency',
    'ast_to_ratio_l20': () => 'Assists divided by turnovers - measures playmaking efficiency',
  };

  const tooltip = tooltips[featureName];
  if (!tooltip) return undefined;

  return typeof tooltip === 'function' ? tooltip(impactTier) : tooltip;
};


const convertImpactTier = (tier: string): 'positive' | 'negative' | 'neutral' => {
  if (tier.includes('positive')) return 'positive';
  if (tier.includes('negative')) return 'negative';
  return 'neutral';
};


const convertFeatureExplanations = (explanations: FeatureExplanation[] | undefined): StatFactor[] => {
  if (!explanations || explanations.length === 0) {
    return [];
  }

  return explanations.map((exp) => ({
    name: exp.description, 
    impact: convertImpactTier(exp.impact_tier),
    impactSymbol: exp.impact_symbol,
    value: `${exp.value.toFixed(2)}${exp.context}`, 
    weight: exp.importance_rank, 
    tooltip: getFeatureTooltip(exp.feature_name, exp.impact_tier),
  }));
};

const stats = [
  { label: 'Points', key: 'points', abbr: 'PTS' },
  { label: 'Rebounds', key: 'rebounds', abbr: 'REB' },
  { label: 'Assists', key: 'assists', abbr: 'AST' },
  { label: 'Steals', key: 'steals', abbr: 'STL' },
  { label: 'Blocks', key: 'blocks', abbr: 'BLK' },
  { label: 'Turnovers', key: 'turnovers', abbr: 'TO' },
  { label: '3-Pointers Made', key: 'threePointersMade', abbr: '3PM' },
] as const;

export function PlayerDetailModal({ prediction, open, onOpenChange }: PlayerDetailModalProps) {
  const navigate = useNavigate();
  const { selectedModels } = useEnsemble();
  const [expandedStats, setExpandedStats] = useState<string[]>([]);
  const [showActuals, setShowActuals] = useState(false);
  const [activeTab, setActiveTab] = useState<'features' | 'confidence'>('features');
  const { player, predictedStats, confidence, isHome, featureExplanations, actualStats, predictionError, gameDate, predictionId, playerId, gameId } = prediction;
  
  
  const { data: confidenceComponents = [], isLoading: isLoadingConfidence } = useConfidenceComponents(playerId, gameId, selectedModels);
  
  const hasActuals = actualStats !== undefined && actualStats !== null;
  
  
  const isPastGame = useMemo(() => {
    if (!gameDate) return false;
    const gameDateObj = new Date(gameDate);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    gameDateObj.setHours(0, 0, 0, 0);
    return gameDateObj < today;
  }, [gameDate]);
  
  const canShowActuals = isPastGame && hasActuals;
  
  
  const canShowAnalyzePlayer = !canShowActuals;
  
  
  const handleAnalyzePlayer = () => {
    if (gameDate) {
      const gameDateObj = new Date(gameDate);
      sessionStorage.setItem('shared-selected-date', gameDateObj.toISOString());
    }
    localStorage.setItem('player-analysis-selected-game', gameId);
    localStorage.setItem('player-analysis-selected-player', playerId);
    localStorage.setItem('player-analysis-selected-stat', 'points');
    onOpenChange(false);
    navigate('/dashboard/player-analysis');
  };

  const toggleStat = (statKey: string) => {
    setExpandedStats(prev =>
      prev.includes(statKey) ? prev.filter(s => s !== statKey) : [...prev, statKey]
    );
  };

  const getImpactIcon = (impact: 'positive' | 'negative' | 'neutral') => {
    switch (impact) {
      case 'positive':
        return <TrendingUp className="h-3 w-3 text-green-400" />;
      case 'negative':
        return <TrendingDown className="h-3 w-3 text-red-400" />;
      default:
        return <Minus className="h-3 w-3 text-muted-foreground" />;
    }
  };


  
  const formatConfidenceExplanation = (component: typeof confidenceComponents[0], statName: string) => {
    const explanations: Array<{ label: string; value: string; impact: 'positive' | 'negative' | 'neutral'; description?: string }> = [];
    
    
    if (component.n_models === 1) {
      
      explanations.push({
        label: 'Model Agreement',
        value: `0.0 / 25 - Single model selected (no agreement to measure)`,
        description: 'When only one model is selected, there\'s no agreement to measure between models. This component is set to 0.',
        impact: 'neutral' as const
      });
    } else if (component.ensemble_score > 0) {
      const score = component.ensemble_score;
      let description = '';
      if (score >= 20) description = `Excellent model agreement (${component.n_models} models)`;
      else if (score >= 15) description = `Good model agreement (${component.n_models} models)`;
      else if (score >= 10) description = `Moderate model agreement (${component.n_models} models)`;
      else description = `Low model agreement (${component.n_models} models)`;
      explanations.push({
        label: 'Model Agreement',
        value: `${score.toFixed(1)} / 25 - ${description}`,
        description: 'Measures how much our different AI models agree on this prediction. Higher agreement means more reliable.',
        impact: score >= 15 ? 'positive' : score >= 10 ? 'neutral' : 'negative'
      });
    }
    
    
    if (component.variance_score > 0) {
      const score = component.variance_score;
      const maxScore = component.n_models === 1 ? 30 : 25; 
      const isLowCountStat = statName === 'steals' || statName === 'blocks' || statName === 'three_pointers_made';
      let description = '';
      if (score >= 20) description = 'Very consistent performance across stats';
      else if (score >= 15) description = 'Consistent performance';
      else if (score >= 10) description = 'Moderate consistency';
      else description = isLowCountStat 
        ? 'Inconsistent performance (normal for low-count stats like steals/blocks)'
        : 'Inconsistent performance';
      
      const bonusNote = component.n_models === 1 ? ' (single-model bonus applied)' : '';
      explanations.push({
        label: 'Performance Consistency',
        value: `${score.toFixed(1)} / ${maxScore} - ${description}${bonusNote}`,
        description: component.n_models === 1 
          ? 'Measures how consistent the player\'s recent performance has been. Single-model predictions receive a +5 bonus (max 30 instead of 25) to account for the lack of ensemble agreement.'
          : 'Measures how consistent the player\'s recent performance has been. More consistent = more predictable.',
        impact: score >= 15 ? 'positive' : score >= 10 ? 'neutral' : 'negative'
      });
    }
    
    
    if (component.feature_score > 0) {
      const score = component.feature_score;
      let description = '';
      if (score >= 12) description = 'Complete feature data available';
      else if (score >= 9) description = 'Most features available';
      else if (score >= 6) description = 'Some features missing';
      else description = 'Significant features missing';
      explanations.push({
        label: 'Data Completeness',
        value: `${score.toFixed(1)} / 15 - ${description}`,
        description: 'Measures how much historical data we have for this player. More data = better predictions.',
        impact: score >= 12 ? 'positive' : score >= 9 ? 'neutral' : 'negative'
      });
    }
    
    
    if (component.experience_score > 0) {
      const score = component.experience_score;
      let description = '';
      if (score >= 12) description = 'Veteran player with extensive experience';
      else if (score >= 8) description = 'Experienced player';
      else if (score >= 5) description = 'Moderate experience';
      else description = 'Limited experience';
      explanations.push({
        label: 'Player Experience',
        value: `${score.toFixed(1)} / 15 - ${description}`,
        description: 'Based on games played this season and career. More experience = more predictable patterns.',
        impact: score >= 12 ? 'positive' : score >= 8 ? 'neutral' : 'negative'
      });
    }
    
    
    if (component.transaction_score > 0) {
      const score = component.transaction_score;
      let description = '';
      if (score >= 12) description = 'Stable team situation';
      else if (score >= 8) description = 'Recently joined team';
      else if (score >= 4) description = 'Very recent transaction';
      else description = 'Just traded/signed';
      explanations.push({
        label: 'Team Stability',
        value: `${score.toFixed(1)} / 15 - ${description}`,
        description: 'Measures how long the player has been with their current team. Recent trades reduce confidence.',
        impact: score >= 12 ? 'positive' : score >= 8 ? 'neutral' : 'negative'
      });
    }
    
    
    if (component.opponent_adj !== 0) {
      const adj = component.opponent_adj;
      let description = '';
      if (adj <= -4) description = 'Elite defensive opponent';
      else if (adj <= -2) description = 'Above-average defensive opponent';
      else if (adj >= 2) description = 'Below-average defensive opponent';
      else if (adj >= 4) description = 'Poor defensive opponent';
      else description = 'Average defensive opponent';
      explanations.push({
        label: 'Opponent Defense',
        value: `${adj > 0 ? '+' : ''}${adj.toFixed(1)} - ${description}`,
        description: 'Adjusts confidence based on opponent\'s defensive rating. Better defense = harder to predict.',
        impact: adj >= 2 ? 'positive' : adj <= -2 ? 'negative' : 'neutral'
      });
    }
    
    
    if (component.injury_adj < 0) {
      const adj = component.injury_adj;
      let description = '';
      if (adj <= -6) description = 'Recently returned from injury (≤2 games)';
      else if (adj <= -4) description = 'Recently returned (3-5 games)';
      else if (adj <= -1) description = 'Recently returned (6-10 games)';
      else description = 'Fully recovered';
      explanations.push({
        label: 'Injury Recovery',
        value: `${adj.toFixed(1)} - ${description}`,
        description: 'Players returning from injury have more unpredictable performance initially.',
        impact: 'negative'
      });
    }
    
    
    if (component.playoff_adj < 0) {
      explanations.push({
        label: 'Game Type',
        value: `${component.playoff_adj.toFixed(1)} - Playoff game (higher variance)`,
        description: 'Playoff games have more variance due to increased intensity and different rotations.',
        impact: 'negative'
      });
    }
    
    
    if (component.back_to_back_adj < 0) {
      explanations.push({
        label: 'Rest Factor',
        value: `${component.back_to_back_adj.toFixed(1)} - Back-to-back game`,
        description: 'Playing on consecutive days can affect performance due to fatigue.',
        impact: 'negative'
      });
    }
    
    return explanations;
  };

  
  const getFactorsForStat = (statKey: string): StatFactor[] => {
    if (!featureExplanations) {
      return [];
    }

    
    const dbKeyMap: Record<string, keyof typeof featureExplanations> = {
      points: 'points',
      rebounds: 'rebounds',
      assists: 'assists',
      steals: 'steals',
      blocks: 'blocks',
      turnovers: 'turnovers',
      threePointersMade: 'three_pointers_made' as any, 
    };

    
    let dbKey = dbKeyMap[statKey];
    let explanations = featureExplanations[dbKey];

    
    if (!explanations && statKey === 'threePointersMade') {
      explanations = featureExplanations['threePointersMade' as keyof typeof featureExplanations];
    }

    if (!explanations || explanations.length === 0) {
      return [];
    }

    return convertFeatureExplanations(explanations);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent 
        className="max-w-2xl max-h-[85vh] overflow-hidden bg-gradient-to-br from-background via-background to-secondary/20 border-border/50"
        style={{
          
          '--density-padding': '0.5rem',
          '--density-gap': '0.5rem',
          '--density-spacing': '0.75rem',
          '--density-card-padding': '0.75rem',
          '--density-section-gap': '0.75rem',
        } as React.CSSProperties}
      >
        <DialogHeader className="pb-3 border-b border-border/30">
          <div className="flex items-center gap-3">
            <div className="relative shrink-0">
              <div className="w-16 h-16 shrink-0 rounded-full overflow-hidden ring-2 ring-primary/50 bg-gradient-to-br from-secondary via-muted to-secondary">
                <img
                  src={player.photoUrl}
                  alt={player.name}
                  className="w-full h-full object-cover object-top"
                  onError={(e) => {
                    e.currentTarget.src = '/player-placeholder.png';
                  }}
                />
              </div>
              <div className="absolute inset-0 rounded-full bg-primary/20 blur-xl -z-10" />
            </div>

            <div className="flex-1 min-w-0">
              <DialogTitle className="text-xl font-bold bg-gradient-to-r from-foreground to-foreground/70 bg-clip-text text-transparent truncate">
                {player.name}
              </DialogTitle>
              <p className="text-sm text-muted-foreground truncate leading-tight">{player.team} • {player.position}</p>
              <div className="flex items-center gap-2 mt-2 flex-wrap">
                <ConfidenceBadge confidence={confidence} size="sm" />
                <span className={cn(
                  'inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-medium border',
                  isHome
                    ? 'bg-gradient-to-r from-accent/20 to-primary/20 text-accent border-accent/30'
                    : 'bg-gradient-to-r from-warning/20 to-warning/10 text-warning border-warning/30'
                )}>
                  <MapPin className="h-3 w-3 shrink-0" />
                  <span className="whitespace-nowrap">{isHome ? 'HOME' : 'AWAY'}</span>
                </span>
                {canShowActuals && (
                  <button
                    onClick={() => setShowActuals(!showActuals)}
                    className={cn(
                      'inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium border transition-colors cursor-pointer',
                      showActuals
                        ? 'bg-gradient-to-r from-primary/20 to-primary/10 text-primary border-primary/30 hover:from-primary/30 hover:to-primary/20'
                        : 'bg-gradient-to-r from-muted/20 to-muted/10 text-muted-foreground border-muted/30 hover:from-muted/30 hover:to-muted/20'
                    )}
                  >
                    <Eye className="h-3 w-3 shrink-0" />
                    <span className="whitespace-nowrap">{showActuals ? 'Hide Actuals' : 'View Actuals'}</span>
                  </button>
                )}
                {canShowAnalyzePlayer && (
                  <button
                    onClick={handleAnalyzePlayer}
                    className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium border transition-colors cursor-pointer bg-gradient-to-r from-primary/20 to-primary/10 text-primary border-primary/30 hover:from-primary/30 hover:to-primary/20"
                  >
                    <BarChart3 className="h-3 w-3 shrink-0" />
                    <span className="whitespace-nowrap">Analyze Player</span>
                  </button>
                )}
              </div>
            </div>
          </div>
        </DialogHeader>

        {showActuals && actualStats && (
          <div className="mb-0 p-3 rounded-lg border border-primary/30 bg-gradient-to-br from-primary/10 via-primary/5 to-transparent">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-lg font-semibold text-foreground">Actual Performance</h3>
              {predictionError !== undefined && (
                <div className="flex items-center gap-3">
                  <span className="text-xs text-muted-foreground">Error: <span className="font-medium text-foreground">{predictionError.toFixed(2)}</span></span>
                </div>
              )}
            </div>
            <div className="grid grid-cols-7 gap-2">
              {stats.map(({ label, key, abbr }) => {
                const predicted = predictedStats[key];
                const actual = actualStats[key];
                const diff = actual - predicted;
                const diffClass = diff > 0 ? 'text-success' : diff < 0 ? 'text-destructive' : 'text-muted-foreground';
                const diffSign = diff > 0 ? '+' : '';
                
                return (
                  <div key={key} className="text-center">
                    <div className="text-xs text-muted-foreground mb-1">{abbr}</div>
                    <div className="text-lg font-bold text-success">{actual.toFixed(1)}</div>
                    <div className={cn("text-xs font-medium", diffClass)}>
                      {diffSign}{diff.toFixed(1)}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as 'features' | 'confidence')} className="w-full">
          <TabsList className="grid w-full grid-cols-2 mb-4">
            <TabsTrigger value="features">Feature Explanations</TabsTrigger>
            <TabsTrigger value="confidence">Confidence Breakdown</TabsTrigger>
          </TabsList>

          <TabsContent value="features" className="mt-0">
            <div className="overflow-y-auto max-h-[calc(85vh-280px)] pr-2 -mr-2 space-y-2 pb-4">
              {stats.map(({ label, key, abbr }) => {
            const rawValue = predictedStats[key];
            const value = typeof rawValue === 'number' ? rawValue.toFixed(1) : rawValue;
            const factors = getFactorsForStat(key);
            const isExpanded = expandedStats.includes(key);

            return (
              <Collapsible key={key} open={isExpanded} onOpenChange={() => toggleStat(key)}>
                <CollapsibleTrigger className="w-full">
                  <div className={cn(
                    'flex items-center justify-between p-2 rounded-lg border transition-all duration-200',
                    isExpanded
                      ? 'bg-gradient-to-r from-primary/10 to-accent/10 border-primary/30'
                      : 'bg-gradient-to-r from-secondary/50 to-muted/30 border-border/30 hover:border-primary/30 hover:bg-gradient-to-r hover:from-primary/5 hover:to-accent/5'
                  )}>
                    <div className="flex items-center gap-3">
                      <span className="text-xs font-bold text-muted-foreground w-10">{abbr}</span>
                      <span className="font-semibold text-foreground">{label}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-lg font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
                        {value}
                      </span>
                      {isExpanded ? (
                        <ChevronUp className="h-4 w-4 text-muted-foreground" />
                      ) : (
                        <ChevronDown className="h-4 w-4 text-muted-foreground" />
                      )}
                    </div>
                  </div>
                </CollapsibleTrigger>

                <CollapsibleContent>
                  <div className="mt-1.5 ml-4 p-2 rounded-lg bg-gradient-to-br from-background to-secondary/10 border border-border/20">
                    <div className="flex items-center gap-2 mb-2">
                      <p className="text-xs text-muted-foreground font-medium">
                        Prediction Factors ({factors.length})
                      </p>
                      <Popover>
                        <PopoverTrigger asChild>
                          <button className="text-muted-foreground hover:text-foreground transition-colors" type="button">
                            <HelpCircle className="h-3.5 w-3.5" />
                          </button>
                        </PopoverTrigger>
                        <PopoverContent className="w-64 p-3" align="start">
                          <div className="space-y-2">
                            <p className="text-xs font-semibold text-foreground mb-2">Impact Symbols:</p>
                            <div className="space-y-1.5 text-xs">
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-green-400">+++</span>
                                <span className="text-muted-foreground">Very positive impact</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-green-400">++</span>
                                <span className="text-muted-foreground">Positive impact</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-green-400">+</span>
                                <span className="text-muted-foreground">Slightly positive</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-muted-foreground">=</span>
                                <span className="text-muted-foreground">Neutral (near average)</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-red-400">-</span>
                                <span className="text-muted-foreground">Slightly negative</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-red-400">--</span>
                                <span className="text-muted-foreground">Negative impact</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="font-mono font-bold text-red-400">---</span>
                                <span className="text-muted-foreground">Very negative impact</span>
                              </div>
                            </div>
                            <p className="text-xs text-muted-foreground mt-2 pt-2 border-t border-border/50">
                              Symbols indicate how each feature compares to league average and its importance to the prediction.
                            </p>
                          </div>
                        </PopoverContent>
                      </Popover>
                    </div>
                    <div className="grid gap-2">
                      {factors.map((factor, idx) => (
                        <div
                          key={idx}
                          className="flex items-center justify-between p-3 rounded-md bg-gradient-to-r from-muted/20 to-transparent hover:from-muted/30 transition-colors border border-border/10"
                        >
                          <div className="flex items-center gap-3 flex-1 min-w-0">
                            {getImpactIcon(factor.impact)}
                            <div className="flex flex-col min-w-0 flex-1">
                              <div className="flex items-center gap-1.5">
                                <span className="text-sm font-medium text-foreground truncate">{factor.name}</span>
                                {factor.tooltip && (
                                  <Popover>
                                    <PopoverTrigger asChild>
                                      <button
                                        className="text-muted-foreground hover:text-foreground transition-colors flex-shrink-0"
                                        type="button"
                                        onClick={(e) => e.stopPropagation()}
                                      >
                                        <HelpCircle className="h-3.5 w-3.5" />
                                      </button>
                                    </PopoverTrigger>
                                    <PopoverContent className="w-72 p-3" align="start">
                                      <p className="text-xs text-muted-foreground leading-relaxed">
                                        {factor.tooltip}
                                      </p>
                                    </PopoverContent>
                                  </Popover>
                                )}
                              </div>
                              {factor.value && (
                                <span className="text-xs text-muted-foreground mt-0.5">{factor.value}</span>
                              )}
                            </div>
                          </div>
                          <div className="flex items-center gap-2 flex-shrink-0 ml-4">
                            {factor.impactSymbol && (
                              <span className={cn(
                                'text-xs font-mono font-bold px-1.5 py-0.5 rounded',
                                factor.impact === 'positive' && 'bg-green-500/20 text-green-400',
                                factor.impact === 'negative' && 'bg-red-500/20 text-red-400',
                                factor.impact === 'neutral' && 'bg-muted/50 text-muted-foreground'
                              )}>
                                {factor.impactSymbol}
                              </span>
                            )}
                            {factor.weight > 0 && (
                            <span className={cn(
                                'text-xs font-semibold px-2 py-1 rounded',
                              factor.impact === 'positive' && 'bg-green-500/20 text-green-400',
                              factor.impact === 'negative' && 'bg-red-500/20 text-red-400',
                                factor.impact === 'neutral' && 'bg-muted/50 text-muted-foreground'
                            )}>
                              {factor.weight}%
                            </span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </CollapsibleContent>
              </Collapsible>
            );
          })}
            </div>
          </TabsContent>

          <TabsContent value="confidence" className="mt-0">
            <div className="overflow-y-auto max-h-[calc(85vh-280px)] pr-2 -mr-2 space-y-2 pb-4">
              {isLoadingConfidence ? (
                <div className="text-center py-8 text-muted-foreground">Loading confidence breakdown...</div>
              ) : confidenceComponents.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">No confidence breakdown available</div>
              ) : (
                <>
                  <div className="mb-4 p-3 rounded-lg border border-primary/20 bg-gradient-to-br from-primary/10 via-primary/5 to-transparent">
                    <div className="flex items-start gap-2">
                      <HelpCircle className="h-4 w-4 text-primary mt-0.5 flex-shrink-0" />
                      <div className="flex-1">
                        <p className="text-xs font-semibold text-foreground mb-1">Understanding Confidence Scores</p>
                        <p className="text-xs text-muted-foreground leading-relaxed">
                          Confidence scores (0-100%) measure how reliable a prediction is based on data quality, model agreement, and player consistency. 
                          Higher confidence doesn't guarantee accuracy, but indicates better conditions for making an accurate prediction. 
                          Each statistic has its own confidence score based on factors specific to that stat.
                        </p>
                      </div>
                    </div>
                  </div>
                  {stats.map(({ label, key, abbr }) => {
                  const component = confidenceComponents.find(c => {
                    const statMap: Record<string, string> = {
                      points: 'points',
                      rebounds: 'rebounds',
                      assists: 'assists',
                      steals: 'steals',
                      blocks: 'blocks',
                      turnovers: 'turnovers',
                      threePointersMade: 'three_pointers_made',
                    };
                    const dbStatName = statMap[key];
                    return c.stat_name === dbStatName || c.stat_name === key;
                  });

                  if (!component) return null;

                  const explanations = formatConfidenceExplanation(component, key);
                  const isExpanded = expandedStats.includes(key);

                  return (
                    <Collapsible key={key} open={isExpanded} onOpenChange={() => toggleStat(key)}>
                      <CollapsibleTrigger className="w-full">
                        <div className={cn(
                          'flex items-center justify-between p-2 rounded-lg border transition-all duration-200',
                          isExpanded
                            ? 'bg-gradient-to-r from-primary/10 to-accent/10 border-primary/30'
                            : 'bg-gradient-to-r from-secondary/50 to-muted/30 border-border/30 hover:border-primary/30 hover:bg-gradient-to-r hover:from-primary/5 hover:to-accent/5'
                        )}>
                          <div className="flex items-center gap-3">
                            <span className="text-xs font-bold text-muted-foreground w-10">{abbr}</span>
                            <span className="font-semibold text-foreground">{label}</span>
                          </div>
                          <div className="flex items-center gap-3">
                            <span className="text-sm font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
                              {component.calibrated_score.toFixed(0)}% confidence
                            </span>
                            {isExpanded ? (
                              <ChevronUp className="h-4 w-4 text-muted-foreground" />
                            ) : (
                              <ChevronDown className="h-4 w-4 text-muted-foreground" />
                            )}
                          </div>
                        </div>
                      </CollapsibleTrigger>

                      <CollapsibleContent>
                        <div className="mt-1.5 ml-4 p-2 rounded-lg bg-gradient-to-br from-background to-secondary/10 border border-border/20">
                          <div className="flex items-center gap-2 mb-2">
                            <p className="text-xs text-muted-foreground font-medium">
                              Confidence Components ({explanations.length})
                            </p>
                          </div>
                          <div className="grid gap-2">
                            {explanations.map((explanation, idx) => (
                              <Popover key={idx}>
                                <PopoverTrigger asChild>
                                  <div
                                    className="flex items-center justify-between p-3 rounded-md bg-gradient-to-r from-muted/20 to-transparent hover:from-muted/30 transition-colors border border-border/10 cursor-help"
                                  >
                                    <div className="flex items-center gap-3 flex-1 min-w-0">
                                      {getImpactIcon(explanation.impact)}
                                      <div className="flex flex-col min-w-0 flex-1">
                                        <div className="flex items-center gap-1.5">
                                          <span className="text-sm font-medium text-foreground">{explanation.label}</span>
                                          <HelpCircle className="h-3 w-3 text-muted-foreground flex-shrink-0" />
                                        </div>
                                        <span className="text-xs text-muted-foreground mt-0.5">{explanation.value}</span>
                                      </div>
                                    </div>
                                  </div>
                                </PopoverTrigger>
                                {explanation.description && (
                                  <PopoverContent className="w-72 p-3" align="start">
                                    <div className="space-y-2">
                                      <p className="text-xs font-semibold text-foreground">{explanation.label}</p>
                                      <p className="text-xs text-muted-foreground leading-relaxed">
                                        {explanation.description}
                                      </p>
                                    </div>
                                  </PopoverContent>
                                )}
                              </Popover>
                            ))}
                            <div className="mt-2 pt-2 border-t border-border/20">
                              <div className="flex items-center justify-between text-xs">
                                <span className="text-muted-foreground">Raw Score:</span>
                                <span className="font-semibold text-foreground">{component.raw_score.toFixed(1)}</span>
                              </div>
                              <div className="flex items-center justify-between text-xs mt-1">
                                <span className="text-muted-foreground">Final Confidence:</span>
                                <span className="font-bold text-primary">{component.calibrated_score.toFixed(0)}%</span>
                              </div>
                            </div>
                          </div>
                        </div>
                      </CollapsibleContent>
                    </Collapsible>
                  );
                }).filter(Boolean)}
                </>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
