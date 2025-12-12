from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
import numpy as np
import logging

__all__ = [
    'ConfidenceConfig',
    'CONFIDENCE_CONFIG',
    'calculate_ensemble_agreement',
    'calculate_multi_stat_variance',
    'reset_variance_diagnostic',
    'enable_variance_diagnostic',
    'calculate_feature_completeness',
    'calculate_experience_score',
    'calculate_transaction_score',
    'calculate_opponent_adjustment',
    'injury_adjustment',
    'playoff_adjustment',
    'back_to_back_adjustment',
    'ConfidenceBreakdown',
    'calculate_confidence_score',
    'calculate_confidence_score_per_stat',
]

@dataclass
class ConfidenceConfig:
    ensemble_max_points: Dict[int, int] = field(
        default_factory=lambda: {4: 25, 3: 20, 2: 15, 1: 0}
    )
    ensemble_alpha: float = 1.0
    ensemble_epsilon: float = 0.1
    
    variance_beta: float = 1.0
    variance_max_points: int = 25
    variance_single_model_bonus: int = 5
    stat_weights: Dict[str, float] = field(
        default_factory=lambda: {
            'points': 1.0,
            'rebounds': 1.0,
            'assists': 0.9,
            'three_pointers_made': 0.9,
            'steals': 0.8,
            'blocks': 0.8,
            'turnovers': 0.7
        }
    )
    
    feature_completeness_max: int = 15
    experience_max: int = 15
    transaction_max: int = 15
    opponent_adj_range: tuple = (-5, 5)
    
    experience_season_thresholds: Dict[str, int] = field(
        default_factory=lambda: {
            'high': 25,
            'medium': 15,
            'low': 8,
            'minimal': 3
        }
    )
    experience_career_thresholds: Dict[str, int] = field(
        default_factory=lambda: {
            'veteran': 200,
            'experienced': 80,
            'moderate': 30,
            'rookie': 0
        }
    )
    
    transaction_days_thresholds: Dict[str, int] = field(
        default_factory=lambda: {
            'very_recent': 7,
            'recent': 14,
            'moderate': 21,
            'old': 21
        }
    )
    transaction_games_thresholds: Dict[str, int] = field(
        default_factory=lambda: {
            'very_few': 3,
            'few': 10,
            'moderate': 20,
            'many': 20
        }
    )
    
    league_avg_def_rating: float = 114.0
    opponent_adj_thresholds: Dict[str, float] = field(
        default_factory=lambda: {
            'elite': -5,
            'above_avg': 0,
            'below_avg': 5,
            'poor': 5
        }
    )
    
    injury_penalties: Dict[str, int] = field(
        default_factory=lambda: {
            'very_recent': 2,
            'recent': 5,
            'moderate': 10,
            'old': 10
        }
    )
    playoff_penalty: int = -5
    back_to_back_penalty: int = -3

CONFIDENCE_CONFIG = ConfidenceConfig()


@dataclass
class ConfidenceBreakdown:
    ensemble_score: float = 0.0
    variance_score: float = 0.0
    feature_score: float = 0.0
    experience_score: float = 0.0
    transaction_score: float = 0.0
    opponent_adj: float = 0.0
    injury_adj: float = 0.0
    playoff_adj: float = 0.0
    back_to_back_adj: float = 0.0
    raw_score: float = 0.0
    calibrated_score: float = 0.0
    n_models: int = 0
    
    def to_dict(self) -> Dict:
        return {
            'ensemble_score': self.ensemble_score,
            'variance_score': self.variance_score,
            'feature_score': self.feature_score,
            'experience_score': self.experience_score,
            'transaction_score': self.transaction_score,
            'opponent_adj': self.opponent_adj,
            'injury_adj': self.injury_adj,
            'playoff_adj': self.playoff_adj,
            'back_to_back_adj': self.back_to_back_adj,
            'raw_score': self.raw_score,
            'calibrated_score': self.calibrated_score,
            'n_models': self.n_models
        }
    
    def log(self, logger: logging.Logger, player_id: int, game_id: int):
        logger.info(
            f"Confidence breakdown for player {player_id}, game {game_id}:\n"
            f"  Ensemble: {self.ensemble_score:.2f}\n"
            f"  Variance: {self.variance_score:.2f}\n"
            f"  Features: {self.feature_score:.2f}\n"
            f"  Experience: {self.experience_score:.2f}\n"
            f"  Transaction: {self.transaction_score:.2f}\n"
            f"  Opponent Adj: {self.opponent_adj:.2f}\n"
            f"  Injury Adj: {self.injury_adj:.2f}\n"
            f"  Playoff Adj: {self.playoff_adj:.2f}\n"
            f"  B2B Adj: {self.back_to_back_adj:.2f}\n"
            f"  Raw Score: {self.raw_score:.2f}\n"
            f"  Final Score: {self.calibrated_score:.2f}\n"
            f"  N Models: {self.n_models}"
        )


def calculate_ensemble_agreement(
    predictions_by_model: Dict[str, Dict[str, float]],
    selected_models: List[str],
    config: Optional[ConfidenceConfig] = None,
    stat_name: Optional[str] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    n_models = len(selected_models)
    max_points = config.ensemble_max_points.get(n_models, 0)
    
    if n_models <= 1:
        return 0.0
    
    if stat_name:
        stat_predictions = predictions_by_model.get(stat_name, {})
        preds = [stat_predictions.get(m) for m in selected_models 
                 if m in stat_predictions]
        preds = [p for p in preds if p is not None]
        
        if len(preds) < 2:
            return 0.0
        
        mean_pred = np.mean(preds)
        std_pred = np.std(preds, ddof=1)
        cv = std_pred / (abs(mean_pred) + config.ensemble_epsilon)
        
        score = 25 / (1 + config.ensemble_alpha * cv)
        return score * (max_points / 25)
    
    agreement_scores = []
    
    for stat in config.stat_weights.keys():
        stat_predictions = predictions_by_model.get(stat, {})
        preds = [stat_predictions.get(m) for m in selected_models 
                 if m in stat_predictions]
        preds = [p for p in preds if p is not None]
        
        if len(preds) < 2:
            continue
        
        mean_pred = np.mean(preds)
        std_pred = np.std(preds, ddof=1)
        cv = std_pred / (abs(mean_pred) + config.ensemble_epsilon)
        
        score = 25 / (1 + config.ensemble_alpha * cv)
        agreement_scores.append(score)
    
    if not agreement_scores:
        return 0.0
    
    median_score = np.median(agreement_scores)
    return median_score * (max_points / 25)


_variance_diagnostic_count = 0
_variance_diagnostic_max = 10
_variance_diagnostic_enabled = False
_logger = logging.getLogger(__name__)

def reset_variance_diagnostic():
    global _variance_diagnostic_count
    _variance_diagnostic_count = 0

def enable_variance_diagnostic():
    global _variance_diagnostic_enabled
    _variance_diagnostic_enabled = True

def calculate_multi_stat_variance(
    player_stats: Dict[str, Dict[str, float]],
    config: Optional[ConfidenceConfig] = None,
    stat_name: Optional[str] = None,
    player_id: Optional[int] = None,
    player_name: Optional[str] = None
) -> float:
    global _variance_diagnostic_count, _variance_diagnostic_enabled
    
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if stat_name:
        if stat_name not in player_stats:
            return config.variance_max_points / 2
        
        mean = player_stats[stat_name].get('mean', 0)
        std = player_stats[stat_name].get('std', 0)
        
        if mean <= 0:
            cv = 1.0
        else:
            cv = std / (mean + 0.1)
        
        cv = min(cv, 1.0)
        
        score = config.variance_max_points * np.exp(-config.variance_beta * cv)
        return score
    
    weighted_scores = []
    weights = []
    stat_details = []
    
    for stat, weight in config.stat_weights.items():
        if stat not in player_stats:
            continue
        
        mean = player_stats[stat].get('mean', 0)
        std = player_stats[stat].get('std', 0)
        
        if mean <= 0:
            cv = 1.0
        else:
            cv = std / (mean + 0.1)
        
        cv = min(cv, 1.0)
        
        score = config.variance_max_points * np.exp(-config.variance_beta * cv)
        weighted_score = score * weight
        
        weighted_scores.append(weighted_score)
        weights.append(weight)
        stat_details.append({
            'stat': stat,
            'mean': mean,
            'std': std,
            'cv': cv,
            'score': score,
            'weight': weight,
            'weighted_score': weighted_score
        })
    
    if not weights:
        return config.variance_max_points / 2
    
    final_score = sum(weighted_scores) / sum(weights)
    
    if _variance_diagnostic_enabled and _variance_diagnostic_count < _variance_diagnostic_max and player_id is not None:
        _variance_diagnostic_count += 1
        player_label = player_name if player_name else f"Player {player_id}"
        print(f"\n{'='*60}")
        print(f"VARIANCE DIAGNOSTIC: {player_label} (ID: {player_id})")
        print(f"{'='*60}")
        print(f"{'Stat':<20} {'Mean':<8} {'Std':<8} {'CV':<8} {'Score':<8} {'Weight':<8} {'Weighted':<8}")
        print(f"{'-'*60}")
        for detail in stat_details:
            print(
                f"{detail['stat']:<20} "
                f"{detail['mean']:<8.2f} "
                f"{detail['std']:<8.2f} "
                f"{detail['cv']:<8.2f} "
                f"{detail['score']:<8.2f} "
                f"{detail['weight']:<8.2f} "
                f"{detail['weighted_score']:<8.2f}"
            )
        print(f"{'-'*60}")
        print(f"FINAL weighted average: {final_score:.2f}")
        print(f"{'='*60}\n")
    
    return final_score


def calculate_feature_completeness(
    available_features: Set[str],
    feature_importances: Dict[str, float],
    feature_groups: Dict[str, List[str]],
    config: Optional[ConfidenceConfig] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    base_score = 0.0
    total_importance = sum(feature_importances.values())
    
    if total_importance > 0:
        for feature, importance in feature_importances.items():
            if feature in available_features:
                base_score += importance
        
        base_score = (base_score / total_importance) * config.feature_completeness_max
    else:
        if len(feature_importances) > 0:
            base_score = (len(available_features) / len(feature_importances)) * config.feature_completeness_max
    
    critical_groups = ['rolling_windows', 'player_status']
    penalty = 0
    
    for group in critical_groups:
        group_features = feature_groups.get(group, [])
        if len(group_features) == 0:
            continue
        
        available_in_group = [f for f in group_features if f in available_features]
        
        if len(available_in_group) == 0:
            penalty += 3
    
    return max(0, base_score - penalty)


def calculate_experience_score(
    games_this_season: int,
    career_games: int,
    config: Optional[ConfidenceConfig] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if games_this_season >= config.experience_season_thresholds['high']:
        season_score = 10
    elif games_this_season >= config.experience_season_thresholds['medium']:
        season_score = 8
    elif games_this_season >= config.experience_season_thresholds['low']:
        season_score = 5
    elif games_this_season >= config.experience_season_thresholds['minimal']:
        season_score = 3
    else:
        season_score = 1
    
    if career_games >= config.experience_career_thresholds['veteran']:
        career_score = 5
    elif career_games >= config.experience_career_thresholds['experienced']:
        career_score = 4
    elif career_games >= config.experience_career_thresholds['moderate']:
        career_score = 2
    else:
        career_score = 1
    
    return season_score + career_score


def calculate_transaction_score(
    days_since_transaction: Optional[int],
    games_with_team: int,
    transaction_type: Optional[str] = None,
    config: Optional[ConfidenceConfig] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if days_since_transaction is None:
        transaction_score = 10
    elif days_since_transaction <= config.transaction_days_thresholds['very_recent']:
        transaction_score = 0
    elif days_since_transaction <= config.transaction_days_thresholds['recent']:
        transaction_score = 3
    elif days_since_transaction <= config.transaction_days_thresholds['moderate']:
        transaction_score = 6
    else:
        transaction_score = 10
    
    if games_with_team <= config.transaction_games_thresholds['very_few']:
        games_score = 0
    elif games_with_team <= config.transaction_games_thresholds['few']:
        games_score = 2
    elif games_with_team <= config.transaction_games_thresholds['moderate']:
        games_score = 4
    else:
        games_score = 5
    
    return transaction_score + games_score


def calculate_opponent_adjustment(
    opponent_def_rating: float,
    league_avg_def_rating: Optional[float] = None,
    config: Optional[ConfidenceConfig] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if league_avg_def_rating is None:
        league_avg_def_rating = config.league_avg_def_rating
    
    delta_dr = opponent_def_rating - league_avg_def_rating
    
    if delta_dr <= -5:
        return -5
    elif delta_dr < 0:
        return -2
    elif delta_dr < 5:
        return +2
    else:
        return +5


def injury_adjustment(
    games_since_return: Optional[int],
    config: Optional[ConfidenceConfig] = None
) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if games_since_return is None:
        return 0
    
    if games_since_return <= config.injury_penalties['very_recent']:
        return -8
    elif games_since_return <= config.injury_penalties['recent']:
        return -5
    elif games_since_return <= config.injury_penalties['moderate']:
        return -2
    else:
        return 0


def playoff_adjustment(is_playoff: bool, config: Optional[ConfidenceConfig] = None) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    return config.playoff_penalty if is_playoff else 0


def back_to_back_adjustment(is_back_to_back: bool, config: Optional[ConfidenceConfig] = None) -> float:
    if config is None:
        config = CONFIDENCE_CONFIG
    return config.back_to_back_penalty if is_back_to_back else 0


def calculate_confidence_score(
    predictions_by_model: Dict[str, Dict[str, float]],
    selected_models: List[str],
    player_stats: Dict[str, Dict[str, float]],
    available_features: Set[str],
    feature_importances: Dict[str, float],
    feature_groups: Dict[str, List[str]],
    games_this_season: int,
    career_games: int,
    days_since_transaction: Optional[int],
    games_with_team: int,
    opponent_def_rating: float,
    calibrator: Optional['ConfidenceCalibrator'] = None,
    config: Optional[ConfidenceConfig] = None,
    logger: Optional[logging.Logger] = None,
    games_since_injury: Optional[int] = None,
    is_playoff: bool = False,
    is_back_to_back: bool = False,
    player_id: Optional[int] = None,
    game_id: Optional[int] = None
) -> Tuple[float, ConfidenceBreakdown]:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    breakdown = ConfidenceBreakdown()
    breakdown.n_models = len(selected_models)
    
    breakdown.ensemble_score = calculate_ensemble_agreement(
        predictions_by_model, selected_models, config=config
    )
    
    breakdown.variance_score = calculate_multi_stat_variance(
        player_stats, config=config, player_id=player_id, player_name=None
    )
    if breakdown.n_models == 1:
        breakdown.variance_score = min(
            config.variance_max_points + config.variance_single_model_bonus,
            breakdown.variance_score + config.variance_single_model_bonus
        )
    
    breakdown.feature_score = calculate_feature_completeness(
        available_features, feature_importances, feature_groups, config=config
    )
    
    breakdown.experience_score = calculate_experience_score(
        games_this_season, career_games, config=config
    )
    
    breakdown.transaction_score = calculate_transaction_score(
        days_since_transaction, games_with_team, config=config
    )
    
    breakdown.opponent_adj = calculate_opponent_adjustment(
        opponent_def_rating, config=config
    )
    
    breakdown.injury_adj = injury_adjustment(games_since_injury, config=config)
    breakdown.playoff_adj = playoff_adjustment(is_playoff, config=config)
    breakdown.back_to_back_adj = back_to_back_adjustment(is_back_to_back, config=config)
    
    breakdown.raw_score = (
        breakdown.ensemble_score +
        breakdown.variance_score +
        breakdown.feature_score +
        breakdown.experience_score +
        breakdown.transaction_score +
        breakdown.opponent_adj +
        breakdown.injury_adj +
        breakdown.playoff_adj +
        breakdown.back_to_back_adj
    )
    
    breakdown.raw_score = max(0, min(105, breakdown.raw_score))
    
    if calibrator and calibrator.is_fitted:
        breakdown.calibrated_score = calibrator.transform(breakdown.raw_score)
    else:
        breakdown.calibrated_score = breakdown.raw_score
    
    if player_id and game_id:
        breakdown.log(logger, player_id, game_id)
    
    return breakdown.calibrated_score, breakdown


def calculate_confidence_score_per_stat(
    stat_name: str,
    predictions_by_model: Dict[str, Dict[str, float]],
    selected_models: List[str],
    player_stats: Dict[str, Dict[str, float]],
    available_features: Set[str],
    feature_importances: Dict[str, float],
    feature_groups: Dict[str, List[str]],
    games_this_season: int,
    career_games: int,
    days_since_transaction: Optional[int],
    games_with_team: int,
    opponent_def_rating: float,
    calibrator: Optional['ConfidenceCalibrator'] = None,
    config: Optional[ConfidenceConfig] = None,
    logger: Optional[logging.Logger] = None,
    games_since_injury: Optional[int] = None,
    is_playoff: bool = False,
    is_back_to_back: bool = False,
    player_id: Optional[int] = None,
    game_id: Optional[int] = None,
    player_name: Optional[str] = None
) -> Tuple[float, ConfidenceBreakdown]:
    if config is None:
        config = CONFIDENCE_CONFIG
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    breakdown = ConfidenceBreakdown()
    breakdown.n_models = len(selected_models)
    
    breakdown.ensemble_score = calculate_ensemble_agreement(
        predictions_by_model, selected_models, config=config, stat_name=stat_name
    )
    
    breakdown.variance_score = calculate_multi_stat_variance(
        player_stats, config=config, stat_name=stat_name, player_id=player_id, player_name=None
    )
    if breakdown.n_models == 1:
        breakdown.variance_score = min(
            config.variance_max_points + config.variance_single_model_bonus,
            breakdown.variance_score + config.variance_single_model_bonus
        )
    
    breakdown.feature_score = calculate_feature_completeness(
        available_features, feature_importances, feature_groups, config=config
    )
    
    breakdown.experience_score = calculate_experience_score(
        games_this_season, career_games, config=config
    )
    
    breakdown.transaction_score = calculate_transaction_score(
        days_since_transaction, games_with_team, config=config
    )
    
    breakdown.opponent_adj = calculate_opponent_adjustment(
        opponent_def_rating, config=config
    )
    
    breakdown.injury_adj = injury_adjustment(games_since_injury, config=config)
    breakdown.playoff_adj = playoff_adjustment(is_playoff, config=config)
    breakdown.back_to_back_adj = back_to_back_adjustment(is_back_to_back, config=config)
    
    breakdown.raw_score = (
        breakdown.ensemble_score +
        breakdown.variance_score +
        breakdown.feature_score +
        breakdown.experience_score +
        breakdown.transaction_score +
        breakdown.opponent_adj +
        breakdown.injury_adj +
        breakdown.playoff_adj +
        breakdown.back_to_back_adj
    )
    
    breakdown.raw_score = max(0, min(105, breakdown.raw_score))
    
    if calibrator and calibrator.is_fitted:
        breakdown.calibrated_score = calibrator.transform(breakdown.raw_score)
    else:
        breakdown.calibrated_score = breakdown.raw_score
    
    if player_id and game_id:
        breakdown.log(logger, player_id, game_id)
    
    return breakdown.calibrated_score, breakdown




