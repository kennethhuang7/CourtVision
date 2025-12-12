<div align="center">

<h1>NBA Player Performance Prediction</h1>

**Machine Learning System for NBA Player Statistics Forecasting**

An ensemble-based prediction system using XGBoost, LightGBM, CatBoost, and Random Forest to forecast points, rebounds, assists, steals, blocks, turnovers, and three-pointers made for upcoming NBA games.

</div>

---

<details open="open">
<summary><strong>Table of Contents</strong></summary>

    <ol>
        <li><a href="#overview">Overview</a></li>
        <li><a href="#feature-building-process">Feature Building Process</a>
            <ul>
                <li><a href="#data-sources">Data Sources</a></li>
                <li><a href="#player-performance-features">Player Performance Features</a></li>
                <li><a href="#game-context-features">Game Context Features</a></li>
                <li><a href="#team-and-opponent-features">Team and Opponent Features</a></li>
                <li><a href="#imputation-strategy">Imputation Strategy</a></li>
            </ul>
        </li>
        <li><a href="#model-training-process">Model Training Process</a>
            <ul>
                <li><a href="#target-statistics">Target Statistics</a></li>
                <li><a href="#model-architectures">Model Architectures</a></li>
                <li><a href="#cross-validation-strategy">Cross-Validation Strategy</a></li>
                <li><a href="#loss-functions">Loss Functions</a></li>
                <li><a href="#model-persistence">Model Persistence</a></li>
            </ul>
        </li>
        <li><a href="#prediction-process">Prediction Process</a>
            <ul>
                <li><a href="#prediction-workflow">Prediction Workflow</a></li>
                <li><a href="#ensemble-averaging">Ensemble Averaging</a></li>
                <li><a href="#fallback-hierarchy">Fallback Hierarchy</a></li>
                <li><a href="#confidence-scoring">Confidence Scoring</a></li>
            </ul>
        </li>
        <li><a href="#complete-feature-reference">Complete Feature Reference</a></li>
    </ol>

</details>

---

## Overview

This system predicts NBA player statistics for upcoming games by:

1. **Building 150+ features** from historical player, team, and opponent data
2. **Training separate models** for each statistic (points, rebounds, assists, etc.)
3. **Combining predictions** from four different algorithms via ensemble averaging
4. **Generating confidence scores** and feature-based explanations for each prediction

**Key Design Principles:**
- Strict data leakage prevention (all features use only pre-game data)
- Contextual imputation based on feature type
- Temporal cross-validation respecting season boundaries
- Specialized loss functions for rare-event statistics

### Code Organization

| Component | File Location |
|-----------|---------------|
| Feature Building | `src/feature_engineering/build_features.py` |
| Team Stats Calculator | `src/feature_engineering/team_stats_calculator.py` |
| Model Training | `src/models/train_{model_type}.py` |
| Hyperparameter Tuning | `src/models/tune_hyperparameters.py` |
| Selective Tuning Config | `src/models/selective_tuning_config.py` |
| Predictions | `src/predictions/predict_games.py` |
| Feature Explanations | `src/predictions/feature_explanations.py` |
| Ensemble Utilities | `src/predictions/ensemble_utils.py` |

---

## Feature Building Process

The feature engineering pipeline transforms raw game statistics into predictive features. All calculations use **only historical data available before each game** to prevent data leakage.

**Training vs. Prediction:**
- **Training:** `build_features_for_training()` processes all historical games at once, creating a complete feature matrix for model training
- **Prediction:** `build_features_for_player()` calculates features for a single player/game combination in real-time
- Both use **identical feature engineering logic** to ensure consistency between training and inference

### Data Sources

| Table | Description |
|-------|-------------|
| `player_game_stats` | Individual game statistics for each player |
| `games` | Game metadata (date, season, teams, status) |
| `players` | Player information (position, team) |
| `teams` | Team information (timezone, arena altitude) |
| `team_ratings` | Pre-calculated team offensive/defensive ratings and pace |
| `team_defensive_stats` | Team-level defensive statistics |
| `position_defense_stats` | Position-specific defensive statistics |

---

### Player Performance Features

<details>
<summary><strong>Rolling Averages (Unweighted)</strong></summary>

    Simple moving averages over the last N games (5, 10, or 20).

    **Statistics calculated:**
    - `points`, `rebounds_total`, `assists`, `steals`, `blocks`, `turnovers`, `three_pointers_made`

    **Feature naming:** `{stat}_l{window}` (e.g., `points_l5`, `rebounds_total_l10`)

    **Data leakage prevention:** Uses `.shift(1)` to exclude the current game

    **Minimum periods:** `min_periods=1` allows calculation even with fewer than window games (important for early-season predictions)

    ```python
    # Formula
    stat_l{window} = mean(stat[t-window:t-1])  # where t is current game
    ```

</details>

<details>
<summary><strong>Exponentially Weighted Rolling Averages</strong></summary>

    Same statistics as unweighted, but with exponential decay weighting where recent games receive higher weights.

    **Decay factor:** 0.1

    ```python
    # Weight calculation
    weight[i] = exp(-0.1 * (window - i - 1))  # for game i in window
    weights = weights / weights.sum()  # normalize
    ```

    **Feature naming:** `{stat}_l{window}_weighted` (e.g., `points_l5_weighted`)

    **Effect:** A game from 5 games ago has ~60% the weight of the most recent game; a game from 20 games ago has ~14% the weight.

</details>

<details>
<summary><strong>Minutes Played Features</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `minutes_played_l{5,10,20}` | Average minutes in recent games |
    | `minutes_played_l{5,10,20}_weighted` | Exponentially weighted average |
    | `minutes_trend` | Linear trend slope over last 10 games (via `np.polyfit`) |

    **Minutes trend:** Returns 0.0 if insufficient variance or games. Positive values indicate increasing playing time.

</details>

<details>
<summary><strong>Per-36 Minute Rate Features</strong></summary>

    Normalizes production to a standard 36-minute game for fair comparison across players with different minutes loads.

    ```python
    # Formula
    {stat}_per_36_l{window} = (sum(stat) / sum(minutes_played)) * 36
    ```

    **Division safety:** Returns 0 if no minutes played (prevents division by zero).

    **Statistics:** All main stats (points, rebounds, assists, steals, blocks, turnovers, three-pointers)

</details>

<details>
<summary><strong>Shooting Percentage Features</strong></summary>

    | Feature | Calculation |
    |---------|-------------|
    | `fg_pct_l{5,10,20}` | `sum(FGM) / sum(FGA)` |
    | `three_pct_l{5,10,20}` | `sum(3PM) / sum(3PA)` |
    | `ft_pct_l{5,10,20}` | `sum(FTM) / sum(FTA)` |
    | `true_shooting_pct_l{5,10,20}` | Rolling average of pre-calculated TS% |

    **Division safety:** All percentage features return 0 when attempts = 0.

</details>

<details>
<summary><strong>Cross-Stat Ratio Features</strong></summary>

    | Feature | Calculation | Edge Case |
    |---------|-------------|-----------|
    | `ast_to_ratio_l{5,10,20}` | `sum(assists) / sum(turnovers)` | Returns assists if TOV = 0 |
    | `pts_per_fga_l{5,10,20}` | `sum(points) / sum(FGA)` | Returns 0 if FGA = 0 |
    | `pts_per_ast_l{5,10,20}` | `sum(points) / sum(assists)` | Returns points if AST = 0 |
    | `reb_rate_l{5,10,20}` | `sum(rebounds) / (sum(minutes) / 36)` | Returns 0 if minutes = 0 |

</details>

<details>
<summary><strong>Usage Rate & Advanced Stats</strong></summary>

    **Usage Rate:**
    | Feature | Description |
    |---------|-------------|
    | `usage_rate_l{5,10,20}` | Average usage rate (% of team plays used) |
    | `usage_rate_l{5,10,20}_weighted` | Exponentially weighted average |

    **Offensive/Defensive Ratings:**
    | Feature | Description |
    |---------|-------------|
    | `offensive_rating_l{5,10,20}` | Points produced per 100 possessions |
    | `defensive_rating_l{5,10,20}` | Points allowed per 100 possessions |
    | `net_rating_l{5,10,20}` | Offensive rating minus defensive rating |

    **Starter Status:**
    | Feature | Description |
    |---------|-------------|
    | `is_starter_l{5,10}` | Proportion of recent games started (0.0 to 1.0) |

</details>

---

### Game Context Features

<details>
<summary><strong>Home/Away & Rest</strong></summary>

    | Feature | Description | Values/Default |
    |---------|-------------|----------------|
    | `is_home` | Home game indicator | 0 or 1 |
    | `days_rest` | Days since last game | Integer (default: 3 for first game) |
    | `is_back_to_back` | Playing on consecutive days | 0 or 1 |
    | `is_well_rested` | 3+ days rest | 0 or 1 |

</details>

<details>
<summary><strong>Schedule Density</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `games_in_last_3_days` | Count of games in last 3 days |
    | `games_in_last_7_days` | Count of games in last 7 days |
    | `is_heavy_schedule` | ≥4 games in last 7 days |
    | `consecutive_games` | Current consecutive game streak |

</details>

<details>
<summary><strong>Season Period</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `season_progress` | Normalized progress (0.0 to 1.0) |
    | `games_played_season` | Games played in current season |
    | `is_early_season` | ≤20 games played |
    | `is_mid_season` | 21-60 games played |
    | `is_late_season` | >60 games played |
    | `games_remaining` | 82 minus team games played |

</details>

<details>
<summary><strong>Playoff Features</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `is_playoff` | Playoff game indicator |
    | `playoff_games_career` | Cumulative career playoff games |
    | `playoff_performance_boost` | `avg(playoff_points) - avg(regular_season_points)` |

</details>

<details>
<summary><strong>Travel & Altitude</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `tz_difference` | Timezone offset difference (opponent - player team) |
    | `west_to_east` | Traveling west to east (harder adjustment) |
    | `east_to_west` | Traveling east to west |
    | `arena_altitude` | Opponent's arena altitude in feet |
    | `altitude_away` | Away game at altitude >3000 feet (e.g., Denver, Utah) |

    **Timezone offsets:**
    - America/New_York: -5
    - America/Chicago: -6
    - America/Denver: -7
    - America/Los_Angeles: -8
    - Default fallback: -6 (Central)

</details>

<details>
<summary><strong>All-Star Break</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `days_since_asb` | Days since All-Star break (clipped to [-365, 365]) |
    | `post_asb_bounce` | 0-14 days after All-Star break |

    **Hardcoded ASB dates:** 2021-03-07, 2022-02-20, 2023-02-19, 2024-02-18, 2025-02-16, 2026-02-15

</details>

<details>
<summary><strong>Teammate Dependency</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `star_teammate_out` | Star teammate (≥20 PPG, ≥15 MPG) is out |
    | `star_teammate_ppg` | PPG of the missing star |
    | `games_without_star` | Cumulative games without star teammate |

</details>

---

### Team and Opponent Features

<details>
<summary><strong>Team Ratings (Calculated As-Of Game Date)</strong></summary>

    All team statistics are calculated using **only games played before the target date**.

    **Player's Team:**
    | Feature | Description |
    |---------|-------------|
    | `offensive_rating_team` | Points per 100 possessions |
    | `defensive_rating_team` | Points allowed per 100 possessions |
    | `pace_team` | Possessions per game |

    **Opponent Team:**
    | Feature | Description |
    |---------|-------------|
    | `offensive_rating_opp` | Opponent's offensive rating |
    | `defensive_rating_opp` | Opponent's defensive rating |
    | `pace_opp` | Opponent's pace |

    **Calculation:**
    ```python
    possessions = FGA - OREB + TOV + 0.44 * FTA
    offensive_rating = (points_for / possessions) * 100
    defensive_rating = (points_against / possessions) * 100
    pace = possessions / game_count
    ```

</details>

<details>
<summary><strong>Opponent Defensive Stats</strong></summary>

    **Team-Level:**
    | Feature | Description | Default |
    |---------|-------------|---------|
    | `opp_field_goal_pct` | Opponent's allowed FG% | 0.45 |
    | `opp_three_point_pct` | Opponent's allowed 3P% | 0.35 |
    | `opp_team_turnovers_per_game` | Opponent's turnovers forced/game | 14.0 |
    | `opp_team_steals_per_game` | Opponent's steals/game | 7.0 |

</details>

<details>
<summary><strong>Position-Specific Opponent Defense</strong></summary>

    Player position is mapped to: **Guard (G)**, **Forward (F)**, or **Center (C)**

    **Position mapping logic:**
    - "CENTER" or "C" → Center (unless contains "GUARD" or "FORWARD")
    - "FORWARD", "F", "F-C" → Forward
    - "GUARD", "G", "G-F", or unknown → Guard (default)

    | Feature | Description |
    |---------|-------------|
    | `opp_points_allowed_to_position` | Points allowed to player's position |
    | `opp_rebounds_allowed_to_position` | Rebounds allowed to position |
    | `opp_assists_allowed_to_position` | Assists allowed to position |
    | `opp_blocks_allowed_to_position` | Blocks allowed to position |
    | `opp_three_pointers_allowed_to_position` | 3PM allowed to position |
    | `opp_position_turnovers_vs_team` | Turnovers forced from position vs player's team |
    | `opp_position_steals_vs_team` | Steals from position vs player's team |
    | `opp_position_turnovers_overall` | Season avg turnovers forced from position |
    | `opp_position_steals_overall` | Season avg steals from position |

</details>

<details>
<summary><strong>Position Encoding (One-Hot)</strong></summary>

    | Feature | Description |
    |---------|-------------|
    | `position_guard` | 1 if guard, 0 otherwise |
    | `position_forward` | 1 if forward, 0 otherwise |
    | `position_center` | 1 if center, 0 otherwise |

</details>

---

### Imputation Strategy

The imputation strategy uses **contextual imputation based on feature type**, applied identically during training and prediction.

> **Critical:** The imputation strategy is **identical** between training and prediction to ensure consistency. This prevents distribution shift and ensures models see the same data format at training and inference time.

<details>
<summary><strong>Imputation Hierarchy (Click to Expand)</strong></summary>

    **Order of operations:**
    1. Calculate league means for all features from training data
    2. Apply tier-based imputation for each feature
    3. Final `fillna(0)` for any remaining NaN values

    | Tier | Feature Type | Imputation Value | Rationale |
    |------|--------------|------------------|-----------|
    | 1 | Team/Opponent/Pace features | **League mean** | Team-level stats should reflect typical NBA values |
    | 2 | Binary indicators (`is_*`) | **0** | Default to negative case |
    | 3 | Position encoding (`position_*`) | **0** | Default to no position match |
    | 4 | Trend features (`*_trend`, travel indicators) | **0** | No trend / neutral |
    | 5 | Player-specific features | **Expanding player mean** → **League mean** | Use player's own history first |
    | 6 | Any remaining NaN | **0** | Final safety net |

    **Tier 5 Detail (Player-Specific Features):**
    ```python
    # Calculate player's expanding mean up to current point
    player_mean = df.groupby('player_id')[col].transform(
        lambda x: x.expanding().mean().shift(1)  # .shift(1) prevents leakage
    )
    # Fill NaN with player mean, then league mean for remaining NaN
    X[col] = X[col].fillna(player_mean).fillna(league_mean)
    ```

</details>

<details>
<summary><strong>Feature Type Detection</strong></summary>

    ```python
    # Tier 1: Team/Opponent/Pace
    if 'team' in col or 'opp' in col or 'pace' in col:
        fill_value = league_mean[col]

    # Tier 2 & 3: Binary and Position
    elif col.startswith('is_') or col.startswith('position_'):
        fill_value = 0

    # Tier 4: Trend features
    elif 'trend' in col or col in ['west_to_east', 'east_to_west', 'post_asb_bounce']:
        fill_value = 0

    # Tier 5: Player-specific (everything else)
    else:
        fill_value = player_expanding_mean → league_mean
    ```

</details>

---

## Model Training Process

### Target Statistics

Models are trained **separately** for each of these seven prediction targets:

| Target | Database Column | Description |
|--------|-----------------|-------------|
| Points | `points` | Total points scored |
| Rebounds | `rebounds_total` | Total rebounds |
| Assists | `assists` | Total assists |
| Steals | `steals` | Total steals |
| Blocks | `blocks` | Total blocks |
| Turnovers | `turnovers` | Total turnovers |
| Three-Pointers Made | `three_pointers_made` | Three-pointers made |

<details>
<summary><strong>Why Raw Statistics Are Excluded From Features</strong></summary>

    Raw stats (e.g., `points`, `rebounds_total`, `minutes_played`) are explicitly excluded from the feature set:

    - Raw stats would create **perfect correlation** with targets
    - This would cause **data leakage**: the model would essentially "cheat" by seeing the answer
    - Instead, we use **derived features** (rolling averages, per-36 rates) that capture patterns without direct access to the target

    **Excluded columns:**
    ```python
    raw_leakage_cols = [
        'offensive_rating', 'defensive_rating', 'usage_rate', 'true_shooting_pct',
        'points', 'rebounds_total', 'assists', 'steals', 'blocks', 'turnovers',
        'three_pointers_made', 'minutes_played', 'is_starter', 'field_goals_made',
        'field_goals_attempted', 'three_pointers_attempted', 'free_throws_made',
        'free_throws_attempted'
    ]
    ```

</details>

---

### Data Preprocessing

**Minimum Games Requirement:**
- **Training:** Requires ≥5 games of history (rows with NaN in `points_l5` or `points_l10` are dropped)
- **Prediction:** Requires ≥5 games in current season (returns None if insufficient)
- This ensures all rolling features can be calculated with meaningful historical context
- Early-season games and rookies with insufficient history are excluded

---

### Model Architectures

Four gradient boosting and ensemble models are trained for each target statistic. The hyperparameters shown below are the **defaults** used when tuned parameters are not available or not selected.

<details>
<summary><strong>XGBoost</strong></summary>

    **Type:** Gradient boosting with regularization

    **Default hyperparameters:**
    ```python
    XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        objective='reg:squarederror'  # or 'count:poisson' for steals/blocks
    )
    ```

</details>

<details>
<summary><strong>LightGBM</strong></summary>

    **Type:** Histogram-based gradient boosting (leaf-wise growth)

    **Default hyperparameters:**
    ```python
    LGBMRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        objective='regression'  # or 'poisson' for steals/blocks
    )
    ```

</details>

<details>
<summary><strong>CatBoost</strong></summary>

    **Type:** Gradient boosting optimized for categorical features

    **Default hyperparameters:**
    ```python
    CatBoostRegressor(
        iterations=100,
        depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bylevel=0.8,
        random_seed=42,
        thread_count=-1,
        loss_function='RMSE'  # or 'Poisson' for steals/blocks
    )
    ```

</details>

<details>
<summary><strong>Random Forest</strong></summary>

    **Type:** Bagged ensemble of decision trees

    **Default hyperparameters:**
    ```python
    RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    ```

</details>

---

### Cross-Validation Strategy

The system uses **season-based temporal cross-validation** to prevent data leakage across time.

```python
if n_seasons >= 3:
    # Season-based splits
    for i, test_season in enumerate(unique_seasons[2:], start=2):
        train_seasons = unique_seasons[:i]  # All previous seasons
        # Train on seasons [0, 1, ..., i-1], validate on season i
        # Example: Train on [2022-23, 2023-24], validate on 2024-25
else:
    # Fallback for limited data
    tscv = TimeSeriesSplit(n_splits=3)
```

**Key properties:**
- Training data **always precedes** validation data chronologically
- Each fold trains on **all prior seasons** and validates on the next
- Prevents future data from leaking into training
- Best model selected by **lowest validation MAE**

---

### Loss Functions

Different loss functions are used based on the statistical distribution of each target:

| Target | XGBoost | LightGBM | CatBoost | Rationale |
|--------|---------|----------|----------|-----------|
| Points | `reg:squarederror` | `regression` | `RMSE` | Standard regression |
| Rebounds | `reg:squarederror` | `regression` | `RMSE` | Standard regression |
| Assists | `reg:squarederror` | `regression` | `RMSE` | Standard regression |
| Turnovers | `reg:squarederror` | `regression` | `RMSE` | Standard regression |
| Three-Pointers | `reg:squarederror` | `regression` | `RMSE` | Standard regression |
| **Steals** | `count:poisson` | `poisson` | `Poisson` | Rare count events |
| **Blocks** | `count:poisson` | `poisson` | `Poisson` | Rare count events |

**Why Poisson for Steals and Blocks?**
- Rare events (typically 0-3 per game) with right-skewed distribution
- Poisson naturally handles count data where variance ≈ mean
- Prevents negative predictions
- Better captures distribution of low-count statistics

---

### Model Persistence

**Saved artifacts per model/target combination:**

| File | Description |
|------|-------------|
| `{model_type}_{target}.pkl` | Trained model (e.g., `xgboost_points.pkl`) |
| `scaler_{model_type}_{target}.pkl` | Fitted StandardScaler |
| `{model_type}_{target}_mae.txt` | Cross-validation MAE |
| `feature_importance_{model_type}_{target}.csv` | Feature importance rankings |

**Feature Scaling:**
```python
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
# Formula: (x - mean) / std → mean=0, std=1
```

The scaler is saved alongside each model to ensure identical scaling at prediction time.

---

### Selective Tuning Configuration

Not all tuned hyperparameters improve performance over defaults. The system uses `selective_tuning_config.py` to control which model/stat combinations use tuned hyperparameters.

<details>
<summary><strong>How Selective Tuning Works</strong></summary>

    **The Problem:**
    - Hyperparameter tuning (via Optuna) finds optimal parameters for each model/stat combination
    - However, tuned parameters sometimes **overfit** to the validation set
    - Some model/stat combinations perform **better with defaults**

    **The Solution:**
    - `selective_tuning_config.py` maintains a whitelist of combinations that benefit from tuning
    - Only whitelisted combinations use tuned parameters
    - All others fall back to robust default parameters

    **Example Configuration:**
    ```python
    # XGBoost: Only use tuned params for blocks/steals (Poisson targets)
    # CatBoost: Use tuned params for all stats
    # LightGBM: Use tuned params for points, rebounds, assists only
    # Random Forest: Use defaults for all (tuning showed minimal improvement)
    ```

    **Usage:**
    ```bash
    # Train with tuned params (where configured)
    python src/models/train_all_models.py --use-tuned-params

    # Train with defaults only
    python src/models/train_all_models.py
    ```

</details>

---

## Prediction Process

### Running Predictions

**Command Line Usage:**

To generate predictions for a specific date:
```bash
# All 4 models for today's date (if no date provided, uses current date)
python src/predictions/predict_games.py

# Single model (default: xgboost)
python src/predictions/predict_games.py 2024-12-15

# Specific model
python src/predictions/predict_games.py 2024-12-15 xgboost

# All 4 models (required for ensemble confidence calculation)
python src/predictions/predict_games.py 2024-12-15 --all

# Recalculate confidence scores only (requires existing predictions from all 4 models)
python src/predictions/predict_games.py 2024-12-15 --recalculate-only

# Enable variance diagnostic logging (shows first 10 players' CV breakdown)
python src/predictions/predict_games.py 2024-12-15 --all --diagnostic
python src/predictions/predict_games.py 2024-12-15 --recalculate-only --diagnostic
```

**Important:** To get predictions from all 4 models (XGBoost, LightGBM, CatBoost, Random Forest), you **must** use the `--all` flag. This generates separate prediction rows for each model in the database, which allows the Streamlit dashboard to create ensemble predictions by averaging across selected models.

**Confidence Recalculation:** When using `--all`, after all 4 models complete their predictions, the system automatically recalculates confidence scores using all available model predictions. This ensures the ensemble agreement component is correctly calculated based on all models, not just individual models. The recalculation process:
- Queries all predictions for the target date from all 4 models
- Groups by player/game combination
- Rebuilds features and recalculates confidence with complete ensemble data
- Updates both `predictions.confidence_score` and `confidence_components` table
- Handles numpy type conversion (numpy.int64/numpy.float64 → Python int/float) for PostgreSQL compatibility

**Recalculation-Only Mode:** The `--recalculate-only` flag allows you to recalculate confidence scores for a date without re-running model predictions. This is useful when:
- Predictions already exist but confidence scores need updating (e.g., after parameter adjustments)
- Debugging confidence calculation issues
- Re-evaluating confidence after database updates

**Diagnostic Mode:** The `--diagnostic` flag enables detailed variance diagnostic logging for the first 10 players processed. This shows per-stat breakdowns (mean, std, CV, score, weight, weighted contribution) to help analyze variance component calculations. Diagnostic mode is off by default.

**Note:** Predictions will only be generated for games that are labeled as `'scheduled'` in the database. Games with other statuses (e.g., `'completed'`, `'in_progress'`, `'postponed'`) will be skipped.

### Prediction Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. LOAD UPCOMING GAMES                                         │
│     Query scheduled games for target date                       │
│     Extract: game_id, teams, season, game_type                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  2. IDENTIFY ELIGIBLE PLAYERS                                   │
│     • ≥5 games in current season                                │
│     • Not injured (status ≠ "Out")                              │
│     • Include recently traded players                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  3. BUILD FEATURES FOR EACH PLAYER                              │
│     • Query last 20 games (before target_date)                  │
│     • Requires ≥5 games in current season (excludes rookies)    │
│     • Calculate all rolling features                            │
│     • Fetch team/opponent stats (as-of target_date)             │
│     • Apply imputation hierarchy                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  4. APPLY MODELS                                                │
│     • Load trained models and scalers                           │
│     • Scale features using saved scalers                        │
│     • Generate predictions (clamped to ≥0)                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  5. ENSEMBLE AVERAGING                                          │
│     prediction = mean(XGBoost, LightGBM, CatBoost, RandomForest)│
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  6. GENERATE CONFIDENCE SCORES & EXPLANATIONS                   │
│     • Calculate confidence (0-100) based on variance, history   │
│     • Generate top 10 feature impacts per statistic             │
│     • Note: Initial confidence calculated per-model             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  7. STORE PREDICTIONS                                           │
│     • Insert/update in database                                 │
│     • Save CSV backup                                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  8. RECALCULATE CONFIDENCE WITH ENSEMBLE DATA (--all only)      │
│     • After all 4 models complete, recalculate confidence       │
│     • Uses all model predictions for ensemble agreement         │
│     • Updates predictions and confidence_components tables       │
│     • Converts numpy types to Python native types for DB        │
└─────────────────────────────────────────────────────────────────┘
```

---

### Ensemble Averaging

**Simple Average (Default):**
```python
prediction = (XGBoost + LightGBM + CatBoost + RandomForest) / 4
```

**Why Ensemble?**
- Different models capture different patterns
- XGBoost: Strong regularization, handles overfitting
- LightGBM: Faster training, finds complex patterns
- CatBoost: Better with categorical features
- Random Forest: Robust to outliers, less overfitting

Averaging reduces variance because individual model errors tend to cancel out.

**Weighted Average (Available but not default):**
```python
# Weights by inverse validation MAE
weights = {model: 1.0 / validation_mae[model] for model in models}
prediction = sum(pred * weight for pred, weight in zip(predictions, weights))
```

> **Note:** The ensemble can be configured to include/exclude specific models. By default, all four models are included, but users can select a subset based on performance metrics via the Streamlit dashboard's Model Performance page.

---

### Fallback Hierarchy

<details>
<summary><strong>Team Statistics Fallback Chain</strong></summary>

    ```
    1. As-of-date calculation (games before target_date)
             ↓ (if insufficient data)
    2. Previous season's statistics
             ↓ (if no previous season)
    3. Current season's aggregate
             ↓ (if still no data)
    4. Default values:
       - offensive_rating: 105.0
       - defensive_rating: 105.0
       - pace: 100.0
    ```

</details>

<details>
<summary><strong>Opponent Defense Fallback Chain</strong></summary>

    Same hierarchy as team statistics:
    ```
    As-of-date → Previous season → Current season → Defaults
    ```

    **Default values:**
    | Feature | Default |
    |---------|---------|
    | `opp_field_goal_pct` | 0.45 |
    | `opp_three_point_pct` | 0.35 |
    | `opp_team_turnovers_per_game` | 14.0 |
    | `opp_team_steals_per_game` | 7.0 |
    | Position-specific stats | 0 |

</details>

<details>
<summary><strong>Player Feature Fallback Chain</strong></summary>

    ```
    1. Recent games data (if available)
       - points_l5 → recent_games['points'].mean()
             ↓ (if no recent games)
    2. Expanding player mean (all prior games)
             ↓ (if no player history)
    3. League mean from training data
             ↓ (if still NaN)
    4. Zero (final safety net)
    ```

</details>

---

### Confidence Scoring

Confidence scores (0-100) measure **prediction quality** — how favorable the conditions are for making an accurate prediction. The score represents the reliability of the prediction environment, NOT the probability of accuracy. 

**Confidence Score Philosophy:**
- A score of **90** means "excellent conditions: consistent player, models agree, good data, no recent trade"
- A score of **40** means "risky conditions: inconsistent player, models disagree, limited data"

**Score Range Interpretations:**

| Score Range | Interpretation | Conditions |
|-------------|----------------|------------|
| **85-100** | Excellent | Very consistent player, strong model agreement, complete data, experienced, stable situation |
| **70-84** | Good | Consistent player, good model agreement, mostly complete data, some experience |
| **55-69** | Average | Moderate consistency, acceptable model agreement, adequate data |
| **40-54** | Below Average | Inconsistent player, models disagree, limited data, or recent changes |
| **Below 40** | Poor | Very inconsistent, high model disagreement, missing critical data, or major uncertainty factors |

The system calculates **separate confidence scores for each of the 7 target statistics** (points, rebounds, assists, steals, blocks, turnovers, three-pointers made), then averages them to produce an overall confidence score stored in the `predictions` table. Detailed per-stat breakdowns are stored in the `confidence_components` table.

<details>
<summary><strong>Confidence Components (Click to Expand)</strong></summary>

    The confidence system uses a multi-component scoring approach with the following components:

    | Component | Points | Description |
    |-----------|--------|-------------|
    | **Ensemble Agreement** | 0-25 | Measures prediction consistency across selected models. Higher agreement = higher score. Scales by ensemble size: 4 models (25 pts), 3 models (20 pts), 2 models (15 pts), 1 model (0 pts). Formula: `25 / (1 + α * CV)` where CV is coefficient of variation. Parameters: α = 1.0, ε = 0.1 (division protection). |
    | **Multi-Stat Variance** | 0-25 (0-30 with single-model bonus) | Measures player consistency across all 7 statistics (or single stat for per-stat calculation). Lower coefficient of variation = higher score. Uses exponential decay: `25 * exp(-β * CV)` where CV is capped at 1.0. Parameter: β = 1.0. **CV cap:** CV is capped at 1.0 to prevent low-count stats (steals, blocks, three-pointers) from disproportionately penalizing variance scores, since these stats have naturally high CV (often 1.5-2.0+) due to their low means, not due to player unpredictability. Single-model predictions receive +5 bonus (capped at 30). **Stat weights** (for multi-stat aggregation): points/rebounds (1.0), assists/three-pointers (0.9), steals/blocks (0.8), turnovers (0.7). |
    | **Feature Completeness** | 0-15 | Importance-weighted availability of features. Missing critical feature groups incur penalties: -3 per missing critical group (rolling_windows, player_status). |
    | **Experience** | 0-15 | Based on season games (0-10 pts) and career games (0-5 pts). Season thresholds: ≥25 games (10 pts), ≥15 games (8 pts), ≥8 games (5 pts), ≥3 games (3 pts), else (1 pt). Career thresholds: ≥200 games (5 pts), ≥80 games (4 pts), ≥30 games (2 pts), else (1 pt). |
    | **Transaction** | 0-15 | Penalizes recent trades/signings. Days component (0-10 pts): ≤7 days = 0, 8-14 = 3, 15-21 = 6, >21 = 10. Games component (0-5 pts): ≤3 games = 0, 4-10 = 2, 11-20 = 4, >20 = 5. Max (15) reached at >21 days AND >20 games. |
    | **Opponent Adjustment** | -5 to +5 | Adjusts based on opponent's defensive rating relative to league average (114.0). Elite defense (≤-5 diff): -5, Above-average (-5 to 0): -2, Below-average (0 to +5): +2, Poor (>+5): +5. |
    | **Domain Adjustments** | Variable | Additional penalties: recent injury return (≤2 games: -8, 3-5 games: -5, 6-10 games: -2, >10: 0), playoff games (-5), back-to-back games (-3) |

    **Calculation Process:**
    1. Each component is calculated independently
    2. Raw score = sum of all components (theoretical max: 100 normally, 105 with single-model variance bonus)
    3. Raw score is clipped to [0, 105]
    4. Optional calibration via isotonic regression (future enhancement)
    5. Final score is rounded to integer [0, 100]

    **Per-Stat Confidence:**
    - Ensemble agreement and variance are calculated **per statistic** (using only that stat's predictions and variance)
    - Feature completeness, experience, transaction, opponent adjustment, and domain adjustments are **shared across all stats** (player-level factors)
    - Each stat receives its own confidence breakdown stored in `confidence_components` table
    - Overall confidence (stored in `predictions` table) is the average of all 7 stat confidences

    **Storage:**
    - Overall confidence score: `predictions.confidence_score` (0-100 integer)
    - Per-stat breakdowns: `confidence_components` table with columns for each component score, raw score, and calibrated score
    - Linked via `prediction_id` foreign key

    **Example Calculation:**
    
    Consider a player prediction with the following components (using updated parameters α = 1.0, β = 1.0):
    - **Ensemble Agreement:** 18 (3 models selected, moderate agreement, CV = 0.39, using α = 1.0: 25 / (1 + 1.0 * 0.39) = 18)
    - **Variance:** 19 (consistent player, CV = 0.27, using β = 1.0: 25 * exp(-1.0 * 0.27) = 19, no bonus since 3 models selected)
    - **Feature Completeness:** 12 (most features available, one critical group missing = -3 penalty from base score of 15)
    - **Experience:** 12 (20 season games = 8 pts, 150 career games = 4 pts)
    - **Transaction:** 15 (no recent transaction = 10 pts, >20 games with team = 5 pts)
    - **Opponent Adjustment:** -2 (above-average defense, opponent DR = 112.0, diff = -2 from league avg 114.0)
    - **Domain:** 0 (no injury, regular season, not back-to-back)
    
    Raw Score = 18 + 19 + 12 + 12 + 15 + (-2) + 0 = **74**
    Clipped to [0, 105]: **74**
    Final Confidence: **74** (0-100 scale) - **Good conditions** (70-84 range)
    
    **Note:** This example shows overall confidence. In practice, each of the 7 statistics receives its own confidence score (with stat-specific ensemble agreement and variance), and the overall confidence is the average of all 7.

</details>

---

### Feature Explanations

For each prediction, the top 10 most impactful features are identified with impact symbols:

| Symbol | Meaning |
|--------|---------|
| `+++` | Very positive impact (high importance, well above average) |
| `++` | Positive impact (above average) |
| `+` | Slightly positive impact |
| `=` | Neutral (near average) |
| `-` | Slightly negative impact |
| `--` | Negative impact (below average) |
| `---` | Very negative impact (high importance, well below average) |

**Calculation:**
1. Load feature importance from training
2. Compare feature value to league average
3. Assign symbol based on importance rank × deviation magnitude

**Storage:** Stored as JSONB in database `predictions` table (excluded from CSV exports due to verbosity)

---

### Edge Cases

<details>
<summary><strong>Special Handling (Click to Expand)</strong></summary>

    | Scenario | Handling |
    |----------|----------|
    | **Rookie players** | Excluded if <5 games; uses league averages for missing features if included |
    | **Newly traded players** | Included if ≥5 season games (with previous team); uses recent games from previous team for player features. **Warning:** System checks for transaction records in `player_transactions` table (past 7 days). If missing, a warning is displayed. Run `detect_and_update_trades.py` to update transaction records for proper confidence scoring. |
    | **Injured players** | Excluded if injury status = "Out"; confidence penalized if recently returned |
    | **Missing team statistics** | Fallback hierarchy (previous season → current season → defaults) |
    | **Feature ordering** | Features reordered at prediction time to match model's expected `feature_names_in_` |
    | **Database connection issues** | `ensure_connection()` implements automatic reconnection with retries |
    | **Numpy type conversion** | All numpy types (int64, float64) converted to Python native types (int, float) before database insertion to ensure PostgreSQL compatibility |
    | **Zero-variance stats** | Players with no historical data for certain stats (e.g., never made a three-pointer) use fallback CV=1.0 for variance calculation (no warnings logged) |

</details>

---

## Complete Feature Reference

<details>
<summary><strong>Full Feature List by Category (~150 features)</strong></summary>

    ### Rolling Averages (42 features)
    - `{stat}_l5`, `{stat}_l10`, `{stat}_l20` (7 stats × 3 windows = 21)
      - Simple moving average of stat over last N games (e.g., `points_l5` = average points in last 5 games)
    - `{stat}_l5_weighted`, `{stat}_l10_weighted`, `{stat}_l20_weighted` (21)
      - Exponentially weighted average (recent games weighted higher, decay factor 0.1)

    ### Minutes Features (7 features)
    - `minutes_played_l{5,10,20}` (3)
      - Average minutes played in last N games
    - `minutes_played_l{5,10,20}_weighted` (3)
      - Exponentially weighted average of minutes
    - `minutes_trend` (1)
      - Linear trend slope of minutes over last 10 games (positive = increasing playing time)

    ### Per-36 Rate Features (21 features)
    - `{stat}_per_36_l{5,10,20}` (7 stats × 3 windows)
      - Stat normalized to per-36-minute rate over last N games (e.g., `points_per_36_l5` = points per 36 min in last 5 games)

    ### Shooting Percentages (12 features)
    - `fg_pct_l{5,10,20}` (3)
      - Field goal percentage over last N games (sum(FGM) / sum(FGA))
    - `three_pct_l{5,10,20}` (3)
      - Three-point percentage over last N games (sum(3PM) / sum(3PA))
    - `ft_pct_l{5,10,20}` (3)
      - Free throw percentage over last N games (sum(FTM) / sum(FTA))
    - `true_shooting_pct_l{5,10,20}` (3)
      - True shooting percentage (accounts for 2PT, 3PT, FT) over last N games

    ### Ratio Features (12 features)
    - `ast_to_ratio_l{5,10,20}` (3)
      - Assist-to-turnover ratio over last N games (sum(assists) / sum(turnovers))
    - `pts_per_fga_l{5,10,20}` (3)
      - Points per field goal attempt over last N games (efficiency metric)
    - `pts_per_ast_l{5,10,20}` (3)
      - Points per assist over last N games
    - `reb_rate_l{5,10,20}` (3)
      - Rebound rate (rebounds per 36 minutes) over last N games

    ### Usage & Advanced (15 features)
    - `usage_rate_l{5,10,20}` (3)
      - Average usage rate (% of team plays used) over last N games
    - `usage_rate_l{5,10,20}_weighted` (3)
      - Exponentially weighted average usage rate
    - `offensive_rating_l{5,10,20}` (3)
      - Points produced per 100 possessions over last N games
    - `defensive_rating_l{5,10,20}` (3)
      - Points allowed per 100 possessions over last N games
    - `net_rating_l{5,10,20}` (3)
      - Net rating (offensive rating - defensive rating) over last N games

    ### Starter Status (2 features)
    - `is_starter_l5`, `is_starter_l10`
      - Proportion of recent games where player started (0.0 to 1.0)

    ### Team Ratings (6 features)
    - `offensive_rating_team`, `defensive_rating_team`, `pace_team`
      - Player's team: points per 100 possessions, points allowed per 100, possessions per game (calculated as-of game date)
    - `offensive_rating_opp`, `defensive_rating_opp`, `pace_opp`
      - Opponent team: same metrics for opposing team (calculated as-of game date)

    ### Opponent Defense (13+ features)
    - `opp_field_goal_pct`, `opp_three_point_pct`
      - Opponent's allowed field goal % and three-point % (team-level, calculated as-of game date)
    - `opp_team_turnovers_per_game`, `opp_team_steals_per_game`
      - Opponent's turnovers forced per game and steals per game (team-level)
    - `opp_points_allowed_to_position`, `opp_rebounds_allowed_to_position`
      - Points and rebounds allowed to player's position (Guard/Forward/Center) by opponent
    - `opp_assists_allowed_to_position`, `opp_blocks_allowed_to_position`
      - Assists and blocks allowed to player's position by opponent
    - `opp_three_pointers_allowed_to_position`
      - Three-pointers made allowed to player's position by opponent
    - `opp_position_turnovers_vs_team`, `opp_position_steals_vs_team`
      - Turnovers and steals forced from player's position in matchups vs player's team (matchup-specific)
    - `opp_position_turnovers_overall`, `opp_position_steals_overall`
      - Season average turnovers and steals forced from player's position by opponent (overall average)

    ### Position Encoding (3 features)
    - `position_guard`, `position_forward`, `position_center`
      - One-hot encoding: 1 if player is that position, 0 otherwise (defaults to Guard if unknown)

    ### Game Context (14 features)
    - `is_home`, `days_rest`, `is_back_to_back`, `is_well_rested`
      - Home game (1/0), days since last game, playing on consecutive days (1/0), 3+ days rest (1/0)
    - `games_in_last_3_days`, `games_in_last_7_days`
      - Count of games played in last 3 days and last 7 days
    - `is_heavy_schedule`, `consecutive_games`
      - ≥4 games in last 7 days (1/0), current consecutive game streak count
    - `games_played_season`, `season_progress`
      - Games played in current season (shifted to exclude current game), normalized season progress (0.0-1.0)
    - `is_early_season`, `is_mid_season`, `is_late_season`, `games_remaining`
      - Binary indicators for season period (≤20, 21-60, >60 games), remaining games in season

    ### Travel & Altitude Features (5 features)
    - `tz_difference`, `west_to_east`, `east_to_west`
      - Timezone offset difference (opponent - player team), traveling west-to-east (1/0), east-to-west (1/0)
    - `arena_altitude`, `altitude_away`
      - Opponent's arena altitude in feet, away game at altitude >3000 feet (1/0, e.g., Denver)

    ### All-Star Break (2 features)
    - `days_since_asb`, `post_asb_bounce`
      - Days since All-Star break (clipped to [-365, 365]), 0-14 days after ASB (1/0)

    ### Playoff Features (3 features)
    - `is_playoff`, `playoff_games_career`, `playoff_performance_boost`
      - Playoff game indicator (1/0), cumulative career playoff games, avg(playoff_points) - avg(regular_season_points)

    ### Teammate Dependency (3 features)
    - `star_teammate_out`, `star_teammate_ppg`, `games_without_star`
      - Star teammate (≥20 PPG, ≥15 MPG) is injured/out (1/0), PPG of missing star, cumulative games without star teammate

</details>

---

<div align="center">

**Questions?** Open an issue on GitHub

</div>