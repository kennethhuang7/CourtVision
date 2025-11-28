DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS betting_lines CASCADE;
DROP TABLE IF EXISTS player_game_stats CASCADE;
DROP TABLE IF EXISTS games CASCADE;
DROP TABLE IF EXISTS injuries CASCADE;
DROP TABLE IF EXISTS referees CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    abbreviation VARCHAR(3) NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    arena_name VARCHAR(100),
    arena_altitude INTEGER DEFAULT 0,
    conference VARCHAR(10),
    division VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    team_id INTEGER REFERENCES teams(team_id),
    jersey_number VARCHAR(3),
    position VARCHAR(50),
    height_inches INTEGER,
    weight_lbs INTEGER,
    birth_date DATE,
    draft_year INTEGER,
    draft_round INTEGER,
    draft_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE referees (
    referee_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL UNIQUE,
    jersey_number VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE games (
    game_id VARCHAR(10) PRIMARY KEY,
    game_date DATE NOT NULL,
    season VARCHAR(7) NOT NULL,
    home_team_id INTEGER NOT NULL REFERENCES teams(team_id),
    away_team_id INTEGER NOT NULL REFERENCES teams(team_id),
    home_score INTEGER,
    away_score INTEGER,
    game_status VARCHAR(20) DEFAULT 'scheduled',
    attendance INTEGER,
    game_duration_minutes INTEGER,
    referee_1_id INTEGER REFERENCES referees(referee_id),
    referee_2_id INTEGER REFERENCES referees(referee_id),
    referee_3_id INTEGER REFERENCES referees(referee_id),
    home_pace DECIMAL(5,2),
    away_pace DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE player_game_stats (
    stat_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    game_id VARCHAR(10) NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    is_starter BOOLEAN DEFAULT FALSE,
    minutes_played DECIMAL(5,2),
    points INTEGER DEFAULT 0,
    rebounds_offensive INTEGER DEFAULT 0,
    rebounds_defensive INTEGER DEFAULT 0,
    rebounds_total INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    steals INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    turnovers INTEGER DEFAULT 0,
    personal_fouls INTEGER DEFAULT 0,
    field_goals_made INTEGER DEFAULT 0,
    field_goals_attempted INTEGER DEFAULT 0,
    three_pointers_made INTEGER DEFAULT 0,
    three_pointers_attempted INTEGER DEFAULT 0,
    free_throws_made INTEGER DEFAULT 0,
    free_throws_attempted INTEGER DEFAULT 0,
    plus_minus INTEGER,
    usage_rate DECIMAL(5,2),
    true_shooting_pct DECIMAL(5,4),
    offensive_rating DECIMAL(6,2),
    defensive_rating DECIMAL(6,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, game_id)
);

CREATE TABLE injuries (
    injury_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    injury_date DATE NOT NULL,
    injury_status VARCHAR(20) NOT NULL,
    injury_type VARCHAR(100),
    description TEXT,
    games_missed INTEGER DEFAULT 0,
    return_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE betting_lines (
    line_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    game_id VARCHAR(10) NOT NULL REFERENCES games(game_id),
    stat_type VARCHAR(20) NOT NULL,
    line_value DECIMAL(5,1) NOT NULL,
    over_odds INTEGER,
    under_odds INTEGER,
    sportsbook VARCHAR(50),
    line_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE predictions (
    prediction_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    game_id VARCHAR(10) NOT NULL REFERENCES games(game_id),
    prediction_date TIMESTAMP NOT NULL,
    predicted_points DECIMAL(5,2),
    predicted_rebounds DECIMAL(5,2),
    predicted_assists DECIMAL(5,2),
    predicted_steals DECIMAL(5,2),
    predicted_blocks DECIMAL(5,2),
    predicted_turnovers DECIMAL(5,2),
    predicted_minutes DECIMAL(5,2),
    confidence_score INTEGER,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, game_id, model_version)
);

CREATE INDEX idx_players_team ON players(team_id);
CREATE INDEX idx_players_active ON players(is_active);
CREATE INDEX idx_games_date ON games(game_date);
CREATE INDEX idx_games_season ON games(season);
CREATE INDEX idx_games_home_team ON games(home_team_id);
CREATE INDEX idx_games_away_team ON games(away_team_id);
CREATE INDEX idx_player_stats_player ON player_game_stats(player_id);
CREATE INDEX idx_player_stats_game ON player_game_stats(game_id);
CREATE INDEX idx_player_stats_date ON player_game_stats(game_id, player_id);
CREATE INDEX idx_injuries_player ON injuries(player_id);
CREATE INDEX idx_injuries_date ON injuries(injury_date);
CREATE INDEX idx_betting_player_game ON betting_lines(player_id, game_id);
CREATE INDEX idx_predictions_player_game ON predictions(player_id, game_id);