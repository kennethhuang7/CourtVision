ALTER TABLE teams ADD COLUMN IF NOT EXISTS latitude DECIMAL(10,6);
ALTER TABLE teams ADD COLUMN IF NOT EXISTS longitude DECIMAL(10,6);
ALTER TABLE teams ADD COLUMN IF NOT EXISTS timezone VARCHAR(50);

DROP TABLE IF EXISTS player_transactions CASCADE;
CREATE TABLE player_transactions (
    transaction_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    from_team_id INTEGER REFERENCES teams(team_id),
    to_team_id INTEGER REFERENCES teams(team_id),
    transaction_type VARCHAR(20),
    transaction_date DATE,
    season VARCHAR(7),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS team_rivalries CASCADE;
CREATE TABLE team_rivalries (
    rivalry_id SERIAL PRIMARY KEY,
    team_1_id INTEGER REFERENCES teams(team_id),
    team_2_id INTEGER REFERENCES teams(team_id),
    rivalry_strength INTEGER DEFAULT 5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_1_id, team_2_id)
);

DROP TABLE IF EXISTS player_career_stats CASCADE;
CREATE TABLE player_career_stats (
    career_stat_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    updated_date DATE,
    career_points INTEGER DEFAULT 0,
    career_rebounds INTEGER DEFAULT 0,
    career_assists INTEGER DEFAULT 0,
    career_steals INTEGER DEFAULT 0,
    career_blocks INTEGER DEFAULT 0,
    career_games INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, updated_date)
);

DROP TABLE IF EXISTS season_predictions CASCADE;
CREATE TABLE season_predictions (
    season_pred_id SERIAL PRIMARY KEY,
    season VARCHAR(7),
    prediction_type VARCHAR(50),
    prediction_date DATE,
    mvp_player_id INTEGER REFERENCES players(player_id),
    mvp_probability DECIMAL(5,3),
    roy_player_id INTEGER REFERENCES players(player_id),
    roy_probability DECIMAL(5,3),
    dpoy_player_id INTEGER REFERENCES players(player_id),
    dpoy_probability DECIMAL(5,3),
    champion_team_id INTEGER REFERENCES teams(team_id),
    champion_probability DECIMAL(5,3),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_player ON player_transactions(player_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON player_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_career_stats_player ON player_career_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_season_pred_season ON season_predictions(season);