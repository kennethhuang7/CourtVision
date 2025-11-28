ALTER TABLE injuries ADD COLUMN IF NOT EXISTS dnp_reason VARCHAR(50);

ALTER TABLE games ADD COLUMN IF NOT EXISTS is_national_tv BOOLEAN DEFAULT FALSE;
ALTER TABLE games ADD COLUMN IF NOT EXISTS tv_broadcaster VARCHAR(50);

DROP TABLE IF EXISTS shot_chart_data CASCADE;
CREATE TABLE shot_chart_data (
    shot_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    game_id VARCHAR(10) NOT NULL REFERENCES games(game_id),
    period INTEGER,
    minutes_remaining INTEGER,
    seconds_remaining INTEGER,
    shot_made BOOLEAN NOT NULL,
    shot_type VARCHAR(50),
    shot_distance INTEGER,
    loc_x INTEGER,
    loc_y INTEGER,
    shot_zone_basic VARCHAR(50),
    shot_zone_area VARCHAR(50),
    shot_zone_range VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS referee_stats CASCADE;
CREATE TABLE referee_stats (
    ref_stat_id SERIAL PRIMARY KEY,
    referee_id INTEGER REFERENCES referees(referee_id),
    season VARCHAR(7),
    games_officiated INTEGER DEFAULT 0,
    avg_fouls_per_game DECIMAL(5,2),
    avg_technical_fouls DECIMAL(5,2),
    home_foul_bias DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(referee_id, season)
);

DROP TABLE IF EXISTS team_ratings CASCADE;
CREATE TABLE team_ratings (
    rating_id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    season VARCHAR(7),
    rating_date DATE,
    elo_rating DECIMAL(8,2),
    offensive_rating DECIMAL(6,2),
    defensive_rating DECIMAL(6,2),
    net_rating DECIMAL(6,2),
    win_pct DECIMAL(5,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shot_chart_player ON shot_chart_data(player_id);
CREATE INDEX IF NOT EXISTS idx_shot_chart_game ON shot_chart_data(game_id);
CREATE INDEX IF NOT EXISTS idx_referee_stats_season ON referee_stats(season);
CREATE INDEX IF NOT EXISTS idx_team_ratings_team_date ON team_ratings(team_id, rating_date);