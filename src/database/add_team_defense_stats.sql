DROP TABLE IF EXISTS team_defensive_stats CASCADE;
CREATE TABLE team_defensive_stats (
    stat_id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    season VARCHAR(7),
    stat_date DATE,
    games_played INTEGER,
    opp_points_per_game DECIMAL(6,2),
    opp_rebounds_per_game DECIMAL(6,2),
    opp_assists_per_game DECIMAL(6,2),
    opp_steals_per_game DECIMAL(6,2),
    opp_blocks_per_game DECIMAL(6,2),
    opp_turnovers_per_game DECIMAL(6,2),
    opp_fg_pct DECIMAL(5,3),
    opp_three_pt_pct DECIMAL(5,3),
    defensive_rating DECIMAL(6,2),
    defensive_rebound_pct DECIMAL(5,3),
    opponent_offensive_rebound_pct DECIMAL(5,3),
    rim_fg_pct_allowed DECIMAL(5,3),
    three_pt_fg_pct_allowed DECIMAL(5,3),
    mid_range_fg_pct_allowed DECIMAL(5,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season, stat_date)
);

DROP TABLE IF EXISTS position_defense_stats CASCADE;
CREATE TABLE position_defense_stats (
    pos_stat_id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    season VARCHAR(7),
    position VARCHAR(10),
    points_allowed_per_game DECIMAL(6,2),
    rebounds_allowed_per_game DECIMAL(6,2),
    assists_allowed_per_game DECIMAL(6,2),
    fg_pct_allowed DECIMAL(5,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season, position)
);

CREATE INDEX IF NOT EXISTS idx_team_def_stats ON team_defensive_stats(team_id, season);
CREATE INDEX IF NOT EXISTS idx_pos_def_stats ON position_defense_stats(team_id, season, position);