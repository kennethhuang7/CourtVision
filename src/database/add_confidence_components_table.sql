CREATE TABLE IF NOT EXISTS confidence_components (
    component_id SERIAL PRIMARY KEY,
    prediction_id INTEGER REFERENCES predictions(prediction_id),
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    prediction_date DATE NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    stat_name VARCHAR(50) NOT NULL,
    
    ensemble_score DECIMAL(6,2) DEFAULT 0,
    variance_score DECIMAL(6,2) DEFAULT 0,
    feature_score DECIMAL(6,2) DEFAULT 0,
    experience_score DECIMAL(6,2) DEFAULT 0,
    transaction_score DECIMAL(6,2) DEFAULT 0,
    opponent_adj DECIMAL(6,2) DEFAULT 0,
    injury_adj DECIMAL(6,2) DEFAULT 0,
    playoff_adj DECIMAL(6,2) DEFAULT 0,
    back_to_back_adj DECIMAL(6,2) DEFAULT 0,
    
    raw_score DECIMAL(6,2) NOT NULL,
    calibrated_score DECIMAL(6,2) NOT NULL,
    n_models INTEGER NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(prediction_id, stat_name)
);

CREATE INDEX IF NOT EXISTS idx_confidence_components_player_game 
    ON confidence_components(player_id, game_id);

CREATE INDEX IF NOT EXISTS idx_confidence_components_player_game_stat 
    ON confidence_components(player_id, game_id, stat_name);

CREATE INDEX IF NOT EXISTS idx_confidence_components_date 
    ON confidence_components(prediction_date);

CREATE INDEX IF NOT EXISTS idx_confidence_components_model 
    ON confidence_components(model_version);

CREATE INDEX IF NOT EXISTS idx_confidence_components_prediction 
    ON confidence_components(prediction_id);

CREATE INDEX IF NOT EXISTS idx_confidence_components_stat 
    ON confidence_components(stat_name);

