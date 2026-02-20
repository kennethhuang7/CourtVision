DROP INDEX IF EXISTS idx_team_ratings_team_season;
DROP INDEX IF EXISTS idx_pos_def_stats;

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN ('team_ratings', 'position_defense_stats', 'team_defensive_stats')
ORDER BY tablename, indexname;
