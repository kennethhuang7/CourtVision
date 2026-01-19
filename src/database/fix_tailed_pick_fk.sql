ALTER TABLE user_picks
DROP CONSTRAINT IF EXISTS user_picks_tailed_from_pick_id_fkey;

ALTER TABLE user_picks
ADD CONSTRAINT user_picks_tailed_from_pick_id_fkey
FOREIGN KEY (tailed_from_pick_id)
REFERENCES user_picks(id)
ON DELETE SET NULL;
