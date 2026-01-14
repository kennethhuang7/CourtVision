CREATE OR REPLACE FUNCTION delete_user_account(user_id_to_delete UUID)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  DELETE FROM message_reactions WHERE user_id = user_id_to_delete;
  
  DELETE FROM message_reactions 
  WHERE message_id IN (
    SELECT id FROM user_messages WHERE sender_id = user_id_to_delete
    UNION
    SELECT id FROM group_messages WHERE sender_id = user_id_to_delete
  );
  
  DELETE FROM pick_user_shares 
  WHERE pick_id IN (SELECT id FROM user_picks WHERE owner_id = user_id_to_delete);
  
  DELETE FROM pick_group_shares 
  WHERE pick_id IN (SELECT id FROM user_picks WHERE owner_id = user_id_to_delete);
  
  DELETE FROM pick_user_shares WHERE shared_with_user_id = user_id_to_delete;
  
  DELETE FROM user_group_members WHERE user_id = user_id_to_delete;
  
  DELETE FROM group_messages 
  WHERE group_id IN (SELECT id FROM user_groups WHERE owner_id = user_id_to_delete);
  
  DELETE FROM user_group_members 
  WHERE group_id IN (SELECT id FROM user_groups WHERE owner_id = user_id_to_delete);
  
  DELETE FROM user_group_invites 
  WHERE group_id IN (SELECT id FROM user_groups WHERE owner_id = user_id_to_delete);
  
  DELETE FROM pick_group_shares 
  WHERE group_id IN (SELECT id FROM user_groups WHERE owner_id = user_id_to_delete);
  
  DELETE FROM user_groups WHERE owner_id = user_id_to_delete;
  
  DELETE FROM user_messages WHERE sender_id = user_id_to_delete;
  
  DELETE FROM group_messages WHERE sender_id = user_id_to_delete;
  
  DELETE FROM user_conversations WHERE user_id = user_id_to_delete;
  
  DELETE FROM user_group_invites 
  WHERE inviter_id = user_id_to_delete OR invitee_id = user_id_to_delete;
  
  DELETE FROM user_friendships 
  WHERE requester_id = user_id_to_delete OR addressee_id = user_id_to_delete;
  
  DELETE FROM user_picks WHERE owner_id = user_id_to_delete;
  
  DELETE FROM user_notifications WHERE user_id = user_id_to_delete;
  
  DELETE FROM user_sessions WHERE user_id = user_id_to_delete;
  
  DELETE FROM user_profiles WHERE user_id = user_id_to_delete;
END;
$$;

GRANT EXECUTE ON FUNCTION delete_user_account(UUID) TO authenticated;
