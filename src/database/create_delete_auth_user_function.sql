CREATE OR REPLACE FUNCTION delete_auth_user(user_id_to_delete UUID)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = auth, public
AS $$
BEGIN
  DELETE FROM auth.users WHERE id = user_id_to_delete;
END;
$$;

GRANT EXECUTE ON FUNCTION delete_auth_user(UUID) TO authenticated;
