-- Remove the `role` column from the `auth.account` table.
ALTER TABLE auth.account
    DROP COLUMN role;
