-- Add the missing `role` column to the `auth.account` table.
ALTER TABLE auth.account
    ADD COLUMN role auth.account_status NOT NULL DEFAULT 'Member';
