-- Add the `auth.invitation_relation` table recording the relation between an
-- invitation token and the account that registered through it.

CREATE TABLE auth.invitation_relation (
    id          bigserial PRIMARY KEY,
    invite_via  uuid NOT NULL REFERENCES auth.invitation (token) ON DELETE CASCADE,
    invitee     uuid NOT NULL REFERENCES auth.account (id) ON DELETE CASCADE,
    accepted_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX invitation_relation_invite_via_idx ON auth.invitation_relation (invite_via);
CREATE INDEX invitation_relation_invitee_idx ON auth.invitation_relation (invitee);
