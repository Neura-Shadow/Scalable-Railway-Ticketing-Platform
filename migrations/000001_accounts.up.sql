BEGIN;

CREATE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL,
    password_hash text NOT NULL,
    role text NOT NULL DEFAULT 'customer'
        CHECK (role IN ('customer', 'admin', 'operator')),
    token_version bigint NOT NULL DEFAULT 1 CHECK (token_version > 0),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (email = lower(email)),
    CHECK (length(email) BETWEEN 3 AND 320),
    CHECK (length(password_hash) BETWEEN 20 AND 255)
);

CREATE UNIQUE INDEX users_email_unique_idx ON users (email);

CREATE TRIGGER users_set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE refresh_tokens (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    family_id uuid NOT NULL,
    jti_hash bytea NOT NULL UNIQUE CHECK (octet_length(jti_hash) = 32),
    token_version bigint NOT NULL CHECK (token_version > 0),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    rotated_to_id uuid REFERENCES refresh_tokens(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > created_at),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

CREATE INDEX refresh_tokens_user_family_idx
    ON refresh_tokens (user_id, family_id, created_at DESC);
CREATE INDEX refresh_tokens_active_expiry_idx
    ON refresh_tokens (expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE passengers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    display_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (length(btrim(display_name)) BETWEEN 1 AND 100)
);

CREATE INDEX passengers_user_idx ON passengers (user_id, created_at, id);

CREATE TRIGGER passengers_set_updated_at
BEFORE UPDATE ON passengers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
