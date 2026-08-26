-- Rotating a signing secret without a window of failed deliveries.
--
-- A single-column rotation is a cutover: the instant we start signing with the new
-- secret, every receiver still checking the old one rejects us, and every delivery
-- in flight fails. The customer cannot fix that by deploying faster, because there
-- is no ordering of the two changes that avoids it — whichever side moves first is
-- wrong until the other catches up.
--
-- So both secrets are held for an overlap window and both signatures are sent. The
-- receiver already accepts a comma-separated list and matches on any entry, so
-- nothing on their side changes at all: they update their secret whenever they like
-- inside the window, and no delivery fails on either side of the switch.
ALTER TABLE endpoints
    ADD COLUMN previous_secret text,
    -- When the old secret stops being sent. Read rather than swept: an expired
    -- previous secret is simply not selected, so a pruner that fell over cannot
    -- extend the window silently.
    ADD COLUMN previous_secret_expires_at timestamptz;

-- Both or neither. A previous secret with no expiry would be sent forever, which
-- turns a rotation into "now there are two live secrets" — the opposite of what a
-- rotation is for. An expiry with no secret is harmless but means the row lied.
ALTER TABLE endpoints
    ADD CONSTRAINT endpoints_previous_secret_check
    CHECK ((previous_secret IS NULL) = (previous_secret_expires_at IS NULL));
