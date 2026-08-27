-- An endpoint gets a transport.
--
-- Everything downstream of building a request is already shared — retries, backoff,
-- the breaker, the rate limit, the attempt log — so the only thing a Telegram or
-- Discord destination changes is the request itself. That is one column.
--
-- The `url` and `secret` columns are reinterpreted rather than joined by new ones,
-- and that is the load-bearing decision. Both chat platforms put their credential in
-- a path segment: Telegram's bot token and Discord's webhook token are *part of the
-- URL* in their native form. A URL is returned by the admin API, stored on every
-- dead letter, and written into a span on every send — so storing one there would
-- leak the credential into three places at once.
--
-- Instead an endpoint stores its *address* in `url` and its *credential* in
-- `secret`, whatever the transport:
--
--   http      url = the customer's URL          secret = the signing key
--   telegram  url = telegram://<chat_id>        secret = the bot token
--   discord   url = discord://<webhook_id>      secret = the webhook token
--
-- `secret` is already the redacted type, never serialised and never printed, so the
-- new credentials inherit every protection the signing secret has.
ALTER TABLE endpoints
    ADD COLUMN transport text NOT NULL DEFAULT 'http';

-- Same reasoning as every other vocabulary column: a typo should be a failed write,
-- not a row that fails at send time for a reason nobody can see from the table.
ALTER TABLE endpoints
    ADD CONSTRAINT endpoints_transport_check
    CHECK (transport IN ('http', 'telegram', 'discord'));
