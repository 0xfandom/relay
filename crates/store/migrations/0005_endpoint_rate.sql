-- How fast each endpoint may be sent to.
--
-- Relay's own fan-out is a denial of service waiting to happen: a customer
-- subscribes to a high-volume event, one burst produces ten thousand deliveries, and
-- their server falls over. Every one of those then fails, retries, and arrives again
-- as a wave. The rate limit is what stops Relay from being the cause of the outage
-- it is then retrying against.

ALTER TABLE endpoints
    -- Sustained deliveries per second.
    ADD COLUMN rate_per_second double precision NOT NULL DEFAULT 10,
    -- The most that may leave at once after an idle period. Separate from the rate
    -- because they answer different questions: the rate is what the endpoint can
    -- sustain, the burst is what it can absorb in one go. A burst of one would smooth
    -- traffic perfectly and leave Relay unable to ever catch up on a backlog.
    ADD COLUMN burst           double precision NOT NULL DEFAULT 20;

ALTER TABLE endpoints
    -- A rate of zero is not "unlimited", it is "never", and it would park every
    -- delivery to this endpoint forever while looking like configuration.
    ADD CONSTRAINT endpoints_rate_positive CHECK (rate_per_second > 0),
    -- A bucket that cannot hold one whole token can never spend one.
    ADD CONSTRAINT endpoints_burst_at_least_one CHECK (burst >= 1);
