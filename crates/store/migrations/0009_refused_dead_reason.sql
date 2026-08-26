-- A refused destination gets its own reason.
--
-- Until now it was recorded as `permanent_failure`, which is true and useless. The
-- three reasons need three different responses from a person:
--
--   attempts_exhausted  the endpoint was down; replay when it is back
--   permanent_failure   the endpoint answered and said no; tell the customer
--   refused             the endpoint is not a public address, or not a port or a
--                       scheme we will speak to — nothing was ever sent
--
-- The third is the only one that is a security signal rather than a delivery
-- problem, and it is the one worth an alert: a customer registering an internal
-- address is either confused or probing, and both are worth knowing about before
-- they try the next spelling. Folded in with ordinary permanent failures, a spike
-- looks exactly like a customer deploying a broken URL.
ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_dead_reason_values_check,
    ADD CONSTRAINT deliveries_dead_reason_values_check
    CHECK (dead_reason IS NULL
           OR dead_reason IN ('permanent_failure', 'attempts_exhausted', 'refused'));
