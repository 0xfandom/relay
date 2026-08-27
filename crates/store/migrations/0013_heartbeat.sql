-- A place for a background process to say it is still running.
--
-- Readiness needs two different facts about the dispatcher and neither one implies
-- the other. Queue lateness says whether work is draining, but says nothing at all
-- while the queue is empty: a dispatcher that died overnight looks identical to one
-- with nothing to do. This table answers the other half — the process was alive
-- recently — and only the two together mean "ready".
--
-- One row per component rather than one per process. Relay runs a single dispatcher
-- today; if it ever runs several, the freshest heartbeat is still the honest answer
-- to "is anything dispatching", and readiness is not the place to discover that one
-- replica of five is down. That belongs to the metrics, which are per instance.
CREATE TABLE relay_heartbeat (
    component text PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now()
);
