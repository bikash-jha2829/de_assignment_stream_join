create SCHEMA IF NOT EXISTS events;
drop table IF EXISTS events.user_events;
create TABLE IF NOT EXISTS events.user_events
(
    id VARCHAR(100),
    event_type VARCHAR(30),
    username  VARCHAR(30),
	user_email VARCHAR(30),
    user_type  float,
	organization_name float,
	received_at  TIMESTAMPTZ
);
drop table IF EXISTS events.org_events;
create TABLE IF NOT EXISTS events.org_events
(
    organization_key VARCHAR(100),
    organization_name VARCHAR(100),
    organization_tier VARCHAR(100),
    created_at TIMESTAMPTZ
};