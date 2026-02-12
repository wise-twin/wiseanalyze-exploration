CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

drop table sites

CREATE TABLE sites (
    site_id UUID PRIMARY KEY,
    plant_name TEXT NOT NULL,
    address TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    country TEXT NOT NULL,
    industrial_activity TEXT
);

ALTER TABLE sites 
ADD CONSTRAINT unique_plant_address 
UNIQUE (plant_name, address);

CREATE TABLE accidents (
    accident_id UUID PRIMARY KEY,
    site_id UUID NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
    title TEXT,
    source TEXT NOT NULL,
    source_id TEXT,
    accident_date DATE,
    severity_scale TEXT,
    raw_data JSONB, 
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NULL
);

ALTER TABLE accidents 
ADD CONSTRAINT unique_accident_title_date
UNIQUE (title, accident_date);

CREATE TABLE causes (
    id BIGSERIAL PRIMARY KEY,
    accident_id UUID unique NOT NULL REFERENCES accidents(accident_id) ON DELETE CASCADE,
    event_category TEXT,
    failure TEXT,
    description TEXT
);

CREATE TABLE substances (
    id BIGSERIAL PRIMARY KEY,
    accident_id UUID unique NOT NULL REFERENCES accidents(accident_id) ON DELETE CASCADE,
    name TEXT,
    cas_number TEXT,
    quantity TEXT,
    clp_class TEXT
);

CREATE TABLE consequences_human (
    id BIGSERIAL PRIMARY KEY,
    accident_id UUID unique NOT NULL REFERENCES accidents(accident_id) ON DELETE CASCADE,
    fatalities INTEGER,
    injuries INTEGER,
    evacuated INTEGER,
    hospitalized INTEGER
);

CREATE TABLE consequences_other (
    id BIGSERIAL PRIMARY KEY,
    accident_id UUID unique NOT NULL REFERENCES accidents(accident_id) ON DELETE CASCADE,
    environmental_impact TEXT,
    economic_cost TEXT,
    disruption_duration TEXT
);
