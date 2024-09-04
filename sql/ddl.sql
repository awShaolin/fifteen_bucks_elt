create schema raw;

CREATE TABLE IF NOT EXISTS raw.cannabis (
    id int8 PRIMARY KEY,
    uid uuid not null,
    strain TEXT,
    cannabinoid_abbreviation TEXT,
    cannabinoid TEXT,
    terpene TEXT,
    medical_use TEXT,
    health_benefit TEXT,
    category TEXT,
    "type" TEXT,
    buzzword TEXT,
    brand TEXT
);

CREATE ROLE af_load WITH 
  password 'af_load'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  INHERIT
  LOGIN
  NOREPLICATION
  NOBYPASSRLS
  CONNECTION LIMIT -1;

GRANT USAGE ON SCHEMA raw TO af_load;
GRANT INSERT, UPDATE, SELECT ON TABLE raw.cannabis TO af_load;


select * from raw.cannabis;