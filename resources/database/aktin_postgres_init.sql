--
-- create new database 'aktin' with its user
--

CREATE DATABASE aktin;

\connect aktin

CREATE USER aktin with PASSWORD 'aktin';
CREATE SCHEMA AUTHORIZATION aktin;
GRANT ALL ON SCHEMA aktin to aktin;
ALTER ROLE aktin WITH LOGIN;
