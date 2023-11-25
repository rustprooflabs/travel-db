-- Revert travel-db:001 from pg

BEGIN;

DROP SCHEMA travel CASCADE;

COMMIT;
