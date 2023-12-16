-- Revert travel-db:003 from pg

BEGIN;

COMMENT ON TABLE travel.travel_mode IS NULL;
COMMENT ON TABLE travel.trip_step IS NULL;
COMMENT ON VIEW travel.trip_point_detail IS NULL;

COMMENT ON COLUMN travel.trip_step.ele_avg IS NULL;
COMMENT ON COLUMN travel.trip_step.ele_max IS NULL;
COMMENT ON COLUMN travel.trip_step.ele_min IS NULL;
COMMENT ON COLUMN travel.trip_step.speed_avg IS NULL;
COMMENT ON COLUMN travel.trip_step.speed_avg_moving IS NULL;
COMMENT ON COLUMN travel.trip_step.time_elapsed IS NULL;
COMMENT ON COLUMN travel.trip_step.time_elapsed_moving IS NULL;

COMMENT ON COLUMN travel.import_duplicate_cleanup.elevation_m IS NULL;
COMMENT ON COLUMN travel.import_duplicate_cleanup.geom IS NULL;
COMMENT ON COLUMN travel.import_duplicate_cleanup.speed_m_s IS NULL;
COMMENT ON COLUMN travel.import_duplicate_cleanup.ts IS NULL;

COMMENT ON COLUMN travel.travel_mode.travel_mode_name IS NULL;

COMMENT ON COLUMN travel.trip.trip_desc IS NULL;


COMMIT;
