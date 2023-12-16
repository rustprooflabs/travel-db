-- Deploy travel-db:003 to pg
-- requires: 002

BEGIN;

COMMENT ON TABLE travel.travel_mode IS 'Lookup table for available travel modes to assign to trip_step records, e.g. foot, motor, etc.';
COMMENT ON TABLE travel.trip_step IS 'A trip step provides more granular detail about the legs/steps of a trip. The time frame of each step must not overlap with the time frame of other steps in the same trip.';
COMMENT ON VIEW travel.trip_point_detail IS 'Main view to query for point level trip data. Handles joins between underlying tables to provide commonly used descriptors.';

COMMENT ON COLUMN travel.trip_step.ele_avg IS 'Average elevation (meters) of this trip strip. Calculated during import.';
COMMENT ON COLUMN travel.trip_step.ele_max IS 'Maximum elevation (meters) of this trip strip. Calculated during import.';
COMMENT ON COLUMN travel.trip_step.ele_min IS 'Minimum elevation (meters) of this trip strip. Calculated during import.';
COMMENT ON COLUMN travel.trip_step.speed_avg IS 'Average speed (meters / second) of the trip step. Calculated during import.';
COMMENT ON COLUMN travel.trip_step.speed_avg_moving IS 'Average speed (meters / second) of the trip step after excluding points where no motion is detected. Calculated during import.';
COMMENT ON COLUMN travel.trip_step.time_elapsed IS 'Time elapsed during this trip step.  Sum of point level intervals, calculated during import.';
COMMENT ON COLUMN travel.trip_step.time_elapsed_moving IS 'Time elapsed during this trip step after excluding points where no motion is detected. Sum of point level intervals, calculated during import.';


COMMENT ON COLUMN travel.import_duplicate_cleanup.elevation_m IS 'Elevation (meters) above sea level. Reported by GPS unit.';
COMMENT ON COLUMN travel.import_duplicate_cleanup.geom IS 'Geometry of the point being excluded as a duplicate based on its timestamp. Reported by GPS unit.';
COMMENT ON COLUMN travel.import_duplicate_cleanup.speed_m_s IS 'Speed (meters / second) observed at this point. Reported by GPS unit.';
COMMENT ON COLUMN travel.import_duplicate_cleanup.ts IS 'Timestamp of the point from the source data.  This value was duplicated with another point in the input data and was excluded.';

COMMENT ON COLUMN travel.travel_mode.travel_mode_name IS 'Name of the travel mode.  This is currently expected to be a lowercase value.';

COMMENT ON COLUMN travel.trip.trip_desc IS 'Description of the trip.';


COMMIT;
