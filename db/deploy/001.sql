-- Deploy travel-db:001 to pg

BEGIN;

CREATE SCHEMA travel;

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Convert extension: https://github.com/rustprooflabs/convert
CREATE EXTENSION IF NOT EXISTS convert;



CREATE TABLE travel.travel_mode
(
    travel_mode_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    travel_mode_name TEXT NOT NULL,
    CONSTRAINT uq_travel_mode_name UNIQUE (travel_mode_name)
);

INSERT INTO travel.travel_mode (travel_mode_name)
    VALUES ('foot'), ('motor'), ('lightrail'), ('airplane')
;



CREATE TABLE travel.trip
(
    trip_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    trip_name TEXT NOT NULL,
    timeframe TSTZRANGE NOT NULL,
    trip_desc TEXT NULL,
    CONSTRAINT uq_trip_name UNIQUE (trip_name),
    CONSTRAINT ck_trip_desc_length
        CHECK (
            LENGTH(trip_desc) > 3
            AND LENGTH(trip_desc) < 10000
            )
);

COMMENT ON COLUMN travel.trip.trip_name IS 'Name of the trip. Trips can include multiple "legs", each leg with multiple steps. User defined.';



CREATE TABLE travel.trip_step
(
    trip_step_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    trip_id BIGINT NOT NULL REFERENCES travel.trip (trip_id),
    leg_name TEXT NOT NULL,
    step_name TEXT NOT NULL,
    timeframe TSTZRANGE NOT NULL,
    travel_mode_id BIGINT NOT NULL REFERENCES travel.travel_mode (travel_mode_id),
    CONSTRAINT uq_travel_trip_step_name_unique_in_trip
        UNIQUE (trip_id, step_name),
    CONSTRAINT trip_steps_no_overlap
        EXCLUDE USING GIST (trip_id with =, timeframe WITH &&)
);

COMMENT ON COLUMN travel.trip_step.leg_name IS 'Name of the leg of the trip.  e.g. Travel to destination. Travel home.';
COMMENT ON COLUMN travel.trip_step.step_name IS 'Name of the step of the leg of a trip.';
COMMENT ON COLUMN travel.trip_step.travel_mode_id IS 'ID of the Mode of travel for this step.';



CREATE TABLE travel.trip_point
(
    trip_point_id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    trip_step_id BIGINT NOT NULL REFERENCES travel.trip_step (trip_step_id),
    travel_mode_status TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    speed NUMERIC NOT NULL,
    speed_rolling NUMERIC NOT NULL,
    speed_rolling_extended NUMERIC NOT NULL,
    ele NUMERIC NULL,
    distance NUMERIC NULL,
    time_elapsed INTERVAL NULL,
    hdop NUMERIC NOT NULL,
    geom GEOMETRY(POINT, 3857) NOT NULL,
    CONSTRAINT uq_trip_point_timestamp UNIQUE (trip_step_id, ts)
);
COMMENT ON TABLE travel.trip_point IS 'Stores cleaned and classified trip points imported from GPX traces.';
COMMENT ON COLUMN travel.trip_point.travel_mode_status IS 'Current status of the travel mode.  E.g. stopped, accelerating, etc.';
COMMENT ON COLUMN travel.trip_point.ts IS 'Timestamp of the point in the trip.';
COMMENT ON COLUMN travel.trip_point.speed IS 'Speed (meters per second) reported by GPS unit.';
COMMENT ON COLUMN travel.trip_point.speed_rolling IS '10 second average speed (meters per second) calculated during import processing from GPS unit''s individual speed values.';
COMMENT ON COLUMN travel.trip_point.speed_rolling_extended IS '60 second average speed (meters per second) calculated during import processing from GPS unit''s individual speed values.';
COMMENT ON COLUMN travel.trip_point.ele IS 'Elevation (meters) above sea level reported by GPS unit.';
COMMENT ON COLUMN travel.trip_point.distance IS 'Distance (meters) travelled between prior point and this point reported by GPS unit.';
COMMENT ON COLUMN travel.trip_point.time_elapsed IS 'Time elapsed between prior GPS observation and this GPS observation.  Reported by GPS unit.';
COMMENT ON COLUMN travel.trip_point.hdop IS 'Horizontal dilution of precision reported by GPS unit. Rankings: < 1: ideal; 1 - 2: Excellent;  2 - 5: Good; 5 -: 10 Moderate; 10 - 20: Fair; > 20: Poor;  Rankings from: https://en.wikipedia.org/wiki/Dilution_of_precision_(navigation)';
COMMENT ON COLUMN travel.trip_point.geom IS 'Point geometry recorded by GPS unit of location.';

CREATE INDEX trip_point_trip_step_id ON travel.trip_point (trip_step_id);



CREATE VIEW travel.trip_points AS
SELECT t.trip_name, ts.leg_name, ts.step_name, ts.timeframe AS step_timeframe,
        tm.travel_mode_name,
        tp.ts, tp.travel_mode_status, tp.time_elapsed,
        tp.speed,
        convert.speed_m_s_to_mph(tp.speed) AS speed_mph,
        tp.ele,
        convert.dist_m_to_ft(tp.ele) AS ele_ft,
        tp.geom
    FROM travel.trip_point tp
    INNER JOIN travel.trip_step ts ON tp.trip_step_id = ts.trip_step_id
    INNER JOIN travel.travel_mode tm ON ts.travel_mode_id = tm.travel_mode_id
    INNER JOIN travel.trip t ON ts.trip_id = t.trip_id
;
COMMENT ON VIEW travel.trip_points IS 'Main view to query for point level trip data. Handles joins between underlying tables to provide commonly used descriptors.';




COMMIT;
