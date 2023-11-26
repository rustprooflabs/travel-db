-- Deploy travel-db:001 to pg

BEGIN;

CREATE SCHEMA travel;

CREATE EXTENSION IF NOT EXISTS postgis;
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

COMMENT ON TABLE travel.trip IS 'Tracks trip level details.';
COMMENT ON COLUMN travel.trip.trip_name IS 'Name of the trip. Trips can include multiple "legs", each leg with multiple steps. User defined.';
COMMENT ON COLUMN travel.trip.timeframe IS 'Range of timestamps encompassing the entire trip.';


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
COMMENT ON COLUMN travel.trip_step.timeframe IS 'Range of timestamps encompassing this step of the trip.  This range should be encompassed by the parent trip timeframe.';



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






CREATE OR REPLACE PROCEDURE travel.load_bad_elf_points(
    _trip_id BIGINT
)
LANGUAGE plpgsql
AS $$

DECLARE input_row_count BIGINT;
DECLARE counts_match BOOLEAN;
DECLARE _input_row_count BIGINT;
DECLARE _gap_gt_1_second BIGINT;
DECLARE _gap_gt_10_second BIGINT;
DECLARE _gap_gt_60_second BIGINT;

BEGIN

RAISE NOTICE 'Loading Bad Elf data to points table for Trip ID: %', _trip_id;

-- prepare to cleanup...
DROP TABLE IF EXISTS trip_cleanup;
CREATE TEMP TABLE trip_cleanup AS
WITH t_step AS (
SELECT ts.trip_step_id, t.trip_id, t.trip_name,
        ts.leg_name, ts.step_name, ts.timeframe, ts.travel_mode_id,
        tm.travel_mode_name
    FROM travel.trip t
    INNER JOIN travel.trip_step ts ON t.trip_id = ts.trip_id
    INNER JOIN travel.travel_mode tm ON ts.travel_mode_id = tm.travel_mode_id
    WHERE t.trip_id = _trip_id
)
SELECT ROW_NUMBER() OVER () AS id,
        ts.trip_step_id, ts.trip_name, ts.leg_name,
        COALESCE(ts.step_name, 'Undefined')::TEXT AS step_name,
        COALESCE(ts.travel_mode_name, '')::TEXT AS travel_mode,
        "time" AS ts,
        badelf_speed AS speed_m_s,
        ele AS elevation_m,
        hdop,
        wkb_geometry AS geom
    FROM staging.track_points tp
    -- WARNING: If trip_steps is missing times, this will exclude rows
    --   from processed data.  This is included as an intentional data cleanup step,
    --   not a mistake.
    INNER JOIN t_step ts ON tp."time" <@ ts.timeframe
    -- WARNING: DO NOT RELY ON Bad Elf's timestamp
    -- being sequential when it can connect to 
    -- iphone via Bluetooth.  If BT disconnects
    -- (e.g. stop for dinner) it can cause issues.
    ORDER BY track_seg_point_id
;

SELECT COUNT(*) INTO input_row_count
    FROM trip_cleanup
;

ASSERT input_row_count > 0, 'No input data.  Ensure Trip including Trip Steps exist in database.  Timestamps of data are compared to timeframe column defined for steps';


WITH counts AS (
SELECT COUNT(*) AS row_count, COUNT(DISTINCT ts)::BIGINT AS distinct_ts_count
    FROM trip_cleanup
)
SELECT CASE WHEN row_count = distinct_ts_count THEN True ELSE False END
        INTO counts_match
    FROM counts
;

ASSERT counts_match, 'Data quality warning:  Duplicate Timestamps exist in source data. It is likely that later steps will fail or behave unexpectedly due to this duplication.';

RAISE NOTICE 'Initial quality check passed...';


DROP TABLE IF EXISTS trip_cleanup_2;
CREATE TEMP TABLE trip_cleanup_2 AS
WITH add_lags AS (
SELECT id, speed_m_s, ts, geom,
        trip_name, leg_name, step_name, travel_mode,
        LAG(geom, 1) OVER (ORDER BY id) AS geom_prior,
        LAG(ts, 1) OVER (ORDER BY id) AS ts_prior,
        -- 60 steps is expected to be ~ 60 seconds elapsed from Bad Elf
        LAG(geom, 60) OVER (ORDER BY id) AS geom_prior_extended,
        LAG(ts, 60) OVER (ORDER BY id) AS ts_prior_extended
    FROM trip_cleanup
), calcs AS (
SELECT id, speed_m_s,
        (ts - ts_prior)::INTERVAL
            AS time_elapsed,
        ST_Distance(geom, geom_prior) AS distance,
        extract(epoch from ts - ts_prior_extended)::INT
            AS time_elapsed_extended,
        ST_Distance(geom, geom_prior_extended) AS distance_extended,
        geom_prior,
        geom_prior_extended
    FROM add_lags
    WHERE ST_Distance(geom, geom_prior_extended) IS NOT NULL
)
SELECT a.*,
        c.time_elapsed, c.distance, c.time_elapsed_extended,
        c.distance_extended,
        c.geom_prior,
        c.geom_prior_extended,
        AVG(a.speed_m_s) OVER (
            ORDER BY a.id
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) AS speed_m_s_rolling,
        AVG(a.speed_m_s) OVER (
            ORDER BY a.id
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS speed_m_s_rolling_extended,
        AVG(c.distance) OVER (
            ORDER BY a.id
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) AS distance_rolling,
        AVG(c.distance) OVER (
            ORDER BY a.id
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS distance_rolling_extended,
        AVG(c.distance) OVER (
            ORDER BY a.id
            ROWS BETWEEN 600 PRECEDING AND CURRENT ROW
        ) AS distance_rolling_super_extended
    FROM trip_cleanup a
    INNER JOIN calcs c ON a.id = c.id
;



-- Best expected w/ Bad Elf: 1 meter
-- Anything under 1m is assumed as drift, setting to 0
UPDATE trip_cleanup_2
    SET distance = 0
    WHERE distance < 1.0 AND distance > 0.0
;

-- Check how many observations had unexpected gaps.  Bad Elf should be
-- recording one per second.
SELECT COUNT(*) INTO _input_row_count
    FROM trip_cleanup_2
    WHERE time_elapsed IS NOT NULL
;
SELECT COUNT(*) FILTER (WHERE time_elapsed > '1 second'::INTERVAL)
                INTO _gap_gt_1_second
    FROM trip_cleanup_2
    WHERE time_elapsed IS NOT NULL
;
SELECT COUNT(*) FILTER (WHERE time_elapsed > '10 seconds'::INTERVAL)
                INTO _gap_gt_10_second
    FROM trip_cleanup_2
    WHERE time_elapsed IS NOT NULL
;
SELECT COUNT(*) FILTER (WHERE time_elapsed > '60 seconds'::INTERVAL)
                INTO _gap_gt_60_second
    FROM trip_cleanup_2
    WHERE time_elapsed IS NOT NULL
;

RAISE NOTICE E'Timestamp gap analysis...\n % rows\n % greater than 1 second gap\n % greater than 10 seconds gap\n % greater than 60 seconds gap', _input_row_count, _gap_gt_1_second, _gap_gt_10_second, _gap_gt_60_second;


----------------------------------------------
-- Calculate Travel mode Status
-- e.g. stopped, accelerating, braking, etc.
----------------------------------------------
ALTER TABLE trip_cleanup_2 ADD COLUMN travel_mode_status TEXT;
COMMENT ON COLUMN trip_cleanup_2.travel_mode_status IS 'Classifies status of the travel mode of the trip, e.g. stopped, accelerating, etc.';

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Stopped'
    WHERE travel_mode_status IS NULL
        AND speed_m_s = 0.0
;

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Stopped'
    WHERE travel_mode_status IS NULL
        AND distance_rolling < 2.0 -- 10 second window
        AND speed_m_s < 2.0
        AND distance_rolling_super_extended < 2.0 -- 600 second window
;

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Cruising'
    WHERE travel_mode_status IS NULL
        --AND travel_mode IN ('motor', 'airplane')
        AND speed_m_s > 2.0 
        -- if rolling and extended both are < 2% percent differences
        AND ABS((speed_m_s - speed_m_s_rolling) / speed_m_s_rolling)
            < .02
        AND ABS((speed_m_s - speed_m_s_rolling_extended) / speed_m_s_rolling_extended)
            < .02
;

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Braking'
    WHERE travel_mode_status IS NULL
        --AND travel_mode IN ('motor', 'airplane')
        AND speed_m_s_rolling > 0.0
        AND (speed_m_s - speed_m_s_rolling) / speed_m_s_rolling
            < -.10
;

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Accelerating'
    WHERE travel_mode_status IS NULL
       -- AND travel_mode IN ('motor', 'airplane')
        AND speed_m_s_rolling > 0.0 -- avoid div/0
        -- Use % diff gt 10% (positive only) over 10 seconds
        AND (speed_m_s - speed_m_s_rolling) / speed_m_s_rolling
            > .10
;

UPDATE trip_cleanup_2
    SET travel_mode_status = 'Fluctuating with Traffic'
    WHERE travel_mode_status IS NULL
        --AND travel_mode IN ('motor', 'airplane')
        AND speed_m_s_rolling > 0.0 -- avoid div/0
        AND ABS((speed_m_s - speed_m_s_rolling) / speed_m_s_rolling)
            <= .10
;
----------------------------------------------
-- End Travel mode Status calculations
----------------------------------------------


-----------------------------------------------
-- Load data into production table
INSERT INTO travel.trip_point (trip_step_id, travel_mode_status, ts,
        speed, speed_rolling, speed_rolling_extended,
        ele, distance, time_elapsed, hdop, geom
    )
SELECT trip_step_id, travel_mode_status, ts, speed_m_s,
        speed_m_s_rolling, speed_m_s_rolling_extended,
        elevation_m, distance, time_elapsed, hdop, geom
    FROM trip_cleanup_2
;



END
$$
;


COMMENT ON PROCEDURE travel.load_bad_elf_points IS 'Loads Bad Elf GPS data from staging.track_points table loaded from ogr2ogr from source GPX data.  This procedure might work with other inputs, but is not tested.';


COMMIT;
