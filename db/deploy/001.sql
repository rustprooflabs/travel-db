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
    -- Following columns populated via data import processing
    time_elapsed INTERVAL NULL,
    time_elapsed_moving INTERVAL NULL,
    speed_avg NUMERIC NULL,
    speed_avg_moving NUMERIC NULL,
    ele_min NUMERIC,
    ele_avg NUMERIC,
    ele_max NUMERIC,
    geom GEOMETRY(MULTILINESTRING, 3857) NULL,
    CONSTRAINT uq_travel_trip_step_name_unique_in_trip
        UNIQUE (trip_id, step_name),
    CONSTRAINT trip_steps_no_overlap
        EXCLUDE USING GIST (trip_id with =, timeframe WITH &&)
);


COMMENT ON COLUMN travel.trip_step.leg_name IS 'Name of the leg of the trip.  e.g. Travel to destination. Travel home.';
COMMENT ON COLUMN travel.trip_step.step_name IS 'Name of the step of the leg of a trip.';
COMMENT ON COLUMN travel.trip_step.travel_mode_id IS 'ID of the Mode of travel for this step.';
COMMENT ON COLUMN travel.trip_step.timeframe IS 'Range of timestamps encompassing this step of the trip.  This range should be encompassed by the parent trip timeframe.';
COMMENT ON COLUMN travel.trip_step.geom IS 'Geometry of trip step.  Populated by data import procedure.';

CREATE INDEX ix_trip_step_trip_id
    ON travel.trip_step (trip_id);
CREATE INDEX ix_trip_step_travel_mode_id
    ON travel.trip_step (travel_mode_id);


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
COMMENT ON COLUMN travel.trip_point.distance IS 'Distance (meters) traveled between prior point and this point.  Calculated during import.';
COMMENT ON COLUMN travel.trip_point.time_elapsed IS 'Time elapsed between prior GPS observation and this GPS observation.  Calculated between timestamps reported by GPS unit observations.';
COMMENT ON COLUMN travel.trip_point.hdop IS 'Horizontal dilution of precision reported by GPS unit. Rankings: < 1: ideal; 1 - 2: Excellent;  2 - 5: Good; 5 -: 10 Moderate; 10 - 20: Fair; > 20: Poor;  Rankings from: https://en.wikipedia.org/wiki/Dilution_of_precision_(navigation)';
COMMENT ON COLUMN travel.trip_point.geom IS 'Point geometry recorded by GPS unit of location.';

CREATE INDEX trip_point_trip_step_id ON travel.trip_point (trip_step_id);



CREATE VIEW travel.trip_point_detail AS
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
COMMENT ON VIEW travel.trip_point_detail IS 'Main view to query for point level trip data. Handles joins between underlying tables to provide commonly used descriptors.';



CREATE UNLOGGED TABLE travel.import_duplicate_cleanup
(
    id BIGINT,
    trip_step_id BIGINT,
    ts TIMESTAMPTZ,
    speed_m_s NUMERIC,
    elevation_m NUMERIC,
    hdop NUMERIC, 
    geom GEOMETRY
);
COMMENT ON TABLE travel.import_duplicate_cleanup IS 'Duplicates during import (travel.load_bad_elf_points) on timestamp get auto-resolved and rejected rows saved here.  Unlogged table hints that long term storage of data should not be expected in this table, as unlogged tables are not crash safe.';


CREATE OR REPLACE PROCEDURE travel.load_bad_elf_points(
    _trip_id BIGINT
)
LANGUAGE plpgsql
AS $$

DECLARE _trip_name TEXT;

-- Initial Validation Variables
DECLARE _ts_duplicates BIGINT;
DECLARE _input_row_count BIGINT;
DECLARE _input_counts_match BOOLEAN;

-- Stage 2 Validation variables
DECLARE _input_row_count_stg_2 BIGINT;
DECLARE _gap_gt_1_second BIGINT;
DECLARE _gap_gt_10_second BIGINT;
DECLARE _gap_gt_60_second BIGINT;

-- Input summary variables
DECLARE _summary_steps_processed BIGINT;
DECLARE _summary_points_added BIGINT;
DECLARE _summary_steps_updated_geom BIGINT;

BEGIN

RAISE NOTICE 'Ensure data is populated to travel.trip AND travel.trip_step before running this procedure.';

SELECT trip_name INTO _trip_name
    FROM travel.trip
    WHERE trip_id = _trip_id
;

RAISE NOTICE 'Loading Bad Elf data to points table for Trip ID: % (%)', _trip_id, _trip_name;
ASSERT _trip_name IS NOT NULL, 'Trip not found.';

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

SELECT COUNT(*) INTO _input_row_count
    FROM trip_cleanup
;

ASSERT _input_row_count > 0, 'No input data.  Ensure Trip including Trip Steps exist in database.  Timestamps of data are compared to timeframe column defined for steps';



DROP TABLE IF EXISTS dup_cleanup;
CREATE TEMP TABLE dup_cleanup AS 
WITH dup_ts AS (
-- Identify timestamps with more than 1 value
SELECT ts
    FROM trip_cleanup
    GROUP BY ts
    HAVING COUNT(*) > 1
), dup_ids AS (
-- Find the IDs involved with the duplication
SELECT dt.ts, tc.id
    FROM dup_ts dt
    INNER JOIN trip_cleanup tc ON dt.ts = tc.ts
), diffs AS (
SELECT d.ts AS dup_ts, d.id AS dup_id, tc.id, tc.ts,
        d.ts - tc.ts AS ts_diff,
        d.id - tc.id AS id_diff,
        tc.geom
    FROM dup_ids d
    -- Compare against +/- 1 IDs (should be roughly +/- 1 seconds)
    INNER JOIN trip_cleanup tc
        ON d.id >= tc.id - 1
            AND d.id <= tc.id + 1
), ids_to_remove AS (
SELECT dup_id
    FROM diffs
    WHERE ts_diff > '10 seconds'::INTERVAL
        OR ts_diff < '-10 seconds'::INTERVAL
        --AND dup_ts = ts -- Limit to records that were duplicated...
    GROUP BY dup_id
    ORDER BY dup_id
)
SELECT tc.id, tc.trip_step_id, tc.ts, tc.speed_m_s, tc.elevation_m, tc.hdop, tc.geom
    FROM ids_to_remove r
    INNER JOIN trip_cleanup tc ON r.dup_id = tc.id
;

SELECT COUNT(*) INTO _ts_duplicates
    FROM dup_cleanup
;

IF _ts_duplicates > 0 THEN
    RAISE WARNING 'Duplicates found on timestamp. Check data in travel.import_duplicate_cleanup to decide if removed records were important.  Fix in input staging table before importing to resolve this issue.';

    INSERT INTO travel.import_duplicate_cleanup
    SELECT *
        FROM dup_cleanup
    ;

    DELETE FROM trip_cleanup t 
        WHERE t.id IN (SELECT id FROM dup_cleanup)
    ;
END IF;


WITH counts AS (
SELECT COUNT(*) AS row_count, COUNT(DISTINCT ts)::BIGINT AS distinct_ts_count
    FROM trip_cleanup
)
SELECT CASE WHEN row_count = distinct_ts_count THEN True ELSE False END
        INTO _input_counts_match
    FROM counts
;

ASSERT _input_counts_match, 'Data quality warning:  Duplicate Timestamps exist in source data. It is likely that later steps will fail or behave unexpectedly due to this duplication.';





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
    ORDER BY id
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


-- Remove these, result of bad sequencing (I think)
-- Not an ideal fix, but a decent-enough patch for now.
DELETE FROM trip_cleanup_2
    WHERE time_elapsed < '0 seconds'::INTERVAL
;
-- Removes most egregious edge cases in strange gaps
DELETE FROM trip_cleanup_2
    WHERE time_elapsed > '1 hour'::INTERVAL
;

-- Got a few strange hops to different parts of the world
-- Sometimes (likely when phone & GPS unit became unpaired and/or paired)
DELETE FROM trip_cleanup_2
    -- pretty safe assumption with 1 second intervals assuming
    -- non-military non-space craft...
    -- 500 meters / second ~= Mach 1.45...
    WHERE distance > 500
;

/*
    Currently Limitation in processing...
    A better quality approach to the above logic would be more
    recursive and iterative with the data fixes.
    It really should fix one row, then recalculate the lead/lag
    values for subsequent rows
    (through extended/super-extended time frames),
    and continue fixing iteratively.
*/

-- Best expected w/ Bad Elf: 1 meter
-- Anything under 1m is assumed as drift, setting to 0
UPDATE trip_cleanup_2
    SET distance = 0
    WHERE distance < 1.0 AND distance > 0.0
;


-- Check how many observations had unexpected gaps.  Bad Elf should be
-- recording one per second.
SELECT COUNT(*) INTO _input_row_count_stg_2
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

RAISE NOTICE E'Timestamp gap analysis...\n % rows\n % greater than 1 second gap\n % greater than 10 seconds gap\n % greater than 60 seconds gap', _input_row_count_stg_2, _gap_gt_1_second, _gap_gt_10_second, _gap_gt_60_second;


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
-- Load data into production tables
-----------------------------------------------
RAISE NOTICE 'Adding data to travel.trip_point';

    WITH insrt AS (
    INSERT INTO travel.trip_point (trip_step_id, travel_mode_status, ts,
            speed, speed_rolling, speed_rolling_extended,
            ele, distance, time_elapsed, hdop, geom
    )
    SELECT new.trip_step_id, new.travel_mode_status, new.ts,
            new.speed_m_s,
            new.speed_m_s_rolling, new.speed_m_s_rolling_extended,
            new.elevation_m, new.distance, new.time_elapsed, new.hdop,
            new.geom
        FROM trip_cleanup_2 new
        -- Exclude matches on ID and ts, avoid UQ violation
        LEFT JOIN travel.trip_point tp
            ON new.trip_step_id = tp.trip_step_id
                AND new.ts = tp.ts
        WHERE tp.trip_point_id IS NULL 
        RETURNING trip_point_id
    )
    SELECT COUNT(*) INTO _summary_points_added
        FROM insrt
    ;


SELECT COUNT(DISTINCT trip_step_id) INTO _summary_steps_processed
    FROM trip_cleanup_2
;


-- Trip Step -- Calculate multilinestring geometry
RAISE NOTICE 'Building line data and updating travel.trip_step geom.';
WITH agg_data AS (
SELECT trip_step_id,
        SUM(time_elapsed) AS time_elapsed,
        SUM(time_elapsed)
            FILTER (WHERE travel_mode_status <> 'Stopped')
                AS time_elapsed_moving,
        AVG(speed_m_s) AS speed_avg,
        AVG(speed_m_s)
            FILTER (WHERE travel_mode_status <> 'Stopped')
                AS speed_avg_moving,
        MIN(elevation_m) AS ele_min,
        AVG(elevation_m) AS ele_avg,
        MAX(elevation_m) AS ele_max,
        ST_MakeLine(geom ORDER BY ts) AS geom
    FROM trip_cleanup_2
    GROUP BY trip_step_id
), do_it AS (
UPDATE travel.trip_step ts
    SET geom = a.geom,
        time_elapsed = a.time_elapsed,
        time_elapsed_moving = a.time_elapsed_moving,
        speed_avg = a.speed_avg,
        speed_avg_moving = a.speed_avg_moving,
        ele_min = a.ele_min,
        ele_avg = a.ele_avg,
        ele_max = a.ele_max
    FROM agg_data a
    WHERE a.trip_step_id = ts.trip_step_id
        -- Not overwriting existing data.  Only checking geom is null,
        --   will force overwrite all other calculated columns when
        --   geom is null.
        AND ts.geom IS NULL
    RETURNING ts.trip_step_id
)
SELECT COUNT(DISTINCT trip_step_id) INTO _summary_steps_updated_geom
    FROM do_it
;


RAISE NOTICE E'Import summary:\n % points added\n Data included covers % steps from trip_step. \n % steps were updated with line geometry', _summary_points_added, _summary_steps_processed, _summary_steps_updated_geom;

IF _summary_steps_processed <> _summary_steps_updated_geom THEN
    RAISE WARNING 'One or more trip_step records already had line geometry.  This procedure cannot be used to update records with existing line data.';
END IF;

END
$$
;


COMMENT ON PROCEDURE travel.load_bad_elf_points IS 'Loads Bad Elf GPS data from staging.track_points table loaded from ogr2ogr from source GPX data. Target tables are travel.trip_point AND travel.trip_step.  It is expected that each step will only have one set of input data from the staging table.  Multiple traces for a single step should be merged before running this procedure. This procedure might work with other inputs, but is not tested.';


COMMIT;
