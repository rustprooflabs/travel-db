-- Revert travel-db:002 from pg

BEGIN;

DROP VIEW travel.trip_point_detail;
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


COMMIT;
