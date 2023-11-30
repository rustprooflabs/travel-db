-- Deploy travel-db:002 to pg

BEGIN;


DROP VIEW travel.trip_point_detail;
CREATE VIEW travel.trip_point_detail AS
SELECT t.trip_id, t.trip_name, ts.trip_step_id, ts.leg_name, ts.step_name,
        ts.timeframe AS step_timeframe,
        tm.travel_mode_name,
        tp.ts, tp.travel_mode_status, tp.time_elapsed,
        tp.speed,
        convert.speed_m_s_to_mph(tp.speed) AS speed_mph,
        tp.ele,
        convert.dist_m_to_ft(tp.ele) AS ele_ft,
        convert.dist_m_to_ft(tp.distance) AS distance_ft,
        tp.geom
    FROM travel.trip_point tp
    INNER JOIN travel.trip_step ts ON tp.trip_step_id = ts.trip_step_id
    INNER JOIN travel.travel_mode tm ON ts.travel_mode_id = tm.travel_mode_id
    INNER JOIN travel.trip t ON ts.trip_id = t.trip_id
;

COMMIT;
