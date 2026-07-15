BEGIN;

DROP TABLE IF EXISTS fares;
DROP TABLE IF EXISTS train_runs;
DROP TABLE IF EXISTS seats;
DROP TABLE IF EXISTS coaches;
DROP TABLE IF EXISTS trains;
DROP TRIGGER IF EXISTS route_stops_validate_sequence ON route_stops;
DROP FUNCTION IF EXISTS validate_route_stop_sequence();
DROP TABLE IF EXISTS route_stops;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS stations;

COMMIT;
