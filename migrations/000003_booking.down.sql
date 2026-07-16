BEGIN;

DROP TABLE IF EXISTS tickets;
DROP TABLE IF EXISTS ticket_orders;
DROP TRIGGER IF EXISTS reservation_seats_validate ON reservation_seats;
DROP FUNCTION IF EXISTS validate_reservation_seat();
DROP TABLE IF EXISTS reservation_seats;
DROP TABLE IF EXISTS reservations;
DROP TRIGGER IF EXISTS seat_inventory_validate_class ON seat_inventory;
DROP FUNCTION IF EXISTS validate_inventory_seat_class();
DROP TABLE IF EXISTS seat_inventory;

COMMIT;
