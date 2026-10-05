-- Create one logical database per Orbit service. Services never share a
-- database and never read each other's tables.

CREATE DATABASE orbit_order;
CREATE DATABASE orbit_inventory;
CREATE DATABASE orbit_payment;
CREATE DATABASE orbit_fulfillment;
CREATE DATABASE orbit_notification;
