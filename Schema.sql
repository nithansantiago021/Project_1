create database air_tracker;
use air_tracker;


-- 1. Airport Table
create table if not exists airport(
	airport_id int auto_increment primary key,
    icao_code varchar(10) unique,
    iata_code varchar(10) unique,
    name varchar(255),
    city varchar(100),
    country varchar(100),
    continent varchar(100),
    latitude decimal(10,8),
    longitude decimal(11,8),
    timezone varchar(100)
);

-- 2. Flights Table
CREATE TABLE IF NOT EXISTS flights (
    flight_id INT AUTO_INCREMENT PRIMARY KEY,
    flight_number VARCHAR(20),
    aircraft_registration VARCHAR(20),
    origin_icao VARCHAR(10),
    origin_iata VARCHAR(10),
    origin_name VARCHAR(255),
    origin_country_code VARCHAR(10),
    destination_icao VARCHAR(10),
    destination_iata VARCHAR(10),
    destination_name VARCHAR(255),
    destination_country_code VARCHAR(10),
    scheduled_departure_utc DATETIME,
    actual_departure_utc DATETIME,
    scheduled_arrival_utc DATETIME,
    actual_arrival_utc DATETIME,
    status VARCHAR(50),
    movement_type VARCHAR(255),
    airline_code_iata VARCHAR(10),
    airline_code_icao VARCHAR(10)
);

-- 3. Aircraft Table
CREATE TABLE IF NOT EXISTS aircraft (
    aircraft_id INT AUTO_INCREMENT PRIMARY KEY,
    flight_number VARCHAR(20),
    aircraft_registration VARCHAR(20) UNIQUE,
    aircraft_model VARCHAR(100),
    -- icao_type_code VARCHAR(20),
    airline_code_iata VARCHAR(10),
    airline_code_icao VARCHAR(10),
    airline_name VARCHAR(255),
    aircraft_manufacturer VARCHAR(50)
);


-- 4. Airport Delays Table
CREATE TABLE IF NOT EXISTS airport_delays (
    delay_id INT AUTO_INCREMENT PRIMARY KEY,
    airport_iata VARCHAR(10),
    delay_date DATE,
    total_flights INT,
    delayed_flights INT,
    avg_delay_min INT,
    median_delay_min INT,
    canceled_flight INT
);

