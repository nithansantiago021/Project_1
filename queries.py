# 1) Show the total number of flights for each aircraft model, listing the model and its count.
## Dashboard --> "Aircraft Model Distribution" --> tab3.py
def get_query1():
    return """
            WITH CleanFlights AS (SELECT MIN(aircraft_registration) as reg, 
                    scheduled_departure_utc, 
                    origin_iata
                FROM flights
                GROUP BY scheduled_departure_utc, origin_iata, destination_iata
            )
            SELECT 
                a.aircraft_model, 
                COUNT(*) AS total_flights
            FROM CleanFlights c
            JOIN aircraft a ON c.reg = a.aircraft_registration
            GROUP BY a.aircraft_model
            ORDER BY total_flights DESC;
        """


def get_query1_specific(selected_iata):
    return f"""
        WITH CleanFlights AS (
            SELECT 
                MIN(f.aircraft_registration) as reg, -- The actual physical plane
                f.scheduled_departure_utc,
                f.origin_iata
            FROM flights f
            WHERE f.origin_iata = '{selected_iata}' OR f.destination_iata = '{selected_iata}'
            GROUP BY f.scheduled_departure_utc, f.origin_iata, f.destination_iata
        )
        SELECT 
            a.aircraft_model, 
            COUNT(*) as total_flights
        FROM CleanFlights c
        JOIN aircraft a ON c.reg = a.aircraft_registration
        WHERE a.aircraft_model IS NOT NULL
        GROUP BY a.aircraft_model
        ORDER BY total_flights DESC;
        """
## Dashboard --> "Most Used Aircraft Models" --> tab3.py 
# 2) List all aircraft (registration, model) that have been assigned to more than 5 flights.
def get_query2():
    return """
        WITH cleanflights AS (SELECT MIN(aircraft_registration) AS reg,
                    scheduled_departure_utc,
                    origin_iata,
                    destination_iata
                    FROM
                    flights
                    GROUP BY scheduled_departure_utc, origin_iata, destination_iata
                    )
        SELECT a.aircraft_registration, a.aircraft_model, count(*) AS total_flights
        FROM cleanflights c 
        JOIN aircraft a
        on c.reg = a.aircraft_registration
        GROUP BY a.aircraft_registration, a.aircraft_model
        HAVING COUNT(*) > 5
        ORDER BY total_flights DESC;
    """

# 3) For each airport, display its name and the number of outbound flights, but only for airports with more than 5 flights.
def get_query3():
    return """SELECT a.name AS airport_name, COUNT(DISTINCT f.scheduled_departure_utc) AS outbound_flights
FROM flights f
JOIN airport a ON f.origin_iata = a.iata_code
GROUP BY a.name
HAVING outbound_flights > 5
ORDER BY outbound_flights DESC;"""

## Dashboard --> "Top 5 International Destinations" --> tab1.py
def get_query3_specific(selected_iata):
    return f"""SELECT 
    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT f.destination_name ORDER BY LENGTH(f.destination_name) ASC SEPARATOR '|'), '|', 1) AS destination_name,
    COUNT(DISTINCT f.scheduled_departure_utc) AS total_flights
FROM flights f
WHERE f.origin_iata = '{selected_iata}'
  AND f.origin_country_code <> f.destination_country_code
GROUP BY f.destination_iata  
ORDER BY total_flights DESC
LIMIT 5;"""

# 4) Find the top 3 destination airports (name, city) by number of arriving flights, sorted by count descending.
def get_query4():
    return """
        SELECT 
        a.name AS airport_name, 
        a.city, 
        COUNT(DISTINCT f.origin_iata, f.scheduled_departure_utc) AS arriving_flights
        FROM flights f JOIN airport a ON f.destination_iata = a.iata_code
        GROUP BY a.name, a.city 
        ORDER BY arriving_flights DESC LIMIT 3;
            """

## Dashboard --> "All Destinations Heatmap" --> tab1.py
def get_query_all_destinations():
    return """
        SELECT 
        a.name AS airport_name, 
        a.city, 
        COUNT(DISTINCT f.origin_iata, f.scheduled_departure_utc) AS arriving_flights
        FROM flights f JOIN airport a ON f.destination_iata = a.iata_code
        GROUP BY a.name, a.city 
        ORDER BY arriving_flights DESC;
            """

# 5) Show for each flight: number, origin, destination, and a label 'Domestic' or 'International' using CASE WHEN on country match.
def get_query5():
    return """SELECT DISTINCT
    flight_number, 
    origin_iata AS origin, 
    destination_iata AS destination,
    CASE 
        WHEN origin_country_code = destination_country_code THEN 'Domestic'
        ELSE 'International'
    END AS flight_type
FROM flights;"""

# 6) Show the 5 most recent arrivals at “DEL” airport including flight number, aircraft, departure airport name, and arrival time, ordered by latest arrival.
def get_query6():
    return """SELECT 
    GROUP_CONCAT(DISTINCT f.flight_number SEPARATOR ', ') AS flight_numbers,
    MAX(f.aircraft_registration) AS aircraft_registration, 
    a.name AS origin_airport, 
    MAX(f.actual_arrival_utc) AS arrival_time
FROM flights f 
JOIN airport a ON f.origin_iata = a.iata_code
WHERE f.destination_iata = 'DEL' AND f.status = 'arrived'
GROUP BY f.scheduled_departure_utc, a.name
ORDER BY arrival_time DESC 
LIMIT 5;"""

## Dashboard --> "Recent Arrival" --> tab1.py
def get_query6_specific(selected_iata):
    return f"""
           SELECT 
                GROUP_CONCAT(DISTINCT flight_number SEPARATOR ', ') AS flight_numbers,
                MAX(aircraft_registration) AS aircraft_registration, 
                origin_iata AS origin_code, 
                MAX(origin_name) AS origin_name,
                MAX(actual_arrival_utc) AS arrival_time
            FROM flights 
            WHERE destination_iata = '{selected_iata}'
              AND status = 'arrived'
              AND actual_arrival_utc IS NOT NULL
            GROUP BY scheduled_departure_utc, origin_iata
            ORDER BY arrival_time DESC 
            LIMIT 5;
        """


def get_query7():
    return"""SELECT name AS airport_name, iata_code
        FROM airport
        WHERE iata_code NOT IN (
            SELECT DISTINCT destination_iata 
            FROM flights 
            WHERE destination_iata IS NOT NULL
        );"""

## Dashboard --> "Outbound-Only Routes" --> tab1.py
def get_query7_spl(selected_iata):
    return f"""
                SELECT DISTINCT 
                    a.name AS destination_name, 
                    f.destination_iata AS airport_code
                FROM flights f
                JOIN airport a ON f.destination_iata = a.iata_code
                WHERE f.origin_iata = '{selected_iata}'
                AND f.destination_iata NOT IN (
                    -- This subquery looks for any return flights back to your selected airport
                    SELECT DISTINCT origin_iata 
                    FROM flights 
                    WHERE destination_iata = '{selected_iata}'
                    AND origin_iata IS NOT NULL
                )
                ORDER BY destination_name;
            """


# 8) For each airline, count the number of flights by status (e.g., 'On Time', 'Delayed', 'Cancelled') using CASE WHEN.
# Dashboard --> "Airline Performance" -->tab2.py
def get_query8():
    return """
        WITH PhysicalFlights AS (
            SELECT 
                MAX(airline_code_iata) AS airline_code_iata,
                status,
                MAX(actual_arrival_utc) AS actual_arrival_utc,
                MAX(scheduled_arrival_utc) AS scheduled_arrival_utc
            FROM flights
            WHERE status IN ('arrived', 'Canceled')
            GROUP BY origin_iata, destination_iata, scheduled_departure_utc, status
        ),
        AirlineDirectory AS (
            SELECT DISTINCT airline_code_iata, airline_name 
            FROM aircraft
            WHERE airline_name IS NOT NULL
        )
        SELECT 
            d.airline_name AS 'Airline Name', 
            p.airline_code_iata AS 'IATA Code',
            SUM(CASE WHEN p.status = 'arrived' AND p.actual_arrival_utc <= p.scheduled_arrival_utc THEN 1 ELSE 0 END) AS 'On-Time Flights',
            SUM(CASE WHEN p.status = 'arrived' AND p.actual_arrival_utc > p.scheduled_arrival_utc THEN 1 ELSE 0 END) AS 'Delayed Flights',
            SUM(CASE WHEN p.status = 'Canceled' THEN 1 ELSE 0 END) AS 'Cancelled Flights'
        FROM PhysicalFlights p
        LEFT JOIN AirlineDirectory d ON p.airline_code_iata = d.airline_code_iata
        GROUP BY d.airline_name, p.airline_code_iata
        ORDER BY 'On-Time Flights' DESC;
        """


# 9) Show all cancelled flights, with aircraft and both airports, ordered by departure time descending.
## Dashboard --> "Cancelled Flights" --> tab1.py
def get_query9():
    return """SELECT 
    GROUP_CONCAT(DISTINCT f.flight_number SEPARATOR ', ') AS flight_numbers,
    MAX(f.aircraft_registration) AS aircraft,
    o.name AS origin_airport, 
    d.name AS destination_airport, 
    f.scheduled_departure_utc
FROM flights f 
JOIN airport o ON f.origin_iata = o.iata_code
JOIN airport d ON f.destination_iata = d.iata_code
WHERE f.status = 'Canceled' 
GROUP BY f.scheduled_departure_utc, o.name, d.name
ORDER BY f.scheduled_departure_utc DESC;"""


# 10) List all city pairs (origin-destination) that have more than 2 different aircraft models operating flights between them.
def get_query10():
    return """WITH PhysicalFlights AS (
    SELECT MIN(aircraft_registration) AS reg, origin_iata, destination_iata
    FROM flights
    GROUP BY scheduled_departure_utc, origin_iata, destination_iata
)
SELECT 
    p.origin_iata, 
    p.destination_iata, 
    COUNT(DISTINCT a.aircraft_model) AS unique_models
FROM PhysicalFlights p
JOIN aircraft a ON p.reg = a.aircraft_registration
WHERE a.aircraft_model IS NOT NULL
GROUP BY p.origin_iata, p.destination_iata
HAVING unique_models > 2
ORDER BY unique_models DESC;"""


# 11) For each destination airport, compute the % of delayed flights (status='Delayed') among all arrivals, sorted by highest percentage. 
def get_query11():
    return """
        WITH UniqueArrivals AS (
            SELECT 
                destination_iata,
                MAX(actual_arrival_utc) AS actual_arrival_utc,
                MAX(scheduled_arrival_utc) AS scheduled_arrival_utc
            FROM flights
            WHERE status = 'arrived'
            GROUP BY origin_iata, destination_iata, scheduled_departure_utc
        )
        SELECT 
            d.name AS destination_airport, 
            d.city,
            ROUND(COUNT(CASE WHEN u.actual_arrival_utc > u.scheduled_arrival_utc THEN 1 END) * 100.0 / COUNT(*), 2) AS percent_delayed
        FROM UniqueArrivals u
        JOIN airport d ON u.destination_iata = d.iata_code
        GROUP BY d.airport_id, d.name, d.city 
        ORDER BY percent_delayed DESC 
        LIMIT 5;
            """


## dashboard --> "Airline Reliability" --> tab2.py
def get_query11_specific(selected_iata):
    return f"""
            WITH UniqueArrivals AS (
                SELECT 
                    MAX(f.flight_number) AS flight_number,
                    MAX(f.actual_arrival_utc) AS actual_arrival_utc,
                    MAX(f.scheduled_arrival_utc) AS scheduled_arrival_utc
                FROM flights f
                WHERE f.destination_iata = '{selected_iata}' AND f.status = 'arrived'
                GROUP BY f.origin_iata, f.scheduled_departure_utc
            )
            SELECT 
                a.airline_name,
                ROUND(COUNT(CASE WHEN u.actual_arrival_utc > u.scheduled_arrival_utc THEN 1 END) * 100.0 / COUNT(*), 2) AS delay_rate
            FROM UniqueArrivals u
            JOIN aircraft a ON u.flight_number = a.flight_number
            GROUP BY a.airline_name 
            ORDER BY delay_rate DESC;
        """

## Dashboard --> (Most Used Aircraft Model by Airline) --> tab2.py
def get_query_airline_aircraft_model():
    return """
        WITH RankedAircraft AS (
            SELECT
                a.airline_name,
                a.aircraft_model,
                COUNT(*) AS flight_count,
                ROW_NUMBER() OVER(PARTITION BY a.airline_name ORDER BY COUNT(*) DESC) as rn
            FROM flights f
            JOIN aircraft a ON f.aircraft_registration = a.aircraft_registration
            WHERE a.airline_name IS NOT NULL AND a.aircraft_model IS NOT NULL
            GROUP BY a.airline_name, a.aircraft_model
        )
        SELECT
            airline_name,
            aircraft_model,
            flight_count
        FROM RankedAircraft
        WHERE rn = 1
        ORDER BY airline_name;
    """



