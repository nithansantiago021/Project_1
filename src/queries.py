def get_airport_list():
    """
    Returns the SQL query to get a list of all airports for selection UI.
    """
    # Selecting iata_code, name, and city to support all current use cases.
    return "SELECT iata_code, name, city FROM airport ORDER BY name"


## Tab3.py --> "Aircraft Model Distribution"
def get_manufacturer_share(iata_code=None):
    where_clause = "" if iata_code == "ALL" else f"WHERE origin_iata = '{iata_code}' OR destination_iata = '{iata_code}'"
    return f"""
        SELECT 
            a.aircraft_manufacturer, 
            COUNT(*) AS total_flights
        FROM flights f
        JOIN aircraft a ON f.aircraft_registration = a.aircraft_registration
        {where_clause}
        GROUP BY a.aircraft_manufacturer
        ORDER BY total_flights DESC;
    """

def get_models_by_manufacturer(manufacturer, iata_code=None):
    where_clause = f"WHERE a.aircraft_manufacturer = '{manufacturer}'"
    if iata_code != "ALL":
        where_clause += f" AND (f.origin_iata = '{iata_code}' OR f.destination_iata = '{iata_code}')"
    
    return f"""
        SELECT 
            a.aircraft_model, 
            COUNT(*) AS total_flights
        FROM flights f
        JOIN aircraft a ON f.aircraft_registration = a.aircraft_registration
        {where_clause}
        GROUP BY a.aircraft_model
        ORDER BY total_flights DESC;
    """



def get_query1(iata_code=None):
    # 1. Handle the "ALL" vs Specific case
    if iata_code == "ALL" or iata_code is None:
        where_clause = ""
    else:
        # Note: Added a space at the start for safety
        where_clause = f" WHERE origin_iata = '{iata_code}' OR destination_iata = '{iata_code}'"
    
    return f"""
            WITH CleanFlights AS (
                SELECT 
                    MIN(aircraft_registration) as reg, 
                    scheduled_departure_utc, 
                    origin_iata,
                    destination_iata
                FROM flights  -- <-- FIX 1: You must specify the table name here
                {where_clause} -- <-- FIX 2: WHERE comes AFTER the table name
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

## Dashboard --> "Most Used Aircraft Models" --> tab3.py 
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


## Dashboard --> "More than 5 outbound flights in a day" --> tab1.py
def get_query3(iata_code = None):
    if iata_code == "ALL":
        where_clause = ""
    else:
        where_clause = f"WHERE origin_iata = '{iata_code}'"

    return f"""SELECT 
    origin_name, 
    destination_name, 
    COUNT(DISTINCT origin_name, destination_name, scheduled_departure_utc, scheduled_arrival_utc) AS outbound_flights
    FROM flights
    {where_clause}
    GROUP BY origin_name, destination_name
    HAVING outbound_flights > 5            -- Only keep routes with more than 5 flights
    ORDER BY outbound_flights DESC;"""


## Dashboard --> "Top 3 destinations" --> tab1.py
def get_query4(iata_code):
    # If 'ALL' is selected, we don't filter the destination, 
    # showing the top 3 routes globally.
    if iata_code == "ALL":
        where_clause = ""
    else:
        where_clause = f"AND f.destination_iata = '{iata_code}'"

    return f"""
        SELECT 
        f.origin_name AS origin_airport,
        f.origin_iata AS IATA,
        COUNT(DISTINCT f.flight_number, f.scheduled_arrival_utc) AS total_flights
        FROM flights f
        WHERE f.movement_type = 'Arrival'{where_clause}
        GROUP BY f.origin_name, f.origin_iata
        ORDER BY total_flights DESC
        LIMIT 3;
    """

## Dashboard --> tab1.py
def get_query5(selected_iata=None):
    # Filter logic
    if selected_iata == "ALL" or selected_iata is None:
        where_clause = "WHERE movement_type = 'Departure'"
    else:
        where_clause = f"WHERE origin_iata = '{selected_iata}' AND movement_type = 'Departure'"

    return f"""
    WITH UniqueFlightList AS (
        SELECT 
            flight_number,
            origin_name AS origin,
            destination_name AS destination,
            CASE 
                WHEN origin_country_code = destination_country_code THEN 'Domestic'
                ELSE 'International'
            END AS flight_type,
            ROW_NUMBER() OVER (
                PARTITION BY origin_iata, destination_iata, scheduled_departure_utc 
                ORDER BY flight_number ASC
            ) as row_num
        FROM flights
        {where_clause}
    )
    SELECT 
        flight_number,
        origin,
        destination,
        flight_type
    FROM UniqueFlightList
    WHERE row_num = 1
    ORDER BY flight_type DESC, origin ASC;
    """

## Dashboard --> "Recent Arrival" --> tab1.py
def get_query6(iata_code=None):
    # Logic to handle the selection
    if iata_code == 'ALL' or iata_code is None:
        where_clause = "WHERE movement_type = 'Arrival'"
    else:
        # Crucial: Use '{iata_code}' with single quotes for SQL strings
        where_clause = f"WHERE destination_iata = '{iata_code}' AND movement_type = 'Arrival'"

    return f"""
    WITH UniqueArrivals AS (
        SELECT 
            flight_number,
            origin_name AS departure_airport,
            destination_name AS origin_airport,
            actual_arrival_utc AS arrival_time,
            ROW_NUMBER() OVER (
                PARTITION BY origin_iata, destination_iata, actual_arrival_utc 
                ORDER BY flight_number ASC
            ) as row_num
        FROM flights
        {where_clause}
    )
    SELECT 
        flight_number,
        departure_airport,
        origin_airport,
        arrival_time
    FROM UniqueArrivals
    WHERE row_num = 1
    ORDER BY arrival_time DESC
    LIMIT 5;
    """


## Dashboard --> "Outbound-Only Routes" --> tab1.py
def get_query7(selected_iata):
    # Case 1: Specific Airport (e.g., 'BLR')
    if selected_iata != "ALL":
        where_clause = f"WHERE f.origin_iata = '{selected_iata}'"
        # Subquery: Find any flights coming BACK to our selected airport
        subquery_filter = f"f2.destination_iata = '{selected_iata}'"
    
    # Case 2: ALL Airports
    else:
        where_clause = "WHERE f.origin_iata IS NOT NULL"
        # Subquery: Find if there is any flight going from B back to A
        subquery_filter = "f2.origin_iata = f.destination_iata AND f2.destination_iata = f.origin_iata"

    return f"""
        SELECT DISTINCT 
            f.origin_name AS origin_name, 
            f.origin_iata AS airport_code,
            f.destination_name AS destination_name, 
            f.destination_iata AS airport_code
        FROM flights f
        {where_clause}
        AND f.destination_iata NOT IN (
            SELECT DISTINCT f2.origin_iata 
            FROM flights f2
            WHERE {subquery_filter}
            AND f2.origin_iata IS NOT NULL
        )
        ORDER BY destination_name;
    """


# Dashboard --> "Airline Performance" -->tab2.py
def get_query8(iata_code=None):
    # 1. Handle the filtering logic
    if iata_code == "ALL" or iata_code is None:
        # If ALL, we just ensure we only look at arrivals globally
        filter_sql = "WHERE movement_type = 'Arrival'"
    else:
        # If specific, we filter by destination AND movement type
        filter_sql = f"WHERE destination_iata = '{iata_code}' AND movement_type = 'Arrival'"

    return f"""
        WITH UniquePhysicalFlights AS (
            SELECT 
                airline_code_iata,
                actual_arrival_utc,
                scheduled_arrival_utc,
                status,
                ROW_NUMBER() OVER (
                    PARTITION BY origin_iata, destination_iata, scheduled_arrival_utc 
                    ORDER BY actual_arrival_utc DESC
                ) as row_num
            FROM flights
            {filter_sql} 
            AND status IN ('Arrived', 'Canceled', 'Delayed')
        )
        SELECT 
            al.airline_name,
            f.airline_code_iata,
            SUM(CASE 
                    WHEN f.status != 'Canceled' 
                    AND f.actual_arrival_utc <= f.scheduled_arrival_utc 
                THEN 1 ELSE 0 END) AS on_time_flights,
            SUM(CASE 
                    WHEN (f.status = 'Delayed' OR f.actual_arrival_utc > f.scheduled_arrival_utc)
                    AND f.status != 'Canceled'
                THEN 1 ELSE 0 END) AS delayed_flights,
            SUM(CASE 
                    WHEN f.status = 'Canceled' 
                THEN 1 ELSE 0 END) AS cancelled_flights
        FROM UniquePhysicalFlights f
        JOIN (SELECT DISTINCT airline_code_iata, airline_name FROM aircraft) al
            ON f.airline_code_iata = al.airline_code_iata
        WHERE f.row_num = 1
        GROUP BY al.airline_name, f.airline_code_iata
        ORDER BY on_time_flights DESC;
        """


## Dashboard --> "Cancelled Flights" --> tab1.py
def get_query9(iata_code=None):
    # Base query parts
    if iata_code == "ALL":
        where_clause = ""
    
    # If a code is provided, add the filter
    else:
        where_clause = f"AND (f.origin_iata = '{iata_code}')"

    return f"""
    WITH UniqueCancellations AS (
        SELECT 
            f.flight_number,
            f.aircraft_registration AS aircraft,
            f.origin_iata,
            f.destination_iata,
            f.scheduled_departure_utc,
            ROW_NUMBER() OVER (
                PARTITION BY f.origin_iata, f.destination_iata, f.scheduled_departure_utc 
                ORDER BY f.flight_number ASC
            ) as row_num
        FROM flights f
        WHERE f.status = 'Canceled'
        {where_clause}
    )
    SELECT 
        uc.flight_number,
        uc.aircraft,
        o.name AS origin_airport,
        d.name AS destination_airport,
        uc.scheduled_departure_utc
    FROM UniqueCancellations uc
    JOIN airport o ON uc.origin_iata = o.iata_code
    JOIN airport d ON uc.destination_iata = d.iata_code
    WHERE uc.row_num = 1
    ORDER BY uc.scheduled_departure_utc DESC;
    """

## Dashboard --> tab3.py
def get_query10(selected_iata=None):
    # Logic to filter the origin if a specific airport is selected
    if selected_iata == "ALL" or selected_iata is None:
        where_clause = "WHERE movement_type = 'Departure'"
    else:
        where_clause = f"WHERE origin_iata = '{selected_iata}' AND movement_type = 'Departure'"

    return f"""
    WITH UniquePhysicalJourneys AS (
        SELECT 
            origin_name,
            destination_name,
            flight_number,
            origin_iata,
            destination_iata,
            scheduled_departure_utc,
            ROW_NUMBER() OVER (
                PARTITION BY origin_iata, destination_iata, scheduled_departure_utc 
                ORDER BY flight_number ASC
            ) as row_num
        FROM flights
        {where_clause}
    )
    SELECT 
        f.origin_name,
        f.destination_name,
        COUNT(DISTINCT a.aircraft_model) AS aircraft_model_count
    FROM UniquePhysicalJourneys f
    JOIN aircraft a 
        ON f.flight_number = a.flight_number
    WHERE f.row_num = 1
    GROUP BY f.origin_name, f.destination_name
    HAVING COUNT(DISTINCT a.aircraft_model) > 2
    ORDER BY aircraft_model_count DESC;
    """


## dashboard --> "Delay status" --> tab1.py
def get_query11(selected_iata):
    # We always need a specific airport for this 'Source' analysis
    if selected_iata == "ALL":
        where_clause = "WHERE f.movement_type = 'Arrival' AND f.status = 'Arrived'"
    else:
        where_clause = f"WHERE f.destination_iata = '{selected_iata}' AND f.movement_type = 'Arrival' AND f.status = 'Arrived'"

    return f"""
        SELECT 
            f.origin_name AS origin_airport,
            f.origin_iata AS iata_code,
            COUNT(DISTINCT CASE 
                WHEN f.actual_arrival_utc > f.scheduled_arrival_utc 
                THEN f.flight_number || f.scheduled_arrival_utc 
            END) * 100.0 / 
            NULLIF(COUNT(DISTINCT f.flight_number, f.scheduled_arrival_utc), 0) AS percent_delayed,
            COUNT(DISTINCT f.flight_number, f.scheduled_arrival_utc) AS total_flights
        FROM flights f
        {where_clause}
        GROUP BY f.origin_name, f.origin_iata
        HAVING total_flights > 2 -- Filter out rare flights for better stats
        ORDER BY percent_delayed DESC
        LIMIT 10;
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



