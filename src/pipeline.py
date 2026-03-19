import os
import mysql.connector
from dotenv import load_dotenv

from . import create_schema, extraction, transformation, load

load_dotenv()

# Used when the API returns partial data for a flight's origin/destination.
AIRPORT_DEFAULT_DATA = {
    'DEL':['VIDP','New Delhi Indira Gandhi', 'in'],
    'BLR':['VOBL','Banglaore Bengaluru', 'in'],
    'DXB':['OMDB', 'Dubai', 'ae'],
    "HKG": ['VHHH', 'Hong Kong Chek Lap Kok', 'hk'],
    "HND": ['RJTT', 'Tokyo Haneda', 'jp'],
    "SYD": ['YSSY', 'Sydney Kingsford Smith', 'au'],
    "SFO": ['KSFO','San Francisco', 'us'],
    "JFK": ['KJFK', 'New York John F Kennedy', 'us'],
    "CGH": ['SBSP', 'São Paulo Congonhas', 'br'],
    "LHR": ['EGLL','London Heathrow','gb'],
    "CDG": ['LFPG','Paris Charles de Gaulle','fr'],
    "JNB": ['FAOR',"Jo'anna Johannesburg OR Tambo", 'za'],
    "SIN": ['WSSS', 'Singapore', 'sg']
}

COLUMN_MAPPING = {
    # Identity
    "number":                           "flight_number",
    "status":                           "status",
    "movement_type":                    "movement_type",

    # Departure airport
    "departure.airport.icao":           "origin_icao",
    "departure.airport.iata":           "origin_iata",
    "departure.airport.name":           "origin_name",
    "departure.airport.countryCode":    "origin_country_code",

    # Departure times 
    "departure.scheduledTime.utc":       "scheduled_departure_utc",
    "departure.revisedTime.utc":          "actual_departure_utc",

    # Arrival airport 
    "arrival.airport.icao":             "destination_icao",
    "arrival.airport.iata":             "destination_iata",
    "arrival.airport.name":             "destination_name",
    "arrival.airport.countryCode":      "destination_country_code",

    # Arrival times 
    "arrival.scheduledTime.utc":         "scheduled_arrival_utc",
    "arrival.revisedTime.utc":            "actual_arrival_utc",

    # Aircraft & airline
    "aircraft.reg":                     "aircraft_registration",
    "aircraft.model":                   "aircraft_model",
    "airline.name":                     "airline_name",
    "airline.iata":                     "airline_code_iata",
    "airline.icao":                     "airline_code_icao",
}


def run_pipeline(api_key, iata_code, from_time, to_time):
    """
    Runs the full ETL pipeline for a given airport and time range.
    """
    print("Starting ETL pipeline...")

    db_config = {
        "host":     os.getenv("DB_HOST"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": create_schema.DB_NAME
    }

    # 1. Schema 
    try:
        create_schema.create_schema(db_config)  
    except mysql.connector.Error as e:
        print(f"Database schema setup failed: {e}")
        return

    #  2. Extraction 
    print(f"--- Processing {iata_code} ---")

    airport_json = extraction.fetch_airport_data(api_key, iata_code)
    flight_data  = extraction.fetch_flight_data(api_key, iata_code, from_time, to_time)

    if not airport_json or not flight_data:
        print(f"Extraction failed for {iata_code}. Skipping.")
        return

    if iata_code not in AIRPORT_DEFAULT_DATA:
        country_code = airport_json.get("country", {}).get("code", "")
        AIRPORT_DEFAULT_DATA[iata_code] = [
            airport_json.get("icao",     ""),
            airport_json.get("fullName", iata_code),  
            country_code.lower(),
        ]

    #  3. Transformation 
    df_airport, missing_cols = transformation.transform_airport_data(airport_json)
    if missing_cols:
        print(f"Airport API response missing columns {missing_cols} for {iata_code}. Skipping.")
        return

    flights_df, aircraft_df, delay_df = transformation.transform_flight_data(
        flight_data, iata_code, AIRPORT_DEFAULT_DATA, COLUMN_MAPPING
    )

    if flights_df is None:
        print(f"No flight data to process for {iata_code}. Skipping.")
        return

    #  4. Load 
    conn = None
    try:
        conn   = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        print("Loading data into database...")

        load.load_airport_data(cursor, conn, df_airport)
        load.load_flights_data(cursor, conn, flights_df)
        load.load_aircraft_data(cursor, conn, aircraft_df)
        load.load_airport_delays_data(cursor, conn, delay_df)

        print("Data loading complete.")

    except mysql.connector.Error as e:
        print(f"Database error: {e}")

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")

    print("ETL pipeline finished.")


if __name__ == "__main__":
    API_KEY   = os.getenv("API_KEY")
    IATA_CODE = "LHR"
    FROM_TIME = "2026-03-14T08:00"
    TO_TIME   = "2026-03-14T20:00"

    run_pipeline(API_KEY, IATA_CODE, FROM_TIME, TO_TIME)
