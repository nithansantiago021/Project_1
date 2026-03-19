import mysql.connector

DB_NAME = "air_tracker2"

TABLES = {
    "airport": """
        CREATE TABLE IF NOT EXISTS airport (
            airport_id  INT          AUTO_INCREMENT PRIMARY KEY,
            icao_code   VARCHAR(10)  UNIQUE,
            iata_code   VARCHAR(10)  UNIQUE,
            name        VARCHAR(255),
            city        VARCHAR(100),
            country     VARCHAR(100),
            continent   VARCHAR(100),
            latitude    DECIMAL(10,8),
            longitude   DECIMAL(11,8),
            timezone    VARCHAR(100)
        )
    """,
    "flights": """
        CREATE TABLE IF NOT EXISTS flights (
            flight_id                INT          AUTO_INCREMENT PRIMARY KEY,
            flight_number            VARCHAR(20),
            aircraft_registration    VARCHAR(20),
            origin_icao              VARCHAR(10),
            origin_iata              VARCHAR(10),
            origin_name              VARCHAR(255),
            origin_country_code      VARCHAR(10),
            destination_icao         VARCHAR(10),
            destination_iata         VARCHAR(10),
            destination_name         VARCHAR(255),
            destination_country_code VARCHAR(10),
            scheduled_departure_utc  DATETIME,
            actual_departure_utc     DATETIME,
            scheduled_arrival_utc    DATETIME,
            actual_arrival_utc       DATETIME,
            status                   VARCHAR(50),
            movement_type            VARCHAR(255),
            airline_code_iata        VARCHAR(10),
            airline_code_icao        VARCHAR(10)
        )
    """,
    "aircraft": """
        CREATE TABLE IF NOT EXISTS aircraft (
            aircraft_id           INT          AUTO_INCREMENT PRIMARY KEY,
            flight_number         VARCHAR(20),
            aircraft_registration VARCHAR(20)  UNIQUE,
            aircraft_model        VARCHAR(100),
            airline_code_iata     VARCHAR(10),
            airline_code_icao     VARCHAR(10),
            airline_name          VARCHAR(255),
            aircraft_manufacturer VARCHAR(50)
        )
    """,
    "airport_delays": """
        CREATE TABLE IF NOT EXISTS airport_delays (
            delay_id         INT         AUTO_INCREMENT PRIMARY KEY,
            airport_iata     VARCHAR(10),
            delay_date       DATE,
            total_flights    INT,
            delayed_flights  INT,
            avg_delay_min    INT,
            median_delay_min INT,
            canceled_flight  INT
        )
    """,
}


def create_schema(db_config):
    """
    Creates the air_tracker database and all tables.
    Accepts db_config dict so it works with both .env and
    user-supplied credentials from Streamlit.
    """
    conn   = None
    cursor = None
    try:
        # Connect without specifying database — it doesn't exist yet
        base_cfg = {k: v for k, v in db_config.items() if k != "database"}
        conn   = mysql.connector.connect(**base_cfg)
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"Database '{DB_NAME}' ready.")

        cursor.execute(f"USE {DB_NAME}")

        for table_name, ddl in TABLES.items():
            cursor.execute(ddl)
            print(f"Table '{table_name}' ready.")

        conn.commit()
        print("Schema setup complete.")

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    create_schema({
        "host":     os.getenv("DB_HOST"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    })
