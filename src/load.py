import pandas as pd



# Helper
def batch_insert(cursor, sql, values, batch_size=500):
    """Insert in chunks to keep query sizes manageable."""
    total = 0
    for i in range(0, len(values), batch_size):
        cursor.executemany(sql, values[i: i + batch_size])
        total += cursor.rowcount
    return total



# Loaders
def load_airport_data(cursor, conn, df_airport):
    """
    Upserts one airport row.
    transform_airport_data already guards missing columns and applies
    to_sql_safe(), so no re-checking needed here.
    """
    airport_cols = [
        "icao_code", "iata_code", "name", "city",
        "country", "continent", "latitude", "longitude", "timezone",
    ]
    row = tuple(df_airport[airport_cols].iloc[0].values)

    # FIX: update all non-key fields on duplicate, not just name
    cursor.execute(
        """INSERT INTO airport
               (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
               name      = VALUES(name),
               city      = VALUES(city),
               country   = VALUES(country),
               continent = VALUES(continent),
               latitude  = VALUES(latitude),
               longitude = VALUES(longitude),
               timezone  = VALUES(timezone)""",
        row,
    )
    conn.commit()
    print(f"  Upserted airport record for {df_airport['iata_code'].iloc[0]}.")


def load_flights_data(cursor, conn, flights_df):
    """
    Inserts flight rows. Uses INSERT IGNORE — add a UNIQUE key on
    (flight_number, scheduled_departure_utc) in the schema if you want
    status updates to be picked up on re-runs via ON DUPLICATE KEY UPDATE.
    transform already applied to_sql_safe(), no second conversion needed.
    """
    cols_flights = [
        "flight_number", "aircraft_registration",
        "origin_icao",   "origin_iata",   "origin_name",   "origin_country_code",
        "destination_icao", "destination_iata", "destination_name", "destination_country_code",
        "scheduled_departure_utc", "actual_departure_utc",
        "scheduled_arrival_utc",   "actual_arrival_utc",
        "status", "movement_type", "airline_code_iata", "airline_code_icao",
    ]
    sql = (
        f"INSERT IGNORE INTO flights ({', '.join(cols_flights)}) "
        f"VALUES ({', '.join(['%s'] * len(cols_flights))})"
    )
    vals    = [tuple(r) for r in flights_df[cols_flights].to_numpy()]
    inserted = batch_insert(cursor, sql, vals)
    conn.commit()
    print(f"  Inserted {inserted} new flight rows.")


def load_aircraft_data(cursor, conn, aircraft_df):
    """
    Inserts aircraft rows. transform already applied to_sql_safe().
    """
    cols_aircraft = [
        "flight_number","aircraft_registration", "aircraft_model",
        "airline_code_icao", "airline_code_iata", "airline_name", "aircraft_manufacturer",
    ]
    sql = (
        f"INSERT IGNORE INTO aircraft ({', '.join(cols_aircraft)}) "
        f"VALUES ({', '.join(['%s'] * len(cols_aircraft))})"
    )
    vals     = [tuple(r) for r in aircraft_df[cols_aircraft].to_numpy()]
    inserted = batch_insert(cursor, sql, vals)
    conn.commit()
    print(f"  Inserted {inserted} new aircraft rows.")


def load_airport_delays_data(cursor, conn, delay_df):
    """
    Inserts delay summary rows. transform already applied to_sql_safe().
    """
    cols_delay = [
        "airport_iata", "delay_date", "total_flights", "delayed_flights",
        "avg_delay_min", "median_delay_min", "canceled_flight",
    ]
    sql = (
        f"INSERT IGNORE INTO airport_delays ({', '.join(cols_delay)}) "
        f"VALUES ({', '.join(['%s'] * len(cols_delay))})"
    )
    vals     = [tuple(r) for r in delay_df[cols_delay].to_numpy()]
    inserted = batch_insert(cursor, sql, vals)
    conn.commit()
    print(f"  Inserted {inserted} delay records.")
