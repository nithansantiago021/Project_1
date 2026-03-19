import os
import requests
import pandas as pd
import numpy as np
import json
import mysql.connector
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
from_time = "2026-03-07T08:00"
to_time   = "2026-03-07T20:00"

iata_list = ["DEL", "BLR", "DXB", "SIN", "HKG", "HND", "SYD", "SFO", "JFK", "CGH", "LHR", "CDG", "JNB"]



airport_default_data = {
    "DEL": ["VIDP", "New Delhi Indira Gandhi",       "in"],
    "BLR": ["VOBL", "Bangalore Kempegowda",          "in"],
    "DXB": ["OMDB", "Dubai International",           "ae"],
    "SIN": ["WSSS", "Singapore Changi",              "sg"],
    "HKG": ["VHHH", "Hong Kong Chek Lap Kok",        "hk"],
    "HND": ["RJTT", "Tokyo Haneda",                  "jp"],
    "SYD": ["YSSY", "Sydney Kingsford Smith",        "au"],
    "SFO": ["KSFO", "San Francisco International",   "us"],
    "JFK": ["KJFK", "New York John F Kennedy",       "us"],
    "CGH": ["SBSP", "São Paulo Congonhas",           "br"],
    "LHR": ["EGLL", "London Heathrow",               "gb"],
    "CDG": ["LFPG", "Paris Charles de Gaulle",       "fr"],
    "JNB": ["FAOR", "Johannesburg OR Tambo",         "za"],
}

db_config = {
    "host":     os.getenv("DB_HOST"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

COLUMN_MAPPING = {
    "number":                        "flight_number",
    "status":                        "status",
    "movement_type":                 "movement_type",
    "departure.airport.icao":        "origin_icao",
    "departure.airport.iata":        "origin_iata",
    "departure.airport.name":        "origin_name",
    "departure.airport.countryCode": "origin_country_code",
    "departure.airport.timeZone":    "origin_timezone",
    "departure.scheduledTime.utc":   "scheduled_departure_utc",
    "departure.scheduledTime.local": "scheduled_departure_local",
    "departure.revisedTime.utc":     "actual_departure_utc",
    "departure.revisedTime.local":   "actual_departure_local",
    "arrival.airport.icao":          "destination_icao",
    "arrival.airport.iata":          "destination_iata",
    "arrival.airport.name":          "destination_name",
    "arrival.airport.countryCode":   "destination_country_code",
    "arrival.airport.timeZone":      "destination_timezone",
    "arrival.scheduledTime.utc":     "scheduled_arrival_utc",
    "arrival.scheduledTime.local":   "scheduled_arrival_local",
    "arrival.revisedTime.utc":       "actual_arrival_utc",
    "arrival.revisedTime.local":     "actual_arrival_local",
    "aircraft.reg":                  "aircraft_registration",
    "aircraft.modeS":                "aircraft_mode_s",
    "aircraft.model":                "aircraft_model",
    "airline.name":                  "airline_name",
    "airline.iata":                  "airline_code_iata",
    "airline.icao":                  "airline_code_icao",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

# wrapper with exponential backoff instead of blind sleep(3).
def fetch_with_retry(url, headers, params=None, retries=3, backoff=2):
    """GET a URL, retrying on rate-limit (429) or server errors (5xx)."""
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=15)
            if res.status_code == 200:
                return res
            if res.status_code == 429:
                wait = backoff ** (attempt + 1)
                print(f"  Rate limited. Retrying in {wait}s… (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
            else:
                print(f"  HTTP {res.status_code} on attempt {attempt + 1}/{retries}")
                time.sleep(backoff)
        except requests.RequestException as e:
            print(f"  Request error: {e} (attempt {attempt + 1}/{retries})")
            time.sleep(backoff)
    return None


# Chunked executemany to avoid oversized single queries.
def batch_insert(cursor, sql, values, batch_size=500):
    """Insert in chunks to keep query sizes manageable."""
    total = 0
    for i in range(0, len(values), batch_size):
        chunk = values[i : i + batch_size]
        cursor.executemany(sql, chunk)
        total += cursor.rowcount
    return total


def clean_time_columns(df, cols):
    """Parse UTC datetime columns and replace 'NaT' strings with None."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
            df[col] = df[col].replace("NaT", np.nan)
    return df


def ensure_columns(df, columns):
    """Add any missing columns as NaN so downstream selects never KeyError."""
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df


def apply_airport_defaults(df, iata):
    """Fill missing origin/destination fields for the home airport."""
    defaults = airport_default_data[iata]
    arrival_defaults = {
        "arrival.airport.iata":        iata,
        "arrival.airport.icao":        defaults[0],
        "arrival.airport.name":        defaults[1],
        "arrival.airport.countryCode": defaults[2],
    }
    departure_defaults = {
        "departure.airport.iata":        iata,
        "departure.airport.icao":        defaults[0],
        "departure.airport.name":        defaults[1],
        "departure.airport.countryCode": defaults[2],
    }
    arr_mask = df["movement_type"] == "Arrival"
    dep_mask = df["movement_type"] == "Departure"

    arr_cols = [c for c in arrival_defaults   if c in df.columns]
    dep_cols = [c for c in departure_defaults if c in df.columns]

    if arr_cols:
        df.loc[arr_mask, arr_cols] = df.loc[arr_mask, arr_cols].fillna(arrival_defaults)
    if dep_cols:
        df.loc[dep_mask, dep_cols] = df.loc[dep_mask, dep_cols].fillna(departure_defaults)

    return df


# Delay computation extracted into a reusable function.
# Early arrivals become NaN (not 0) so they don't pollute averages.
def compute_delays(df):
    """
    Derive delay_min for each flight.
    - Positive  → real delay (minutes late)
    - Negative  → early arrival, stored as NaN
    - NaN actual_time → flight not yet operated, stored as NaN
    """
    df = df.copy()
    is_arrival = df["movement_type"] == "Arrival"

    df["sched_time"] = np.where(is_arrival, df["scheduled_arrival_utc"],  df["scheduled_departure_utc"])
    df["actual_time"] = np.where(is_arrival, df["actual_arrival_utc"],    df["actual_departure_utc"])

    df["sched_time"]  = pd.to_datetime(df["sched_time"],  errors="coerce")
    df["actual_time"] = pd.to_datetime(df["actual_time"], errors="coerce")
    df["delay_date"]  = df["sched_time"].dt.date

    delay_seconds     = (df["actual_time"] - df["sched_time"]).dt.total_seconds()
    # clip negatives (early arrivals) to NaN, not 0
    df["delay_min"]   = delay_seconds.div(60).where(delay_seconds > 0)

    df["is_delayed"]  = (df["status"] == "Delayed") | (df["delay_min"] > 0)
    # case-insensitive cancel check — robust to API string changes
    df["is_canceled"] = df["status"].str.lower().str.contains("cancel", na=False)

    return df


def build_delay_summary(df, iata):
    """Aggregate per-day delay stats for one airport."""
    summary = df.groupby("delay_date").agg(
        total_flights   = ("flight_number", "nunique"),
        delayed_flights = ("is_delayed",    "sum"),
        avg_delay_min   = ("delay_min",     lambda x: x[x > 0].mean()),
        median_delay_min= ("delay_min",     lambda x: x[x > 0].median()),
        canceled_flight = ("is_canceled",   "sum"),
    ).reset_index()

    summary.insert(0, "airport_iata", iata)
    summary.fillna(0, inplace=True)

    round_cols = ["delayed_flights", "avg_delay_min", "median_delay_min", "canceled_flight"]
    summary[round_cols] = summary[round_cols].round(0).astype(int)
    return summary


# ─────────────────────────────────────────────
# Main ETL loop
# ─────────────────────────────────────────────
try:
    conn   = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    for iata in iata_list:
        print(f"\n{'─'*50}")
        print(f"Processing {iata}…")

        # ── 1. Fetch airport metadata ──────────────────────
        air_url = f"https://aerodatabox.p.rapidapi.com/airports/iata/{iata}"
        air_res = fetch_with_retry(air_url, HEADERS)

        if air_res is None:
            print(f"  Could not fetch airport data for {iata}. Skipping.")
            continue

        airport_json = air_res.json()
        df_airport   = pd.json_normalize(airport_json).rename(columns={
            "icao":               "icao_code",
            "iata":               "iata_code",
            "fullName":           "name",
            "municipalityName":   "city",
            "country.name":       "country",
            "continent.name":     "continent",
            "location.lat":       "latitude",
            "location.lon":       "longitude",
            "timeZone":           "timezone",
        })

        # ── 2. Fetch flight data ───────────────────────────
        flight_url = (
            f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}"
            f"/{from_time}/{to_time}"
        )
        flight_res = fetch_with_retry(flight_url, HEADERS, params={"withLeg": "true"})

        if flight_res is None:
            print(f"  Could not fetch flight data for {iata}. Skipping.")
            continue

        data     = flight_res.json()
        filename = f"flight_data_{iata}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"  Saved raw data → {filename}")

        # ── 3. Separate arrivals / departures ─────────────
        df_arr = pd.json_normalize(data.get("arrivals",   []))
        df_dep = pd.json_normalize(data.get("departures", []))

        if not df_arr.empty:
            df_arr["movement_type"] = "Arrival"
        if not df_dep.empty:
            df_dep["movement_type"] = "Departure"

        if df_arr.empty and df_dep.empty:
            print(f"  No flight data for {iata} in this window. Skipping.")
            continue

        all_flights = pd.concat([df_arr, df_dep], ignore_index=True)

        # ── 4. Apply defaults & transform ─────────────────
        all_flights = apply_airport_defaults(all_flights, iata)
        all_flights = ensure_columns(all_flights, COLUMN_MAPPING.keys())

        clean_flights = all_flights[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)
        clean_flights = clean_time_columns(
            clean_flights,
            ["scheduled_departure_utc", "actual_departure_utc",
             "scheduled_arrival_utc",   "actual_arrival_utc"],
        )
        clean_flights["aircraft_manufacturer"] = (
            clean_flights["aircraft_model"].str.split().str[0]
        )

        # ── 5. Load → airport ──────────────────────────────
        airport_cols = ["icao_code", "iata_code", "name", "city",
                        "country", "continent", "latitude", "longitude", "timezone"]
        
        # Guard: only proceed if all expected columns exist
        missing_airport_cols = [c for c in airport_cols if c not in df_airport.columns]
        if missing_airport_cols:
            print(f"  Airport API response missing columns: {missing_airport_cols}. Skipping airport insert.")
        else:
            row = tuple(df_airport[airport_cols].iloc[0].values)
            cursor.execute(
                """INSERT INTO airport (icao_code, iata_code, name, city, country,
                                        continent, latitude, longitude, timezone)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE name=VALUES(name)""",
                row,
            )
            conn.commit()
            print(f"  Upserted airport record for {iata}.")

        # ── 6. Load → flights ──────────────────────────────
        cols_flights = [
            "flight_number", "aircraft_registration",
            "origin_icao",   "origin_iata",   "origin_name",   "origin_country_code",
            "destination_icao", "destination_iata", "destination_name", "destination_country_code",
            "scheduled_departure_utc", "actual_departure_utc",
            "scheduled_arrival_utc",   "actual_arrival_utc",
            "status", "movement_type", "airline_code_iata", "airline_code_icao",
        ]
        flights_sql_df = clean_flights[cols_flights].where(
            pd.notnull(clean_flights[cols_flights]), None
        )
        sql_flights = (
            f"INSERT IGNORE INTO flights ({', '.join(cols_flights)}) "
            f"VALUES ({', '.join(['%s'] * len(cols_flights))})"
        )
        vals_flights = [tuple(r) for r in flights_sql_df.to_numpy()]
        inserted = batch_insert(cursor, sql_flights, vals_flights)
        conn.commit()
        print(f"  Inserted {inserted} new flight rows for {iata}.")

        # ── 7. Load → aircraft ─────────────────────────────
        cols_aircraft = [
            "aircraft_registration", "aircraft_model",
            "airline_code_icao", "airline_code_iata", "airline_name", "aircraft_manufacturer",
        ]
        unique_aircraft = (
            clean_flights.dropna(subset=["aircraft_registration"])
            .drop_duplicates(subset=["aircraft_registration"], keep="first")
        )
        aircraft_sql_df = unique_aircraft[cols_aircraft].where(
            pd.notnull(unique_aircraft[cols_aircraft]), None
        )
        sql_aircraft = (
            f"INSERT IGNORE INTO aircraft ({', '.join(cols_aircraft)}) "
            f"VALUES ({', '.join(['%s'] * len(cols_aircraft))})"
        )
        vals_aircraft = [tuple(r) for r in aircraft_sql_df.to_numpy()]
        inserted = batch_insert(cursor, sql_aircraft, vals_aircraft)
        conn.commit()
        print(f"  Inserted {inserted} new aircraft rows for {iata}.")

        # ── 8. Load → airport_delays ───────────────────────
        clean_flights = compute_delays(clean_flights)
        delay_summary = build_delay_summary(clean_flights, iata)

        cols_delay = [
            "airport_iata", "delay_date", "total_flights", "delayed_flights",
            "avg_delay_min", "median_delay_min", "canceled_flight",
        ]
        delay_sql_df = delay_summary[cols_delay].where(
            pd.notnull(delay_summary[cols_delay]), None
        )
        sql_delay = (
            f"INSERT IGNORE INTO airport_delays ({', '.join(cols_delay)}) "
            f"VALUES ({', '.join(['%s'] * len(cols_delay))})"
        )
        vals_delay = [tuple(r) for r in delay_sql_df.to_numpy()]
        inserted = batch_insert(cursor, sql_delay, vals_delay)
        conn.commit()
        print(f"  Inserted {inserted} delay records for {iata}.")

        # Polite pause between airports (respects API rate limits)
        time.sleep(1)

except Exception as e:
    print(f"\nFatal error: {e}")
    raise

finally:
    if "conn" in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")