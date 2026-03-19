import pandas as pd
import numpy as np

# Low-level helpers
def clean_time_columns(df, cols):
    for col in cols:
        if col in df.columns:
            # Normalise the trailing-Z format → pandas-parseable ISO string
            df[col] = df[col].astype(str).str.replace(r"Z$", "+00:00", regex=True)
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            # Drop timezone info — store as naive UTC for MySQL DATETIME columns
            df[col] = df[col].dt.tz_localize(None)
            # Keep as datetime objects for internal calculations; convert to string at the very end
    return df


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df


def to_sql_safe(df):
    # Convert datetime objects to strings for SQL before returning
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S").replace("NaT", None)
    return df.where(pd.notnull(df), None)


# Schema compatibility
def _normalise_movement_schema(df, movement_type):
    movement_cols = [c for c in df.columns if c.startswith("movement.")]
    if not movement_cols:
        return df   

    target_prefix = "departure" if movement_type == "Departure" else "arrival"
    rename_map = {
        col: col.replace("movement.", target_prefix + ".", 1)
        for col in movement_cols
    }
    return df.rename(columns=rename_map)


# Domain helpers
def apply_airport_defaults(df, iata, airport_default_data):
    if iata not in airport_default_data:
        return df

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


def compute_delays(df):
    df = df[df["movement_type"] == "Arrival"].copy()

    # Ensure they are datetime for math
    df["sched_time"]  = pd.to_datetime(df["sched_time"],  errors="coerce")
    df["actual_time"] = pd.to_datetime(df["actual_time"], errors="coerce")
    
    # helper for grouping
    df["delay_date"]  = df["sched_time"].dt.date

    # Calculate delay in minutes
    delay_seconds    = (df["actual_time"] - df["sched_time"]).dt.total_seconds()
    df["delay_min"]  = delay_seconds.div(60).fillna(0) # Use 0 for math, filter later

    # Flags
    df["is_delayed"]  = (df["status"] == "Delayed") | (df["delay_min"] > 0)
    df["is_canceled"] = df["status"].str.lower().str.contains("cancel", na=False)

    return df


def build_delay_summary(df, iata):
    # 1. Deduplicate to handle codesharing
    # A unique flight is defined by Origin, Destination, and Scheduled Time
    unique_flights = df.sort_values("delay_min", ascending=False).drop_duplicates(
        subset=["origin_name", "destination_name", "sched_time"]
    )

    # 2. Aggregate cleaned data
    summary = unique_flights.groupby("delay_date").agg(
        total_flights    = ("sched_time",    "nunique"),
        delayed_flights  = ("is_delayed",    "sum"),
        avg_delay_min    = ("delay_min",     lambda x: x[x > 0].mean()),
        median_delay_min = ("delay_min",     lambda x: x[x > 0].median()),
        canceled_flight  = ("is_canceled",   "sum"),
    ).reset_index()

    summary.insert(0, "airport_iata", iata)
    summary.fillna(0, inplace=True)

    # Clean up types for SQL
    round_cols = ["delayed_flights", "avg_delay_min", "median_delay_min", "canceled_flight"]
    summary[round_cols] = summary[round_cols].round(0).astype(int)
    return summary


# Public transform functions
def transform_airport_data(airport_json):
    airport_cols = ["icao_code", "iata_code", "name", "city",
                    "country", "continent", "latitude", "longitude", "timezone"]

    df = pd.json_normalize(airport_json).rename(columns={
        "icao":             "icao_code",
        "iata":             "iata_code",
        "fullName":         "name",
        "municipalityName": "city",
        "country.name":     "country",
        "continent.name":   "continent",
        "location.lat":     "latitude",
        "location.lon":     "longitude",
        "timeZone":         "timezone",
    })

    missing = [c for c in airport_cols if c not in df.columns]
    if missing:
        return None, missing

    return to_sql_safe(df[airport_cols]), []


def transform_flight_data(flight_data, iata, airport_default_data, column_mapping):
    df_arr = pd.json_normalize(flight_data.get("arrivals",   []))
    df_dep = pd.json_normalize(flight_data.get("departures", []))

    if not df_arr.empty:
        df_arr["movement_type"] = "Arrival"
        df_arr = _normalise_movement_schema(df_arr, "Arrival")
    if not df_dep.empty:
        df_dep["movement_type"] = "Departure"
        df_dep = _normalise_movement_schema(df_dep, "Departure")

    if df_arr.empty and df_dep.empty:
        return None, None, None

    all_flights = pd.concat([df_arr, df_dep], ignore_index=True)
    all_flights = apply_airport_defaults(all_flights, iata, airport_default_data)
    all_flights = ensure_columns(all_flights, column_mapping.keys())

    clean_flights = all_flights[list(column_mapping.keys())].rename(columns=column_mapping)
    
    # Standardize time to datetime objects
    clean_flights = clean_time_columns(
        clean_flights,
        ["scheduled_departure_utc", "actual_departure_utc",
         "scheduled_arrival_utc",   "actual_arrival_utc"],
    )
    
    clean_flights["aircraft_manufacturer"] = (
        clean_flights["aircraft_model"].str.split().str[0]
    )

    # 1. flights table (We keep all records here, including codeshares)
    cols_flights = [
        "flight_number", "aircraft_registration",
        "origin_icao",   "origin_iata",   "origin_name",   "origin_country_code",
        "destination_icao", "destination_iata", "destination_name", "destination_country_code",
        "scheduled_departure_utc", "actual_departure_utc",
        "scheduled_arrival_utc",   "actual_arrival_utc",
        "status", "movement_type", "airline_code_iata", "airline_code_icao",
    ]
    flights_df = to_sql_safe(clean_flights[cols_flights].copy())

    # 2. aircraft table
    cols_aircraft = [
        "flight_number",
        "aircraft_registration", "aircraft_model",
        "airline_code_icao", "airline_code_iata", "airline_name", "aircraft_manufacturer",
    ]
    unique_aircraft = (
        clean_flights
        .dropna(subset=["aircraft_registration"])
        .drop_duplicates(subset=["aircraft_registration"], keep="first")
    )
    aircraft_df = to_sql_safe(unique_aircraft[cols_aircraft].copy())

    # 3. airport_delays table (We deduplicate inside build_delay_summary)
    cols_delay = [
        "airport_iata", "delay_date", "total_flights", "delayed_flights",
        "avg_delay_min", "median_delay_min", "canceled_flight",
    ]
    processed_df = compute_delays(clean_flights)
    delay_summary = build_delay_summary(processed_df, iata)
    delay_df = to_sql_safe(delay_summary[cols_delay])

    return flights_df, aircraft_df, delay_df