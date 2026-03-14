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

# Data
from_time = "2026-03-07T08:00"
to_time = "2026-03-07T20:00"
iata_list = ["DEL","BLR","DXB","HKG","HND","SYD","SFO","JFK","CGH","LHR","CDG","JNB","SIN"]

db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}


airport_default_data = {
    'DEL':['VOBL','New Delhi Indira Gandhi', 'in'],
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

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    for iata in iata_list:
        print(f"Processing {iata}...")
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}/{from_time}/{to_time}"
        response = requests.get(url, headers=HEADERS, params={"withLeg": "true"})
        data = response.json()

        filename = f"flight_data_{iata}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"Successfully saved data to {filename}")

        # separation arrivals and departures
        df_arrivals = pd.json_normalize(data.get('arrivals', []))
        if not df_arrivals.empty:
            df_arrivals['movement_type'] = 'Arrival'
            
        df_departures = pd.json_normalize(data.get('departures', []))
        if not df_departures.empty:
            df_departures['movement_type'] = 'Departure'

        # Safely skip if API returned absolutely no data for this window
        if df_arrivals.empty and df_departures.empty:
            print(f"No flight data for {iata} in this time window. Skipping...")
            continue

        # concatenate the table
        all_flights_df = pd.concat([df_arrivals, df_departures], ignore_index=True)

        # Define your default fill values in dictionaries
        arrival_defaults = {
            'arrival.airport.iata': iata,
            'arrival.airport.icao': airport_default_data[iata][0],
            'arrival.airport.name': airport_default_data[iata][1],
            'arrival.airport.countryCode': airport_default_data[iata][2]
        }

        departure_defaults = {
            'departure.airport.iata': iata,
            'departure.airport.icao': airport_default_data[iata][0],
            'departure.airport.name': airport_default_data[iata][1],
            'departure.airport.countryCode': airport_default_data[iata][2]
        }

        # Create your masks
        arrival_mask = all_flights_df['movement_type'] == 'Arrival'
        departure_mask = all_flights_df['movement_type'] == 'Departure'

        # Apply the defaults
        # Check if columns exist before filling to prevent KeyErrors
        arr_cols = [c for c in arrival_defaults.keys() if c in all_flights_df.columns]
        if arr_cols:
            all_flights_df.loc[arrival_mask, arr_cols] = all_flights_df.loc[arrival_mask, arr_cols].fillna(arrival_defaults)

        dep_cols = [c for c in departure_defaults.keys() if c in all_flights_df.columns]
        if dep_cols:
            all_flights_df.loc[departure_mask, dep_cols] = all_flights_df.loc[departure_mask, dep_cols].fillna(departure_defaults)

        mapping_total = {
            'number': 'flight_number',
            'status': 'status',
            'movement_type': 'movement_type',
            'departure.airport.icao': 'origin_icao',
            'departure.airport.iata': 'origin_iata',
            'departure.airport.name': 'origin_name',
            'departure.airport.countryCode': 'origin_country_code',
            'departure.airport.timeZone': 'origin_timezone',
            'departure.scheduledTime.utc': 'scheduled_departure_utc',
            'departure.scheduledTime.local': 'scheduled_departure_local',
            'departure.revisedTime.utc': 'actual_departure_utc',
            'departure.revisedTime.local': 'actual_departure_local',
            'arrival.airport.icao': 'destination_icao',
            'arrival.airport.iata': 'destination_iata',
            'arrival.airport.name': 'destination_name',
            'arrival.airport.countryCode': 'destination_country_code',
            'arrival.airport.timeZone': 'destination_timezone',
            'arrival.scheduledTime.utc': 'scheduled_arrival_utc',
            'arrival.scheduledTime.local': 'scheduled_arrival_local',
            'arrival.revisedTime.utc': 'actual_arrival_utc',
            'arrival.revisedTime.local': 'actual_arrival_local',
            'aircraft.reg': 'aircraft_registration',
            'aircraft.modeS': 'aircraft_mode_s',
            'aircraft.model': 'aircraft_model',
            'airline.name': 'airline_name',
            'airline.iata': 'airline_code_iata',
            'airline.icao': 'airline_code_icao'
        }

        # Ensure all mapped columns exist in the dataframe to prevent KeyError
        for col in mapping_total.keys():
            if col not in all_flights_df.columns:
                all_flights_df[col] = np.nan

        # Extract only required columns
        clean_df_flights = all_flights_df[list(mapping_total.keys())].rename(columns=mapping_total)

        # Cleaning time format
        for col in ['scheduled_departure_utc','actual_departure_utc','scheduled_arrival_utc','actual_arrival_utc']:
            clean_df_flights[col] = pd.to_datetime(clean_df_flights[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            # Prevent string 'NaT' from causing SQL crashes
            clean_df_flights[col] = clean_df_flights[col].replace('NaT', np.nan)

        # Extract aircraft manufacturer
        clean_df_flights['aircraft_manufacturer'] = clean_df_flights['aircraft_model'].str.split().str[0]

        ########################## flights table ######################################
        cols_flights = [
            'flight_number', 'aircraft_registration', 'origin_icao', 'origin_iata', 'origin_name', 'origin_country_code' , 
            'destination_icao','destination_iata', 'destination_name', 'destination_country_code',
            'scheduled_departure_utc', 'actual_departure_utc', 'scheduled_arrival_utc', 'actual_arrival_utc', 
            'status', 'movement_type' ,'airline_code_iata', 'airline_code_icao'
        ]

        # Convert NaNs to None right before SQL insertion
        flights_sql_df = clean_df_flights[cols_flights].where(pd.notnull(clean_df_flights[cols_flights]), None)
        
        sql_flights = f"""
        INSERT IGNORE INTO flights ({", ".join(cols_flights)})
        VALUES ({", ".join(['%s'] * len(cols_flights))})
        """
        val_flights = [tuple(x) for x in flights_sql_df.to_numpy()]

        cursor.executemany(sql_flights, val_flights)
        conn.commit()
        print(f"Successfully integrated {cursor.rowcount} new flights of {iata} into SQL!")

        ########################## aircraft table ######################################
        unique_aircraft_df = clean_df_flights.dropna(subset=['aircraft_registration']).drop_duplicates(subset=['aircraft_registration'], keep='first')

        cols_aircraft = [
            'aircraft_registration', 'aircraft_model',
            'airline_code_icao','airline_code_iata','airline_name','aircraft_manufacturer'
        ]

        aircraft_sql_df = unique_aircraft_df[cols_aircraft].where(pd.notnull(unique_aircraft_df[cols_aircraft]), None)

        sql_aircraft = f"""
        INSERT IGNORE INTO aircraft ({", ".join(cols_aircraft)})
        VALUES ({", ".join(['%s'] * len(cols_aircraft))})
        """
        val_aircraft = [tuple(x) for x in aircraft_sql_df.to_numpy()]

        cursor.executemany(sql_aircraft, val_aircraft)
        conn.commit()
        print(f"Successfully integrated {cursor.rowcount} new aircrafts of {iata} into SQL!")

        ########################## airport_delay ######################################
        main_df = clean_df_flights.copy()
        is_arrival = main_df['movement_type'] == 'Arrival'

        main_df['sched_time'] = np.where(is_arrival, main_df['scheduled_arrival_utc'], main_df['scheduled_departure_utc'])
        main_df['actual_time'] = np.where(is_arrival, main_df['actual_arrival_utc'], main_df['actual_departure_utc'])

        main_df['sched_time'] = pd.to_datetime(main_df['sched_time'], errors='coerce')
        main_df['actual_time'] = pd.to_datetime(main_df['actual_time'], errors='coerce')
        main_df['delay_date'] = main_df['sched_time'].dt.date

        main_df['delay_min'] = (main_df['actual_time'] - main_df['sched_time']).dt.total_seconds() / 60
        
        # Keep positive delays, turn negative (early) flights to 0, and retain NaNs for unflown flights
        main_df['delay_min'] = np.where(main_df['delay_min'] > 0, main_df['delay_min'], 0)
        main_df.loc[main_df['actual_time'].isna(), 'delay_min'] = np.nan

        main_df['is_delayed'] = (main_df['status'] == 'Delayed') | (main_df['delay_min'] > 0)
        main_df['is_canceled'] = main_df['status'].isin(['Canceled', 'CanceledUncertain'])

        airport_delay_df = main_df.groupby('delay_date').agg(
            **{
                'total_flights': ('flight_number', 'nunique'),
                'delayed_flights': ('is_delayed', 'sum'),
                'avg_delay_min': ('delay_min', lambda x: x[x > 0].mean()),
                'median_delay_min': ('delay_min', lambda x: x[x > 0].median()),
                'canceled_flight': ('is_canceled', 'sum')
            }
        ).reset_index()

        airport_delay_df.insert(0, 'airport_iata', iata)
        airport_delay_df.fillna(0, inplace=True)
        
        cols_to_int = ['delayed_flights', 'avg_delay_min', 'median_delay_min', 'canceled_flight']
        airport_delay_df[cols_to_int] = airport_delay_df[cols_to_int].round(0).astype(int)

        cols_delay = [
            'airport_iata','delay_date','total_flights', 'delayed_flights',
            'avg_delay_min','median_delay_min','canceled_flight'
        ]

        delay_sql_df = airport_delay_df[cols_delay].where(pd.notnull(airport_delay_df[cols_delay]), None)

        sql_delay = f"""
        INSERT IGNORE INTO airport_delays ({", ".join(cols_delay)})
        VALUES ({", ".join(['%s'] * len(cols_delay))})
        """
        val_delay = [tuple(x) for x in delay_sql_df.to_numpy()]

        cursor.executemany(sql_delay, val_delay)
        conn.commit()
        print(f"Successfully integrated {cursor.rowcount} delay records of {iata} in SQL\n")

        time.sleep(3)

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
