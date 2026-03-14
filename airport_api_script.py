import os
import requests
import pandas as pd
import mysql.connector
import time  #To prevent rate limiting
from dotenv import load_dotenv

load_dotenv()


API_KEY = os.getenv("API_KEY")
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}
iata_list = ["DEL","BLR","DXB","SIN","HKG","HND","SYD","SFO","JFK","CGH","LHR","CDG","JNB"]

db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

#Open the Connection
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to MySQL successfully.")

    for i in iata_list:
        print(f"Fetching data for {i}...")
        
        #Airport Data Extraction
        air_url = f"https://aerodatabox.p.rapidapi.com/airports/iata/{i}"
        air_res = requests.get(air_url, headers=HEADERS)
        
        if air_res.status_code != 200:
            print(f"Failed to fetch {i}. Skipping.")
            continue
            
        airport_json = air_res.json()

        #Flattening and Cleaning
        df = pd.json_normalize(airport_json)

        df_cleaned = df.rename(columns={
            'icao': 'icao_code',
            'iata': 'iata_code',
            'fullName': 'name',
            'municipalityName': 'city',
            'country.name': 'country',
            'continent.name': 'continent',
            'location.lat': 'latitude',
            'location.lon': 'longitude',
            'timeZone': 'timezone'
        })

        final_columns = ['icao_code', 'iata_code', 'name', 'city', 'country', 'continent', 'latitude', 'longitude', 'timezone']
        
        #only required columns exist
        df_final = df_cleaned[final_columns]

        #SQL Insertion
        sql = """
        INSERT INTO airport (icao_code, iata_code, name, city, country, continent, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE name=VALUES(name)
        """

        val = tuple(df_final.iloc[0].values)
        cursor.execute(sql, val)
        conn.commit()
        
        print(f"Integrated {i} into SQL.")
        
        #API Rate Limit (Wait 1.5 seconds)
        time.sleep(1.5)

except Exception as e:
    print(f"Major Error: {e}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")