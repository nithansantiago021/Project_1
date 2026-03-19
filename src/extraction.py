import os
import requests
import time
import json

def fetch_with_retry(url, headers, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, params = {"withLeg": "true"})
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

def fetch_airport_data(api_key, iata_code):
    url = f"https://aerodatabox.p.rapidapi.com/airports/iata/{iata_code}"
    headers = {"x-rapidapi-key": api_key, "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}
    
    print(f"Fetching airport data for {iata_code}...")
    res = fetch_with_retry(url, headers)
    
    if res:
        print(f"Successfully fetched airport data for {iata_code}.")
        time.sleep(1.5)  # Rate limit
        return res.json()
    else:
        print(f"Failed to fetch airport data for {iata_code}.")
        return None

def fetch_flight_data(api_key, iata_code, from_time, to_time):
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata_code}/{from_time}/{to_time}"
    headers = {"x-rapidapi-key": api_key, "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}
    
    print(f"Fetching flight data for {iata_code}...")
    res = fetch_with_retry(url, headers)
    
    if res:
        print(f"Successfully fetched flight data for {iata_code}.")
        data = res.json()
        
        # Save raw data to a file
        filename = f"flight_data_{iata_code}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Saved raw data to {filename}")
        
        return data
    else:
        print(f"Failed to fetch flight data for {iata_code}.")
        return None
