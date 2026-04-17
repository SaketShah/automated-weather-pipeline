import requests
import pandas as pd
import sqlite3
from datetime import datetime
import logging

logging.basicConfig(
    filename='weather_pipeline.log',     # The file where logs will be saved
    level=logging.INFO,                  # Record INFO, WARNING, ERROR, and CRITICAL
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def run_weather_etl():
    """Extracts, transforms, and loads live weather data."""
    logging.info("--- Starting Weather Pipeline Run ---")
    
    # 1. EXTRACT

    logging.info("Extracting live weather for Saskatoon from Open-Meteo...")
    url = "https://api.open-meteo.com/v1/forecast?latitude=52.1332&longitude=-106.6700&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=America%2FChicago"
    
    try:
        response = requests.get(url)
        # If the API crashes, we log it as an ERROR and stop the script safely
        if response.status_code != 200:
            logging.error(f"Failed to fetch data! API returned status code: {response.status_code}")
            return
            
        raw_data = response.json()
    except Exception as e:
        # If the internet goes down completely, this catches it!
        logging.critical(f"Total connection failure: {e}")
        return
    
    # 2. TRANSFORM

    logging.info("Transforming forecast data...")
    daily_data = raw_data['daily']
    df = pd.DataFrame(daily_data)
    
    df = df.rename(columns={
        'time': 'date',
        'temperature_2m_max': 'max_temp_c',
        'temperature_2m_min': 'min_temp_c',
        'precipitation_sum': 'precipitation_mm'
    })
    
    df['pipeline_run_time'] = datetime.now().strftime('%Y-%m-%d %H:%M')

  
    # 3. LOAD (Idempotent)

    logging.info("Loading idempotently into weather_warehouse.db...")
    try:
        with sqlite3.connect('weather_warehouse.db') as conn:
            cursor = conn.cursor()
            
            new_dates = df['date'].tolist()
            placeholders = ', '.join(['?'] * len(new_dates))
            delete_query = f"DELETE FROM saskatoon_forecast WHERE date IN ({placeholders})"
            
            try:
                cursor.execute(delete_query, new_dates)
                logging.info(f"Cleared out any existing data for the {len(new_dates)} incoming dates.")
                
            except sqlite3.OperationalError as e:
                # We save the error as 'e' and look at the exact text inside it
                if "no such table" in str(e):
                    logging.info("First run detected: Table doesn't exist yet. Skipping delete step.")
                    pass 
                else:
                    raise e
            # ... moving on to append ...------
            
            # Now it safely moves on to create the table and append the data!
            df.to_sql('saskatoon_forecast', conn, index=False, if_exists='append')
            logging.info(f"Successfully appended {len(df)} new rows to the vault.")
            
    except Exception as e:
        # This will only catch REAL database failures now
        logging.error(f"Critical database failure: {e}")
        return
    
    logging.info("--- Pipeline Complete! ---")


# EXECUTING THE PIPELINE

if __name__ == "__main__":
    print("GitHub Actions execution starting...")
    run_weather_etl()
    print("GitHub Actions execution finished!")