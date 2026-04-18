import requests
import pandas as pd
import os
from sqlalchemy import create_engine, text
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

    logging.info("Connecting to Supabase Cloud Vault...")
    
    # 1. Securely fetch the database URL from GitHub Secrets
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logging.critical("No DATABASE_URL found! Pipeline aborted.")
        return

    # 2. Create the connection engine
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as conn:
            
            # --- The Idempotent Delete ---
            # Postgres uses a slightly different syntax for lists than SQLite
            dates_list = df['date'].tolist()
            delete_query = text("DELETE FROM saskatoon_forecast WHERE date = ANY(:dates)")
            
            try:
                # We execute the delete and force a commit
                conn.execute(delete_query, {"dates": dates_list})
                conn.commit()
                logging.info(f"Cleared out old data for {len(dates_list)} dates.")
            except Exception as e:
                # If the table doesn't exist, Postgres throws an error. We catch it and rollback!
                conn.rollback()
                logging.info("First run detected: Table doesn't exist yet. Skipping delete.")

            # --- The Append ---
            df.to_sql('saskatoon_forecast', engine, index=False, if_exists='append')
            logging.info(f"Successfully beamed {len(df)} rows to the cloud!")
            
    except Exception as e:
        logging.error(f"Critical Cloud Database failure: {e}")
        return
    
    logging.info("--- Pipeline Complete! ---")


# EXECUTING THE PIPELINE

if __name__ == "__main__":
    print("GitHub Actions execution starting...")
    run_weather_etl()
    print("GitHub Actions execution finished!")
