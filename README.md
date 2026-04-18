# Automated-weather-pipeline


An automated, end-to-end Data Engineering pipeline that extracts daily weather forecasts, transforms the data, and loads it idempotently into a cloud-hosted PostgreSQL data warehouse. Orchestrated entirely in the cloud using GitHub Actions CI/CD.

## Architecture & Tech Stack
* **Language:** Python 3.10
* **Extraction:** [Open-Meteo API](https://open-meteo.com/) (Requests)
* **Transformation:** Pandas
* **Data Warehouse:** PostgreSQL (Hosted via [Supabase](https://supabase.com/))
* **Database Connector:** SQLAlchemy & psycopg2
* **Orchestration / CI/CD:** GitHub Actions
* **Observability:** Python `logging` library

## How It Works
1. **Extract:** Connects to the Open-Meteo API to pull the live 7-day forecast for Saskatoon, SK (Max Temp, Min Temp, Precipitation).
2. **Transform:** Cleans the JSON payload, normalizes column names, and injects pipeline metadata (execution timestamps).
3. **Load (Idempotent):** Securely connects to a Supabase PostgreSQL vault using environment variables. It employs a "Delete-Then-Append" SQL strategy to ensure the database can be updated infinitely without creating duplicate rows.
4. **Automate:** A GitHub Actions Cron Job spins up an ephemeral Ubuntu server every day at 14:00 UTC (8:00 AM CST), installs dependencies, injects secure database credentials, executes the ETL script, and shuts down.

## Run it Locally

If you want to run this pipeline on your local machine, you will need to connect it to your own PostgreSQL database.

**1. Clone the repository:**
`git clone https://github.com/SaketShah/automated-weather-pipeline.git`
`cd automated-weather-pipeline`

**2. Install dependencies:**
`pip install -r requirements.txt`

**3. Set your Environment Variable:**
The script requires a `DATABASE_URL` to securely connect to the database. 
* **Mac/Linux:** `export DATABASE_URL="postgresql://username:password@your-host:6543/postgres"`
* **Windows (Command Prompt):** `set DATABASE_URL="postgresql://username:password@your-host:6543/postgres"`
* **Windows (PowerShell):** `$env:DATABASE_URL="postgresql://username:password@your-host:6543/postgres"`

**4. Execute the pipeline:**
`python weather_pipeline.py`

**5. Check the logs:**
A `weather_pipeline.log` file will be generated automatically in your directory to track execution status and catch any database/API errors.
