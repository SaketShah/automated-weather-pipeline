# Saskatoon Weather ETL Pipeline

An automated, end-to-end Data Engineering pipeline that extracts daily weather forecasts, transforms the data, and loads it idempotently into a local data warehouse. Orchestrated entirely in the cloud using GitHub Actions CI/CD.

## Architecture & Tech Stack
* **Language:** Python 3.10
* **API/Extraction:** [Open-Meteo API](https://open-meteo.com/) (Requests)
* **Transformation:** Pandas
* **Data Warehouse:** SQLite3
* **Orchestration / CI/CD:** GitHub Actions
* **Observability:** Python `logging` library

## How It Works
1. **Extract:** Connects to the Open-Meteo API to pull the 7-day forecast for Saskatoon, SK (Max Temp, Min Temp, Precipitation).
2. **Transform:** Cleans the JSON payload, normalizes column names, and injects pipeline metadata (execution timestamps).
3. **Load (Idempotent):** Employs a "Delete-Then-Append" strategy to ensure the database can be updated infinitely without creating duplicate rows.
4. **Automate:** A GitHub Actions Cron Job spins up an Ubuntu server every day at 8:00 AM UTC to execute the script autonomously.
