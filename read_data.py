import sqlite3
import pandas as pd

# Pandas can read SQL directly into a DataFrame!
with sqlite3.connect('weather_warehouse.db') as conn:
    df = pd.read_sql("SELECT * FROM saskatoon_forecast", conn)

print(df)