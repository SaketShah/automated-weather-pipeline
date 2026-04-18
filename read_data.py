import pandas as pd
import os
from sqlalchemy import create_engine

# Make sure you set your DATABASE_URL environment variable first!
db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)

# Read the cloud table directly into a Pandas DataFrame!
df = pd.read_sql("SELECT * FROM saskatoon_forecast", engine)

print("Opening the Cloud Vault...\n")
print(df)
