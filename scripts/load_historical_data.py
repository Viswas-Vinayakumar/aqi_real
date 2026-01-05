import pandas as pd
import os
from supabase import create_client
from dotenv import load_dotenv
import supabase

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# 1. Load Excel
df = pd.read_excel("data/aqi_historical.xlsx")

# 2. Rename columns to match DB (example)
df = df.rename(columns={
    "Date": "observed_date",
    "Overall AQI Value": "aqi",
    "CO": "co",
    "Ozone": "o3",
    "PM10": "pm10",
    "PM25": "pm25",
    "NO2": "no2",
})

# FIX: Convert date to JSON-safe string
df["observed_date"] = df["observed_date"].dt.date.astype(str)

# 3. Add required fields
df["location"] = "Berlin"
df["source"] = "historical_dataset"

NUMERIC_COLUMNS = ["aqi", "pm25", "pm10", "o3", "co", "no2"]

# Replace common junk values with NaN
df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].replace(
    [".", " ", "", "NA", "N/A", "--"],
    pd.NA
)

# Force numeric conversion
df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].apply(
    pd.to_numeric, errors="coerce"
)


# 4. Drop rows with missing critical values
df = df.dropna(
    subset=["observed_date", "aqi", "pm25", "pm10", "o3"]
)

DB_COLUMNS = [
    "observed_date",
    "location",
    "aqi",
    "co",
    "o3",
    "pm10",
    "pm25",
    "no2",
    "source",
]

df = df[DB_COLUMNS]

# Final safety: remove any remaining NaN values
df = df.dropna()


# 5. Convert to list of dicts
records = df.to_dict(orient="records")

print(f"Inserting {len(records)} rows...")

# 6. Insert in batches (Supabase limit safe)
BATCH_SIZE = 500

for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i+BATCH_SIZE]
    response = supabase.table("air_quality_raw").insert(batch).execute()

print("Done.")

#gitcheck