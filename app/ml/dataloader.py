import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def load_raw_data(location="Berlin"):
    response = (
        supabase
        .table("air_quality_raw")
        .select("*")
        .eq("location", location)
        .order("observed_date")
        .execute()
    )

    df = pd.DataFrame(response.data)
    df["observed_date"] = pd.to_datetime(df["observed_date"])

    return df

#gitcheck
