from fastapi import APIRouter
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/aqi", tags=["AQI"])

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


@router.get("/raw")
def get_raw_aqi(location: str):
    response = (
        supabase
        .table("air_quality_raw")
        .select("*")
        .eq("location", location)
        .order("observed_date")
        .execute()
    )
    return response.data

@router.get("/predictions")
def get_predictions(location: str):
    response = (
        supabase
        .table("air_quality_predictions")
        .select("*")
        .eq("location", location)
        .order("target_date")
        .execute()
    )
    return response.data
