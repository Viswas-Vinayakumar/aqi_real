from fastapi import FastAPI
import supabase
from app.api.aqi import router as aqi_router

app = FastAPI(title="AQI Real API")

app.include_router(aqi_router)

@app.get("/")
def health():
    return {"status": "ok"}

#gitcheck