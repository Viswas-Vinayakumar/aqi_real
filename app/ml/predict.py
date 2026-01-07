# app/ml/predict.py

from datetime import timedelta
import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from app.ml.dataloader import load_raw_data
from app.ml.features import build_features
from app.ml.model import build_training_data, train_and_evaluate

# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

LOCATION = "Berlin"
MODEL_VERSION = "linear_v1"
PREDICTION_DAYS = 3

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# --------------------------------------------------
# Prediction logic
# --------------------------------------------------

def predict_future(model, df_feat, days=PREDICTION_DAYS):
    """
    Recursive multi-step AQI forecasting
    """
    df_future = df_feat.copy()
    predictions = []

    last_date = df_future["observed_date"].max()

    for step in range(days):
        latest = df_future.iloc[-1:]

        X_latest = latest[
            [
                "pm25",
                "pm10",
                "pm25_ratio",
                "gas_load",
                "pm25_3day_avg",
                "pm25_lag1",
                "pm25_lag2",
                "aqi_lag1",
                "month",
                "day_of_year",
            ]
        ]

        predicted_aqi = int(model.predict(X_latest)[0])
        target_date = last_date + timedelta(days=step + 1)

        predictions.append({
            "location": LOCATION,
            "target_date": target_date.isoformat(),
            "predicted_aqi": predicted_aqi,
            "model_version": MODEL_VERSION,
        })

        # Feed prediction back into features (recursive step)
        new_row = latest.copy()
        new_row["observed_date"] = target_date
        new_row["aqi"] = predicted_aqi
        new_row["aqi_lag1"] = predicted_aqi
        new_row["pm25_lag2"] = latest["pm25_lag1"].values
        new_row["pm25_lag1"] = latest["pm25"].values
        new_row["month"] = target_date.month
        new_row["day_of_year"] = target_date.timetuple().tm_yday

        df_future = pd.concat([df_future, new_row], ignore_index=True)

    return predictions

# --------------------------------------------------
# Persistence
# --------------------------------------------------

def save_predictions(predictions):
    """
    Upsert predictions safely (no duplicates)
    Requires UNIQUE(location, target_date, model_version)
    """
    if not predictions:
        print("No predictions to save.")
        return

    supabase.table("air_quality_predictions") \
        .upsert(
            predictions,
            on_conflict="location,target_date,model_version"
        ) \
        .execute()

# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def run():
    print("Loading historical data...")
    df = load_raw_data()

    print("Building features...")
    df_feat = build_features(df)

    print(f"Feature rows: {len(df_feat)}")

    print("Training model...")
    X, y = build_training_data(df_feat)
    model, mae = train_and_evaluate(X, y)

    print(f"Model MAE: {mae:.2f}")

    print("Predicting future AQI...")
    future_predictions = predict_future(model, df_feat)

    for p in future_predictions:
        print(p)

    print("Saving predictions to Supabase...")
    save_predictions(future_predictions)

    print("Pipeline completed successfully.")

# --------------------------------------------------

if __name__ == "__main__":
    run()
