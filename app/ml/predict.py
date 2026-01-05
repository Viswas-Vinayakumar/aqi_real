from app.ml.dataloader import load_raw_data
from app.ml.features import build_features
from app.ml.model import build_training_data, train_and_evaluate
from datetime import timedelta
import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def predict_future(model, df_feat, days=3):
    """
    Recursive forecasting:
    Uses latest known data to predict future AQI step by step
    """
    df_future = df_feat.copy()
    predictions = []

    last_date = df_future["observed_date"].max()

    for i in range(days):
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
        target_date = last_date + timedelta(days=i + 1)

        predictions.append(
            {
                "target_date": target_date.isoformat(),
                "predicted_aqi": predicted_aqi,
            }
        )

        # Feed prediction back into the dataset (recursive step)
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


def run():
    # 1. Load historical data
    df = load_raw_data()

    # 2. Build features
    df_feat = build_features(df)

    print(df_feat[
        [
            "observed_date",
            "pm25",
            "pm10",
            "pm25_ratio",
            "gas_load",
            "pm25_3day_avg",
        ]
    ].head())

    print("Feature rows:", len(df_feat))

    # 3. Train + evaluate model
    X, y = build_training_data(df_feat)
    model, mae = train_and_evaluate(X, y)

    print("Test MAE:", mae)

    # 4. Predict next 3 days
    future_preds = predict_future(model, df_feat, days=3)

    print("\nFuture AQI predictions:")
    for p in future_preds:
        print(p)

    # 5. Save predictions
    save_predictions(future_preds)
    print("Predictions saved to Supabase")


def save_predictions(predictions, model_version="linear_v1"):
    rows = []
    for p in predictions:
        rows.append({
            "location": "Berlin",
            "target_date": p["target_date"],
            "predicted_aqi": p["predicted_aqi"],
            "model_version": model_version
        })

    supabase.table("air_quality_predictions").insert(rows).execute()


if __name__ == "__main__":
    run()
