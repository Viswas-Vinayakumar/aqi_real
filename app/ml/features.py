import pandas as pd

def build_features(df):
    df = df.copy()

    df["so2"] = df["so2"].fillna(0)
    df["co"] = df["co"].fillna(0)
    df["no2"] = df["no2"].fillna(0)

    df = df.infer_objects(copy=False)

    df["pm25_ratio"] = df["pm25"] / (df["pm10"] + 1)
    df["gas_load"] = df["no2"] + df["so2"] + df["co"]
    df["pm25_3day_avg"] = df["pm25"].rolling(3, min_periods=1).mean()

    # 🔥 LAG FEATURES (THIS IS THE BIG WIN)
    df["pm25_lag1"] = df["pm25"].shift(1)
    df["pm25_lag2"] = df["pm25"].shift(2)
    df["aqi_lag1"] = df["aqi"].shift(1)

    df["month"] = df["observed_date"].dt.month
    df["day_of_year"] = df["observed_date"].dt.dayofyear


    return df.dropna()

#gitcheck