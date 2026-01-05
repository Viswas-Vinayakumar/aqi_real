import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error


def build_training_data(df: pd.DataFrame):
    """
    Builds supervised learning data:
    Features from day T -> AQI of day T+1
    """
    df = df.copy()

    # Target = next day's AQI
    df["target_aqi"] = df["aqi"].shift(-1)

    # Last row has no future value
    df = df.dropna(subset=["target_aqi"])

    X = df[
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


    y = df["target_aqi"]

    return X, y


def time_based_split(X, y, train_ratio=0.8):
    """
    Time-aware split (NO shuffling).
    Past -> Train, Future -> Test
    """
    split_idx = int(len(X) * train_ratio)

    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]

    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]

    return X_train, X_test, y_train, y_test


def train_and_evaluate(X, y):
    """
    Trains baseline Linear Regression and evaluates honestly
    """
    X_train, X_test, y_train, y_test = time_based_split(X, y)

    model = LinearRegression()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    return model, mae

#gitcheck
