import os

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "PS_20174392719_1491204439457_log.csv"
)

MODEL_DIR = os.path.join(BASE_DIR, "model", "artifacts")

MODEL_PATH = os.path.join(
    MODEL_DIR,
    "isolation_forest.pkl"
)

SCALER_PATH = os.path.join(
    MODEL_DIR,
    "scaler.pkl"
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SAMPLE_SIZE = 100_000
RANDOM_STATE = 42


# ---------------------------------------------------------
# Main training process
# ---------------------------------------------------------

def train_model():

    print("=" * 60)
    print("REAL-TIME FRAUD DETECTION")
    print("Isolation Forest Training")
    print("=" * 60)

    # Check dataset
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"PaySim dataset not found: {DATA_PATH}"
        )

    print("\nLoading historical PaySim data...")

    # Read a random sample from the large CSV
    df = pd.read_csv(
        DATA_PATH
    )

    print(f"Total historical transactions: {len(df):,}")

    # Take a manageable training sample
    if len(df) > SAMPLE_SIZE:
        df_sample = df.sample(
            n=SAMPLE_SIZE,
            random_state=RANDOM_STATE
        )
    else:
        df_sample = df.copy()

    print(
        f"Training sample size: {len(df_sample):,}"
    )

    # -----------------------------------------------------
    # Feature selection
    # -----------------------------------------------------

    feature_columns = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    X = df_sample[feature_columns].copy()

    # Convert values to numeric
    X = X.apply(
        pd.to_numeric,
        errors="coerce"
    )

    # Handle missing/infinite values
    X = X.replace(
        [float("inf"), float("-inf")],
        pd.NA
    )

    X = X.fillna(0)

    print("\nFeatures used:")
    for feature in feature_columns:
        print(f"  - {feature}")

    # -----------------------------------------------------
    # Feature scaling
    # -----------------------------------------------------

    print("\nScaling features...")

    scaler = StandardScaler()

    X_scaled = scaler.fit_transform(X)

    # -----------------------------------------------------
    # Train Isolation Forest
    # -----------------------------------------------------

    print("\nTraining Isolation Forest...")

    model = IsolationForest(
        n_estimators=100,
        contamination=0.01,
        random_state=RANDOM_STATE,
        n_jobs=-1
    )

    model.fit(X_scaled)

    print("Model training completed.")

    # -----------------------------------------------------
    # Generate anomaly predictions
    # -----------------------------------------------------

    predictions = model.predict(X_scaled)

    anomaly_count = (predictions == -1).sum()
    normal_count = (predictions == 1).sum()

    print("\nTraining results:")
    print(f"  Normal transactions : {normal_count:,}")
    print(f"  Anomalies detected  : {anomaly_count:,}")

    # -----------------------------------------------------
    # Save model and scaler
    # -----------------------------------------------------

    os.makedirs(
        MODEL_DIR,
        exist_ok=True
    )

    joblib.dump(
        model,
        MODEL_PATH
    )

    joblib.dump(
        scaler,
        SCALER_PATH
    )

    print("\nSaved files:")

    print(f"  Model  : {MODEL_PATH}")
    print(f"  Scaler : {SCALER_PATH}")

    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    train_model()