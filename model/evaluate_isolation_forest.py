import os

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "PS_20174392719_1491204439457_log.csv"
)

MODEL_PATH = os.path.join(
    BASE_DIR,
    "model",
    "artifacts",
    "isolation_forest.pkl"
)

SCALER_PATH = os.path.join(
    BASE_DIR,
    "model",
    "artifacts",
    "scaler.pkl"
)

SAMPLE_SIZE = 100_000
RANDOM_STATE = 42


def evaluate_model():

    print("=" * 70)
    print("REAL-TIME FRAUD DETECTION")
    print("Isolation Forest - False Negative Focused Evaluation")
    print("=" * 70)

    # -----------------------------------------------------
    # Load dataset
    # -----------------------------------------------------

    print("\nLoading PaySim data...")

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}"
        )

    df = pd.read_csv(DATA_PATH)

    print(f"Total transactions: {len(df):,}")

    df_sample = df.sample(
        n=min(SAMPLE_SIZE, len(df)),
        random_state=RANDOM_STATE
    )

    print(
        f"Evaluation sample size: {len(df_sample):,}"
    )

    # -----------------------------------------------------
    # Features
    # -----------------------------------------------------

    feature_columns = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    X = df_sample[feature_columns].copy()

    X = X.apply(
        pd.to_numeric,
        errors="coerce"
    )

    X = X.replace(
        [float("inf"), float("-inf")],
        pd.NA
    )

    X = X.fillna(0)

    y_true = df_sample["isFraud"].astype(int)

    print("\nActual transactions:")
    print(f"  Fraud     : {y_true.sum():,}")
    print(f"  Legitimate: {(y_true == 0).sum():,}")

    # -----------------------------------------------------
    # Load model and scaler
    # -----------------------------------------------------

    print("\nLoading trained model and scaler...")

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    X_scaled = scaler.transform(X)

    # -----------------------------------------------------
    # Generate anomaly scores
    # -----------------------------------------------------

    print("\nGenerating anomaly scores...")

    scores = model.decision_function(X_scaled)

    # Lower score = more anomalous
    #
    # We test multiple alert thresholds.
    # A larger alert percentage means a more sensitive
    # fraud detector, which can reduce false negatives.

    alert_percentages = [
        1,
        2,
        3,
        5,
        10,
    ]

    results = []

    print("\n" + "=" * 70)
    print("THRESHOLD ANALYSIS")
    print("=" * 70)

    for percentage in alert_percentages:

        threshold = pd.Series(scores).quantile(
            percentage / 100
        )

        y_pred = (
            scores <= threshold
        ).astype(int)

        tn, fp, fn, tp = confusion_matrix(
            y_true,
            y_pred,
            labels=[0, 1]
        ).ravel()

        precision = precision_score(
            y_true,
            y_pred,
            zero_division=0
        )

        recall = recall_score(
            y_true,
            y_pred,
            zero_division=0
        )

        f1 = f1_score(
            y_true,
            y_pred,
            zero_division=0
        )

        results.append(
            {
                "alert_percentage": percentage,
                "tn": tn,
                "fp": fp,
                "fn": fn,
                "tp": tp,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            }
        )

        print(
            f"\nAlert Rate: {percentage}%"
        )
        print(f"  True Positives : {tp:,}")
        print(f"  False Negatives: {fn:,}")
        print(f"  False Positives: {fp:,}")
        print(f"  Precision      : {precision:.4f}")
        print(f"  Recall         : {recall:.4f}")
        print(f"  F1 Score       : {f1:.4f}")

    # -----------------------------------------------------
    # Select threshold prioritizing recall
    # -----------------------------------------------------

    best_result = max(
        results,
        key=lambda result: (
            result["recall"],
            result["precision"]
        )
    )

    print("\n" + "=" * 70)
    print("FALSE NEGATIVE PRIORITY")
    print("=" * 70)

    print(
        f"\nSelected alert rate: "
        f"{best_result['alert_percentage']}%"
    )

    print(
        f"True Positives : "
        f"{best_result['tp']:,}"
    )

    print(
        f"False Negatives: "
        f"{best_result['fn']:,}"
    )

    print(
        f"False Positives: "
        f"{best_result['fp']:,}"
    )

    print(
        f"Precision      : "
        f"{best_result['precision']:.4f}"
    )

    print(
        f"Recall         : "
        f"{best_result['recall']:.4f}"
    )

    print(
        f"F1 Score       : "
        f"{best_result['f1']:.4f}"
    )

    print("\nInterpretation:")

    if best_result["fn"] > 0:
        print(
            f"The model still misses "
            f"{best_result['fn']:,} fraudulent transactions."
        )
        print(
            "Further feature engineering and model "
            "improvement are required to reduce false negatives."
        )
    else:
        print(
            "No fraudulent transactions were missed "
            "in this evaluation sample."
        )

    print("\n" + "=" * 70)
    print("EVALUATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    evaluate_model()