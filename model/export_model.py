import os
import joblib


BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

MODEL_DIR = os.path.join(
    BASE_DIR,
    "model",
    "artifacts"
)

MODEL_PATH = os.path.join(
    MODEL_DIR,
    "isolation_forest.pkl"
)

SCALER_PATH = os.path.join(
    MODEL_DIR,
    "scaler.pkl"
)


def verify_serialized_model():

    print("=" * 60)
    print("REAL-TIME FRAUD DETECTION")
    print("Serialized Model Verification")
    print("=" * 60)

    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"Model file not found: {MODEL_PATH}"
        )

    if not os.path.exists(SCALER_PATH):
        raise FileNotFoundError(
            f"Scaler file not found: {SCALER_PATH}"
        )

    print("\nLoading serialized model...")

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    print("Model loaded successfully.")
    print("Scaler loaded successfully.")

    print("\nModel type:")
    print(f"  {type(model).__name__}")

    print("\nScaler type:")
    print(f"  {type(scaler).__name__}")

    print("\nSerialized artifacts:")

    print(
        f"  Model  : {MODEL_PATH}"
    )

    print(
        f"  Scaler : {SCALER_PATH}"
    )

    print("\nArtifact sizes:")

    print(
        f"  Model  : {os.path.getsize(MODEL_PATH) / 1024:.2f} KB"
    )

    print(
        f"  Scaler : {os.path.getsize(SCALER_PATH) / 1024:.2f} KB"
    )

    print("\n" + "=" * 60)
    print("MODEL SERIALIZATION VERIFIED")
    print("=" * 60)


if __name__ == "__main__":
    verify_serialized_model()