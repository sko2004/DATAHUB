"""
DataHub Module 5 — Test Data Generator
Creates realistic CSV, JSON, and Parquet files and commits them via the API.
Run: python create_test_data.py
"""
import json
import io
import os
import requests
import numpy as np
import pandas as pd
import tempfile

BASE_URL = "http://localhost:8000"


# ─────────────────────────────────────────────
# 1. AUTH — get admin token
# ─────────────────────────────────────────────
def get_token(username="admin_user", password="Admin@123"):
    r = requests.post(f"{BASE_URL}/auth/login", data={"username": username, "password": password})
    r.raise_for_status()
    token = r.json()["access_token"]
    print(f"✅ Logged in as {username}")
    return token


# ─────────────────────────────────────────────
# 2. UPLOAD helper
# ─────────────────────────────────────────────
def upload_file(token, project_name, message, file_path, branch="main", custom_metrics=None):
    headers = {"Authorization": f"Bearer {token}"}
    fname = os.path.basename(file_path)
    mime = (
        "text/csv" if fname.endswith(".csv") else
        "application/json" if fname.endswith(".json") else
        "application/octet-stream"
    )
    with open(file_path, "rb") as f:
        files = {"file": (fname, f, mime)}
        data = {
            "project_name": project_name,
            "message": message,
            "branch": branch,
        }
        if custom_metrics:
            data["custom_metrics"] = json.dumps(custom_metrics)
        r = requests.post(f"{BASE_URL}/metadata/upload-and-commit", headers=headers, files=files, data=data)
    if r.status_code == 201:
        resp = r.json()
        print(f"  ✅ Committed '{fname}' → {resp['commit_hash'][:10]}... "
              f"({resp['metadata']['row_count']} rows, {resp['metadata']['column_count']} cols)")
        if resp['metadata'].get('ai_summary'):
            print(f"     🤖 AI: {resp['metadata']['ai_summary'][:100]}...")
        return resp
    else:
        print(f"  ❌ Failed to commit '{fname}': {r.status_code} {r.text[:200]}")
        return None


# ─────────────────────────────────────────────
# 3. TEST FILE GENERATORS
# ─────────────────────────────────────────────
def make_iris_csv(path):
    """Classic Iris-like dataset."""
    np.random.seed(42)
    n = 150
    species = np.random.choice(["setosa", "versicolor", "virginica"], n)
    df = pd.DataFrame({
        "sepal_length": np.round(np.random.normal(5.8, 0.8, n), 2),
        "sepal_width":  np.round(np.random.normal(3.0, 0.4, n), 2),
        "petal_length": np.round(np.random.normal(3.7, 1.7, n), 2),
        "petal_width":  np.round(np.random.normal(1.2, 0.7, n), 2),
        "species":      species,
    })
    # Add some nulls for realism
    df.loc[np.random.choice(df.index, 8, replace=False), "sepal_width"] = None
    df.to_csv(path, index=False)


def make_model_metrics_csv(path):
    """ML training logs with accuracy, loss, f1_score per epoch."""
    np.random.seed(7)
    epochs = 50
    df = pd.DataFrame({
        "epoch":     range(1, epochs + 1),
        "accuracy":  np.round(0.5 + 0.45 * (1 - np.exp(-np.arange(epochs) / 10)) + np.random.normal(0, 0.01, epochs), 4),
        "val_accuracy": np.round(0.5 + 0.42 * (1 - np.exp(-np.arange(epochs) / 10)) + np.random.normal(0, 0.015, epochs), 4),
        "loss":      np.round(0.8 * np.exp(-np.arange(epochs) / 12) + np.random.normal(0, 0.01, epochs), 4),
        "val_loss":  np.round(0.85 * np.exp(-np.arange(epochs) / 12) + np.random.normal(0, 0.015, epochs), 4),
        "f1_score":  np.round(0.48 + 0.44 * (1 - np.exp(-np.arange(epochs) / 10)) + np.random.normal(0, 0.01, epochs), 4),
    })
    df.to_csv(path, index=False)


def make_sales_csv(path):
    """E-commerce sales data."""
    np.random.seed(15)
    n = 500
    regions   = ["North", "South", "East", "West"]
    products  = ["Laptop", "Phone", "Tablet", "Watch", "Headphones"]
    df = pd.DataFrame({
        "order_id":    [f"ORD-{i:04d}" for i in range(1, n + 1)],
        "product":     np.random.choice(products, n),
        "region":      np.random.choice(regions, n),
        "quantity":    np.random.randint(1, 20, n),
        "unit_price":  np.round(np.random.uniform(50, 2000, n), 2),
        "discount_pct":np.round(np.random.uniform(0, 0.3, n), 2),
        "revenue":     None,  # computed below
        "customer_age":np.random.randint(18, 70, n),
        "is_returned": np.random.choice([True, False], n, p=[0.05, 0.95]),
    })
    df["revenue"] = np.round(df["unit_price"] * df["quantity"] * (1 - df["discount_pct"]), 2)
    # Inject some nulls
    df.loc[np.random.choice(df.index, 15, replace=False), "customer_age"] = None
    df.to_csv(path, index=False)


def make_sensor_json(path):
    """IoT sensor readings in JSON format."""
    np.random.seed(99)
    n = 200
    records = []
    for i in range(n):
        records.append({
            "sensor_id":   f"SENS-{np.random.randint(1, 10):02d}",
            "timestamp":   f"2024-{np.random.randint(1,12):02d}-{np.random.randint(1,28):02d}T{np.random.randint(0,23):02d}:{np.random.randint(0,59):02d}:00Z",
            "temperature": round(float(np.random.normal(22.5, 3.0)), 2),
            "humidity":    round(float(np.random.normal(55, 10)), 2),
            "pressure":    round(float(np.random.normal(1013, 5)), 2),
            "vibration":   round(float(abs(np.random.normal(0, 0.5))), 4),
            "status":      np.random.choice(["normal", "warning", "critical"], p=[0.85, 0.12, 0.03]),
        })
    with open(path, "w") as f:
        json.dump(records, f, indent=2)


def make_churn_parquet(path):
    """Customer churn prediction dataset."""
    np.random.seed(21)
    n = 300
    df = pd.DataFrame({
        "customer_id":     [f"CUST-{i:04d}" for i in range(1, n + 1)],
        "tenure_months":   np.random.randint(1, 72, n),
        "monthly_charges": np.round(np.random.uniform(20, 120, n), 2),
        "total_charges":   np.round(np.random.uniform(100, 8000, n), 2),
        "num_products":    np.random.randint(1, 5, n),
        "support_calls":   np.random.randint(0, 15, n),
        "satisfaction_score": np.random.randint(1, 6, n),
        "contract_type":   np.random.choice(["Monthly", "One year", "Two year"], n),
        "churn":           np.random.choice([0, 1], n, p=[0.73, 0.27]),
        "auc_score":       np.round(np.random.uniform(0.75, 0.92, n), 4),
    })
    df.loc[np.random.choice(df.index, 10, replace=False), "satisfaction_score"] = None
    df.to_parquet(path, index=False)


# ─────────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  DataHub — Test Data Generator")
    print("=" * 60)

    token = get_token()

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    import shutil

    with tempfile.TemporaryDirectory() as tmpdir:

        # ── PROJECT 1: ML Classification ──────────────────────────
        print("\n📁 Project: ml-classification")

        # Commit 1: raw iris data
        p1 = os.path.join(tmpdir, "iris_raw.csv")
        make_iris_csv(p1)
        shutil.copy(p1, os.path.join(data_dir, "iris_raw.csv"))
        upload_file(token, "ml-classification", "Add raw Iris dataset v1.0", p1)

        # Commit 2: model training metrics
        p2 = os.path.join(tmpdir, "training_metrics.csv")
        make_model_metrics_csv(p2)
        shutil.copy(p2, os.path.join(data_dir, "training_metrics.csv"))
        upload_file(
            token, "ml-classification",
            "Add model training logs — epoch 1-50",
            p2,
            custom_metrics={"model": "ResNet-50", "optimizer": "Adam", "lr": 0.001}
        )

        # Commit 3: same iris file again (dedup test)
        p3 = os.path.join(tmpdir, "iris_raw_v2.csv")
        make_iris_csv(p3)   # same content → duplicate blob
        shutil.copy(p3, os.path.join(data_dir, "iris_raw_v2.csv"))
        upload_file(token, "ml-classification", "Re-commit iris dataset (dedup test)", p3, branch="experiment")

        # ── PROJECT 2: E-commerce Analytics ────────────────────────
        print("\n📁 Project: ecommerce-analytics")

        p4 = os.path.join(tmpdir, "sales_q1.csv")
        make_sales_csv(p4)
        shutil.copy(p4, os.path.join(data_dir, "sales_q1.csv"))
        upload_file(token, "ecommerce-analytics", "Add Q1 2024 sales data", p4,
                    custom_metrics={"total_revenue_usd": 1245000, "avg_order_value": 249.0})

        # ── PROJECT 3: IoT Monitoring ───────────────────────────────
        print("\n📁 Project: iot-sensor-monitoring")

        p5 = os.path.join(tmpdir, "sensor_readings.json")
        make_sensor_json(p5)
        with open(p5, "r") as f:
            pd.DataFrame(json.load(f)).to_csv(os.path.join(data_dir, "sensor_readings.csv"), index=False)
        upload_file(token, "iot-sensor-monitoring", "Batch sensor readings 2024-Q1",
                    p5, custom_metrics={"anomaly_rate": 0.03, "uptime_pct": 99.7})

        # ── PROJECT 4: Customer Churn ───────────────────────────────
        print("\n📁 Project: churn-prediction")

        p6 = os.path.join(tmpdir, "churn_dataset.parquet")
        make_churn_parquet(p6)
        pd.read_parquet(p6).to_csv(os.path.join(data_dir, "churn_dataset.csv"), index=False)
        upload_file(token, "churn-prediction", "Initial churn dataset with AUC scores",
                    p6, custom_metrics={"model_auc": 0.87, "churn_rate": 0.27, "precision": 0.84, "recall": 0.79})

    # ── Summary ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Fetching final stats...")
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{BASE_URL}/metadata/stats/summary", headers=headers)
    if r.ok:
        s = r.json()
        print(f"  📊 Projects:       {s['total_projects']}")
        print(f"  📊 Commits:        {s['total_commits']}")
        print(f"  📊 Unique Blobs:   {s['total_blobs']}")
        print(f"  📊 Files Indexed:  {s['total_indexed_files']}")
        print(f"  📊 Rows Indexed:   {s.get('total_rows_indexed', 0)}")
        print(f"  📊 Storage Used:   {s.get('total_storage_bytes', 0)} bytes")
    print("\n✅ All test data created! Open http://localhost:3000 to explore.")
    print("=" * 60)


if __name__ == "__main__":
    main()
