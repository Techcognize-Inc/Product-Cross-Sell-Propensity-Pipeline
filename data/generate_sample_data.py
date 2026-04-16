import json
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# =========================
# CONFIG
# =========================
NUM_CUSTOMERS = 500000
DAYS_OF_HISTORY = 90

BASE_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

UCI_DATA_PATH = os.path.join(BASE_DIR, "bank-marketing.csv")

INCOME_BANDS = ['25-40k','40-60k','60-90k','90-120k','120k+']
REGIONS = ['North', 'South', 'East', 'West']

MERCHANT_CATEGORIES = [
    'home_improvement','electronics','travel','retail',
    'insurance','utilities','salary'
]


# =========================
# LOAD DATA
# =========================
def load_uci_data():
    df = pd.read_csv(UCI_DATA_PATH, sep=';')
    df['customer_id'] = [f'cust_{i+1:06d}' for i in range(len(df))]
    return df


# =========================
# DEMOGRAPHICS (OPTIMIZED)
# =========================
def build_demographics(uci_df: pd.DataFrame):
    n = len(uci_df)

    return pd.DataFrame({
        "customer_id": uci_df["customer_id"].values,
        "age": uci_df["age"].values,
        "income_band": np.random.choice(INCOME_BANDS, n),
        "region": np.random.choice(REGIONS, n),
        "existing_products": [
            json.dumps({
                "home_loan": int(x == "yes"),
                "credit_card": 0,
                "fixed_deposit": 0,
                "personal_loan": int(y == "yes")
            })
            for x, y in zip(uci_df["housing"], uci_df["loan"])
        ]
    })


# =========================
# HOLDINGS
# =========================
def build_holdings(demographics_df):
    parsed = demographics_df["existing_products"].apply(json.loads)

    return pd.DataFrame({
        "customer_id": demographics_df["customer_id"],
        "has_home_loan": parsed.apply(lambda x: x["home_loan"]),
        "has_credit_card": parsed.apply(lambda x: x["credit_card"]),
        "has_fixed_deposit": parsed.apply(lambda x: x["fixed_deposit"]),
        "has_personal_loan": parsed.apply(lambda x: x["personal_loan"]),
    })


# =========================
# CAMPAIGN
# =========================
def build_campaign_history(uci_df):
    return pd.DataFrame({
        "customer_id": uci_df["customer_id"],
        "last_campaign_response": np.where(uci_df["y"] == "yes", "yes", "no"),
        "last_campaign_date": "2023-01-01",
        "campaign_touch_count": uci_df["campaign"]
    })


# =========================
# TRANSACTIONS (FASTER VERSION)
# =========================
def build_transactions(demographics_df, fake):
    start_date = datetime.utcnow() - timedelta(days=DAYS_OF_HISTORY)

    customer_ids = demographics_df["customer_id"].values
    num_customers = len(customer_ids)

    # number of transactions per customer
    txn_counts = np.random.randint(20, 40, size=num_customers)

    # flatten everything in vectorized chunks
    all_rows = []

    for i in range(num_customers):

        cid = customer_ids[i]
        n = txn_counts[i]

        base_days = np.random.randint(0, DAYS_OF_HISTORY, n)
        amounts = np.random.randint(10, 3000, n)
        cats = np.random.choice(MERCHANT_CATEGORIES, n)
        channels = np.random.choice(["branch", "online", "mobile"], n)

        txn_dates = [
            (start_date + timedelta(days=int(d))).isoformat()
            for d in base_days
        ]

        # Faker removed from inner loop (IMPORTANT FIX)
        merchants = ["merchant_" + str(np.random.randint(1, 10000)) for _ in range(n)]

        for j in range(n):
            all_rows.append([
                cid,
                txn_dates[j],
                cats[j],
                "salary" if cats[j] == "salary" else "debit",
                float(amounts[j]),
                merchants[j],
                channels[j]
            ])

    return pd.DataFrame(all_rows, columns=[
        "customer_id",
        "transaction_ts",
        "merchant_category",
        "transaction_type",
        "amount",
        "merchant_name",
        "channel"
    ])

# =========================
# PERSIST
# =========================
def persist_df(df, name):
    path = os.path.join(OUTPUT_DIR, name)
    print(f"Writing {path} | rows={len(df)}")

    df.to_parquet(path, index=False)


# =========================
# MAIN
# =========================
def main():
    fake = Faker()
    Faker.seed(42)
    np.random.seed(42)

    print("Loading UCI data...")
    uci_df = load_uci_data()

    # scale safely
    reps = (NUM_CUSTOMERS // len(uci_df)) + 1
    uci_df = pd.concat([uci_df] * reps, ignore_index=True).head(NUM_CUSTOMERS)
    uci_df["customer_id"] = [f"cust_{i:06d}" for i in range(NUM_CUSTOMERS)]

    print("Building datasets...")

    demographics = build_demographics(uci_df)
    holdings = build_holdings(demographics)
    campaign = build_campaign_history(uci_df)
    transactions = build_transactions(demographics, fake)

    print("Writing parquet files...")

    persist_df(demographics, "demographics.parquet")
    persist_df(holdings, "product_holdings.parquet")
    persist_df(campaign, "campaign_history.parquet")
    persist_df(transactions, "transactions.parquet")

    print("✅ Done. Files created in:", OUTPUT_DIR)


if __name__ == "__main__":
    main()