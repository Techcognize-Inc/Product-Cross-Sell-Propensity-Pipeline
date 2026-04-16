import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import psycopg2
import plotly.express as px
import time

# =========================
# CONFIG
# =========================
KAFKA_SERVER = "kafka:9092"
OFFER_TOPIC = "crm.realtime_offers"

DB_CONFIG = {
    "host": "postgres",
    "user": "airflow",
    "password": "airflow",
    "dbname": "airflow"
}

st.set_page_config(page_title="Propensity Dashboard", layout="wide")


# =========================
# KAFKA CONSUMER (REAL-TIME FEED)
# =========================
def get_kafka_consumer():
    return KafkaConsumer(
        OFFER_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )


# =========================
# LOAD BATCH PROPENSITY DATA
# =========================
def load_propensity_scores():
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT * FROM mart_propensity_scores"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# =========================
# REAL-TIME STREAM FETCH
# =========================
def fetch_realtime_offers(consumer, limit=50):
    messages = []
    for _ in range(limit):
        msg = consumer.poll(timeout_ms=1000)
        for tp, records in msg.items():
            for record in records:
                messages.append(record.value)
    return pd.DataFrame(messages) if messages else pd.DataFrame()


# =========================
# UI SECTION 1: REAL-TIME INTENT FEED
# =========================
def realtime_feed():
    st.subheader("🔥 Real-Time Intent Feed")

    consumer = get_kafka_consumer()

    placeholder = st.empty()

    while True:
        data = fetch_realtime_offers(consumer, 20)

        if not data.empty:
            placeholder.dataframe(data)

        time.sleep(5)
        st.rerun()


# =========================
# UI SECTION 2: WEEKLY PROPENSITY OVERVIEW
# =========================
def propensity_overview(df):
    st.subheader("📊 Weekly Propensity Overview")

    if df.empty:
        st.warning("No data found")
        return

    cols = st.columns(4)

    products = df["product_family"].unique()

    for i, product in enumerate(products[:4]):
        subset = df[df["product_family"] == product]

        with cols[i]:
            st.metric(
                label=product,
                value=len(subset),
                delta=f"Avg Score: {subset['propensity_score'].mean():.1f}"
            )


# =========================
# UI SECTION 3: CAMPAIGN BUILDER
# =========================
def campaign_builder(df):
    st.subheader("🎯 Campaign Builder")

    product = st.selectbox("Select Product", df["product_family"].unique())
    min_score = st.slider("Minimum Propensity Score", 0, 100, 60)

    filtered = df[
        (df["product_family"] == product) &
        (df["propensity_score"] >= min_score)
    ]

    st.write(f"Customers: {len(filtered)}")

    st.dataframe(filtered)


# =========================
# UI SECTION 4: SIGNAL EXPLAINER
# =========================
def signal_explainer(df):
    st.subheader("🧠 Signal Reason Explainer")

    sample = df.head(10)

    for _, row in sample.iterrows():
        with st.expander(f"Customer {row['customer_id']} - Score {row['propensity_score']}"):
            st.write("Top Drivers:")
            st.write("- Transaction behavior score")
            st.write("- Product holding pattern")
            st.write("- Campaign response history")


# =========================
# UI SECTION 5: PERFORMANCE TRACKER
# =========================
def performance_tracker(df):
    st.subheader("📈 Campaign Performance")

    fig = px.line(
        df.groupby("product_family")["propensity_score"].mean().reset_index(),
        x="product_family",
        y="propensity_score",
        title="Avg Propensity Score by Product"
    )

    st.plotly_chart(fig, use_container_width=True)


# =========================
# MAIN APP
# =========================
def main():
    st.title("🏦 Bank Propensity Scoring Dashboard")

    df = load_propensity_scores()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Overview",
        "Campaign Builder",
        "Signal Explainer",
        "Performance"
    ])

    with tab1:
        propensity_overview(df)

    with tab2:
        campaign_builder(df)

    with tab3:
        signal_explainer(df)

    with tab4:
        performance_tracker(df)


if __name__ == "__main__":
    main()