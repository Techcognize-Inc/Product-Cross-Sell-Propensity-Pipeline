import time
import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASS", "airflow"),
    "dbname": os.getenv("DB_NAME", "airflow"),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
}

PRODUCT_LABELS = {
    "home_loan": "Home Loan",
    "credit_card": "Credit Card",
    "fixed_deposit": "Fixed Deposit",
    "personal_loan": "Personal Loan",
}

ESTIMATED_REVENUE_PER_CONVERSION = {
    "home_loan": 250000,
    "credit_card": 8000,
    "fixed_deposit": 25000,
    "personal_loan": 60000,
}

st.set_page_config(page_title="Campaign Builder Dashboard", layout="wide")
px.defaults.template = "plotly_white"


def inject_custom_styles():
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(1200px 520px at 8% -10%, #d7efff 0%, transparent 56%),
                    radial-gradient(950px 520px at 100% 0%, #ffe4cc 0%, transparent 52%),
                    linear-gradient(180deg, #f8fbff 0%, #eef5ff 100%);
                color: #1f2a44;
            }
            [data-testid="stHeader"] {
                background: rgba(255, 255, 255, 0.7);
                border-bottom: 1px solid rgba(31, 42, 68, 0.08);
                backdrop-filter: blur(6px);
            }
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #132744 0%, #1f3f69 100%);
            }
            [data-testid="stSidebar"] * {
                color: #eef5ff;
            }
            .hero {
                padding: 1.05rem 1.2rem;
                border-radius: 14px;
                background: linear-gradient(120deg, #0f6ab7 0%, #1d8fd8 45%, #42b883 100%);
                color: #ffffff;
                box-shadow: 0 14px 36px rgba(16, 76, 136, 0.24);
                margin-bottom: 0.9rem;
            }
            .hero-title {
                font-size: 1.28rem;
                font-weight: 700;
                margin: 0;
            }
            .hero-sub {
                margin-top: 0.15rem;
                opacity: 0.96;
                font-size: 0.95rem;
            }
            [data-baseweb="tab-list"] {
                gap: 0.3rem;
            }
            [data-baseweb="tab"] {
                background: #eaf2ff;
                border-radius: 10px;
                border: 1px solid #d4e4ff;
                padding: 0.55rem 0.9rem;
                color: #27415f;
            }
            [data-baseweb="tab"][aria-selected="true"] {
                background: linear-gradient(120deg, #0f6ab7 0%, #1d8fd8 100%);
                border-color: transparent;
                color: #ffffff;
            }
            [data-testid="stMetric"] {
                background: #ffffff;
                border: 1px solid #d9e8ff;
                border-left: 6px solid #1d8fd8;
                border-radius: 12px;
                padding: 0.45rem 0.6rem;
                box-shadow: 0 6px 18px rgba(21, 85, 145, 0.08);
            }
            [data-testid="stDataFrame"], [data-testid="stPlotlyChart"] {
                background: #ffffff;
                border: 1px solid #d9e8ff;
                border-radius: 12px;
                padding: 0.35rem;
                box-shadow: 0 8px 24px rgba(23, 64, 105, 0.08);
            }
            .stButton > button {
                border-radius: 10px;
                border: none;
                background: linear-gradient(120deg, #ff8b3d 0%, #ff6f5e 100%);
                color: white;
                font-weight: 600;
            }
            .stButton > button:hover {
                filter: brightness(0.97);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_conn():
    last_err = None
    # Retry transient DNS/network startup race conditions inside docker-compose.
    for attempt in range(1, 6):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as err:
            last_err = err
            if attempt < 5:
                time.sleep(1.5 * attempt)
    raise last_err


@st.cache_data(ttl=30)
def query_df(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_conn()
        try:
            return pd.read_sql_query(sql, conn, params=params)
        finally:
            conn.close()
    except psycopg2.OperationalError:
        st.warning("Database is temporarily unavailable. Retrying on next refresh.")
        return pd.DataFrame()


def age_to_band(age_val):
    if pd.isna(age_val):
        return "Unknown"
    age = int(age_val)
    if age < 25:
        return "18-24"
    if age < 35:
        return "25-34"
    if age < 45:
        return "35-44"
    if age < 55:
        return "45-54"
    if age < 65:
        return "55-64"
    return "65+"


def signal_reasons(row: pd.Series):
    product = row.get("product_family")
    reasons = []

    if product == "home_loan":
        reasons = [
            (row.get("home_improvement_spend_30d", 0), "Home Improvement Spend", "Recent home-improvement spend is high, indicating active renovation needs."),
            (row.get("total_spend_30d", 0), "Overall Spend Momentum", "Overall spending has increased, suggesting near-term financing appetite."),
            (max(0, 10 - row.get("days_since_last_txn", 30)), "Recent Activity", "Customer has transacted recently, so outreach timing is favorable."),
        ]
    elif product == "credit_card":
        reasons = [
            (row.get("txn_count_30d", 0), "Frequent Transactions", "High transaction frequency suggests strong card-usage potential."),
            (row.get("unique_category_count_30d", 0), "Category Diversity", "Spending across many categories fits credit card benefits usage."),
            (1 if row.get("existing_products_flag", 1) == 0 else 0, "No Existing Card Flag", "Customer is not currently marked with this product, indicating upsell headroom."),
        ]
    elif product == "fixed_deposit":
        reasons = [
            (row.get("salary_credit_30d", 0), "Salary Credit Strength", "Strong salary inflows indicate ability to lock funds in deposits."),
            (row.get("campaign_response_flag", 0), "Campaign Responsiveness", "Past campaign responsiveness indicates higher acceptance probability."),
            (max(0, 20 - row.get("days_since_last_txn", 30)), "Recent Banking Activity", "Recent activity suggests the customer is currently engaged."),
        ]
    else:
        reasons = [
            ((row.get("travel_spend_30d", 0) + row.get("electronics_spend_30d", 0)), "Lifestyle Spend Pattern", "Travel/electronics spend suggests potential need for short-term financing."),
            (row.get("salary_credit_30d", 0), "Income Signal", "Stable salary credits support repayment capacity."),
            (row.get("avg_txn_30d", 0), "Ticket Size", "Higher average transaction size indicates credit demand potential."),
        ]

    ordered = sorted(reasons, key=lambda x: float(x[0] or 0), reverse=True)[:3]
    top_text = [f"{name}: {desc}" for _, name, desc in ordered]
    return top_text


def load_realtime_intent_feed(limit_rows: int = 500):
    sql = """
    select
        ro.received_at,
        ro.customer_id,
        coalesce(mcl.campaign_segment,
            case
                when ro.final_score >= 90 then 'HOT'
                when ro.final_score >= 75 then 'WARM'
                else 'WATCH'
            end
        ) as customer_segment,
        ro.product,
        ro.final_score as composite_intent_score,
        ro.trigger_event
    from public.realtime_offers ro
    left join public.mart_campaign_lists mcl
      on mcl.customer_id = ro.customer_id
     and mcl.product_family = ro.product
    order by ro.received_at desc
    limit %s
    """
    return query_df(sql, params=[limit_rows])


def load_weekly_propensity_counts():
    sql = """
    with weekly as (
        select
            product_family,
            date_trunc('week', score_ts)::date as week_start,
            count(*) filter (where propensity_score >= 75) as high_propensity_count
        from public.mart_propensity_scores
        group by 1,2
    )
    select *
    from weekly
    where week_start >= (current_date - interval '14 weeks')
    order by week_start, product_family
    """
    return query_df(sql)


def load_campaign_candidates(product: str, min_score: int, region: str, age_band: str):
    sql = """
    with joined as (
        select
            m.customer_id,
            m.product_family,
            m.propensity_score,
            m.score_ts,
            r.age,
            r.income_band,
            r.total_spend_30d,
            r.txn_count_30d,
            r.avg_txn_30d,
            r.unique_category_count_30d,
            r.home_improvement_spend_30d,
            r.electronics_spend_30d,
            r.travel_spend_30d,
            r.salary_credit_30d,
            r.days_since_last_txn,
            r.existing_products_flag,
            r.campaign_response_flag,
            r.campaign_touch_count,
            case mod(abs(hashtext(m.customer_id)), 4)
                when 0 then 'North'
                when 1 then 'South'
                when 2 then 'East'
                else 'West'
            end as region
        from public.mart_propensity_scores m
        left join raw.propensity_inputs r
          on m.customer_id = r.customer_id
         and m.product_family = r.product_family
    )
    select *
    from joined
    where product_family = %s
      and propensity_score >= %s
      and (%s = 'All' or region = %s)
      and (
            %s = 'All'
            or (
                case
                    when age is null then 'Unknown'
                    when age < 25 then '18-24'
                    when age < 35 then '25-34'
                    when age < 45 then '35-44'
                    when age < 55 then '45-54'
                    when age < 65 then '55-64'
                    else '65+'
                end
            ) = %s
        )
    order by propensity_score desc
    limit 700
    """
    return query_df(sql, params=[product, min_score, region, region, age_band, age_band])


def load_performance_last_12_weeks():
    sql = """
    select
        date_trunc('week', received_at)::date as week_start,
        product,
        count(*) as offers_sent,
        avg(final_score) as avg_final_score
    from public.realtime_offers
    where received_at >= (current_date - interval '12 weeks')
    group by 1,2
    order by 1,2
    """
    return query_df(sql)


def render_realtime_panel(auto_refresh: bool):
    st.subheader("Real-Time Intent Feed")
    st.caption("Latest offer events from Flink-backed realtime stream. Auto-refresh every 30 seconds.")

    feed_df = load_realtime_intent_feed()
    if feed_df.empty:
        st.info("No realtime offers yet.")
        if auto_refresh:
            time.sleep(30)
            st.rerun()
        return

    # Normalise received_at to UTC-aware timestamps for "today" comparison
    if "received_at" in feed_df.columns:
        feed_df["received_at"] = pd.to_datetime(feed_df["received_at"], utc=True, errors="coerce")
    today_start = pd.Timestamp(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0), tz="UTC")
    today_df = feed_df[feed_df["received_at"] >= today_start] if "received_at" in feed_df.columns else feed_df

    # Per-product today-intent count metrics (the "47 Home Loan events this morning" view)
    metric_products = ["home_loan", "credit_card", "fixed_deposit", "personal_loan"]
    metric_cols = st.columns(4)
    for idx, prod in enumerate(metric_products):
        count = int(today_df[today_df["product"] == prod].shape[0])
        with metric_cols[idx]:
            st.metric(label=f"{PRODUCT_LABELS[prod]} — Today", value=count)

    # Product filter
    products_available = ["All"] + sorted(feed_df["product"].dropna().unique().tolist())
    col_filter, _ = st.columns([2, 5])
    with col_filter:
        selected_rt_product = st.selectbox(
            "Filter by Product", options=products_available, index=0, key="rt_product_filter"
        )

    display_feed = feed_df if selected_rt_product == "All" else feed_df[feed_df["product"] == selected_rt_product]
    display_df = display_feed.rename(columns={
        "customer_id": "Customer",
        "customer_segment": "Segment",
        "product": "Product",
        "composite_intent_score": "Composite Intent Score",
        "trigger_event": "Trigger Signal",
        "received_at": "Received At",
    })
    st.dataframe(display_df, width="stretch", hide_index=True)

    if auto_refresh:
        time.sleep(30)
        st.rerun()


def render_weekly_overview_panel():
    st.subheader("Weekly Propensity Overview")

    weekly_df = load_weekly_propensity_counts()
    if weekly_df.empty:
        st.info("No weekly propensity history available yet.")
        return

    product_order = ["home_loan", "credit_card", "fixed_deposit", "personal_loan"]
    latest_week = weekly_df["week_start"].max()
    prev_week = latest_week - timedelta(days=7)

    cols = st.columns(4)
    for idx, product in enumerate(product_order):
        prod_df = weekly_df[weekly_df["product_family"] == product]
        latest_count = int(prod_df.loc[prod_df["week_start"] == latest_week, "high_propensity_count"].sum())
        prev_count = int(prod_df.loc[prod_df["week_start"] == prev_week, "high_propensity_count"].sum())
        wow_change = latest_count - prev_count
        est_revenue = latest_count * 0.02 * ESTIMATED_REVENUE_PER_CONVERSION[product]

        with cols[idx]:
            st.metric(
                label=PRODUCT_LABELS[product],
                value=f"{latest_count:,}",
                delta=f"WoW: {wow_change:+,}",
            )
            st.caption(f"Estimated conversion revenue @2%: ${est_revenue:,.0f}")


def render_campaign_builder_panel():
    st.subheader("Campaign List Builder")
    st.caption("Use product, score, region, and age filters to generate campaign-ready customer lists with reasons.")

    product_options = ["home_loan", "credit_card", "fixed_deposit", "personal_loan"]

    with st.form("campaign_builder_form"):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            selected_product = st.selectbox(
                "Select Product",
                options=product_options,
                format_func=lambda x: PRODUCT_LABELS.get(x, x),
            )
        with c2:
            min_score = st.slider("Minimum Propensity Score", min_value=0, max_value=100, value=75, step=1)
        with c3:
            region = st.selectbox("Region", options=["All", "North", "South", "East", "West"], index=0)
        with c4:
            age_band = st.selectbox("Age Band", options=["All", "18-24", "25-34", "35-44", "45-54", "55-64", "65+", "Unknown"], index=0)

        submitted = st.form_submit_button("Build Campaign List", width="stretch")

    if not submitted:
        st.info("Showing default campaign list. Adjust filters and click Build Campaign List to refresh with your criteria.")

    candidate_df = load_campaign_candidates(selected_product, min_score, region, age_band)
    if candidate_df.empty:
        st.warning("No customers match the selected criteria.")
        return

    candidate_df["age_band"] = candidate_df["age"].apply(age_to_band)
    candidate_df["top_3_signal_reasons"] = candidate_df.apply(
        lambda r: signal_reasons(r), axis=1
    )
    candidate_df["Top 3 Signal Reasons"] = candidate_df["top_3_signal_reasons"].apply(
        lambda reasons: " | ".join([reason.split(":")[0] for reason in reasons])
    )

    st.success(f"Matched customers: {len(candidate_df):,}")

    export_cols = [
        "customer_id", "product_family", "propensity_score",
        "region", "age_band", "income_band", "Top 3 Signal Reasons",
    ]
    export_df = candidate_df[export_cols].rename(columns={
        "customer_id": "Customer ID",
        "product_family": "Product",
        "propensity_score": "Propensity Score",
        "region": "Region",
        "age_band": "Age Band",
        "income_band": "Income Band",
        "Top 3 Signal Reasons": "Top 3 Signal Reasons",
    })

    dl_col, _ = st.columns([2, 5])
    with dl_col:
        st.download_button(
            label=f"Export {len(export_df):,} customers to CRM (CSV)",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name=f"crm_export_{selected_product}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    st.dataframe(
        export_df,
        hide_index=True,
        width="stretch",
    )

    st.subheader("Signal Reason Explainer")
    for _, row in candidate_df.head(50).iterrows():
        title = (
            f"Customer {row['customer_id']} | "
            f"{PRODUCT_LABELS.get(row['product_family'], row['product_family'])} | "
            f"Score {round(float(row['propensity_score']), 2)}"
        )
        with st.expander(title):
            for i, reason in enumerate(row["top_3_signal_reasons"], start=1):
                st.write(f"{i}. {reason}")


def render_performance_panel():
    st.subheader("Campaign Performance Tracker")
    st.caption("Baseline vs propensity-targeted conversion-rate view for last 12 weeks.")

    perf_df = load_performance_last_12_weeks()
    if perf_df.empty:
        st.info("No offer history available yet for the 12-week tracker.")
        return

    week_index = pd.date_range(
        end=datetime.utcnow().date(),
        periods=12,
        freq="W-MON",
    ).date
    products = ["home_loan", "credit_card", "fixed_deposit", "personal_loan"]

    grid = pd.MultiIndex.from_product([week_index, products], names=["week_start", "product"]).to_frame(index=False)
    perf_df = grid.merge(perf_df, on=["week_start", "product"], how="left").fillna({"offers_sent": 0, "avg_final_score": 0})

    perf_df["baseline_rate_pct"] = 1.0
    perf_df["propensity_targeted_rate_pct"] = 2.0
    perf_df["baseline_est_conversions"] = perf_df["offers_sent"] * 0.01
    perf_df["targeted_est_conversions"] = perf_df["offers_sent"] * 0.02

    long_df = perf_df.melt(
        id_vars=["week_start", "product"],
        value_vars=["baseline_rate_pct", "propensity_targeted_rate_pct"],
        var_name="series",
        value_name="conversion_rate_pct",
    )
    long_df["series"] = long_df["series"].replace(
        {
            "baseline_rate_pct": "Baseline",
            "propensity_targeted_rate_pct": "Propensity Targeted",
        }
    )
    long_df["product_label"] = long_df["product"].map(PRODUCT_LABELS)

    fig = px.line(
        long_df,
        x="week_start",
        y="conversion_rate_pct",
        color="series",
        facet_col="product_label",
        facet_col_wrap=2,
        markers=True,
        title="Conversion Rate Comparison by Product (Last 12 Weeks)",
        color_discrete_map={
            "Baseline": "#9fb7d9",
            "Propensity Targeted": "#ff7a59",
        },
    )
    fig.update_yaxes(title_text="Conversion Rate (%)")
    st.plotly_chart(fig, width="stretch")

    revenue_df = perf_df.copy()
    revenue_df["targeted_est_revenue"] = revenue_df.apply(
        lambda r: r["targeted_est_conversions"] * ESTIMATED_REVENUE_PER_CONVERSION[r["product"]], axis=1
    )
    revenue_summary = revenue_df.groupby("product", as_index=False).agg(
        offers_sent=("offers_sent", "sum"),
        targeted_est_conversions=("targeted_est_conversions", "sum"),
        targeted_est_revenue=("targeted_est_revenue", "sum"),
    )
    revenue_summary["Product"] = revenue_summary["product"].map(PRODUCT_LABELS)

    st.dataframe(
        revenue_summary[["Product", "offers_sent", "targeted_est_conversions", "targeted_est_revenue"]].rename(columns={
            "offers_sent": "Offers Sent (12w)",
            "targeted_est_conversions": "Estimated Conversions @2%",
            "targeted_est_revenue": "Estimated Revenue @2%",
        }),
        hide_index=True,
        width="stretch",
    )


def main():
    inject_custom_styles()
    st.markdown(
        """
        <div class="hero">
          <p class="hero-title">Marketing Campaign Builder Dashboard</p>
          <p class="hero-sub">Realtime intent intelligence, propensity planning, and campaign performance in one command center.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Controls")
        auto_refresh = st.toggle("Auto-refresh real-time panel every 30s", value=False)
        st.button("Refresh Now", on_click=query_df.clear)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Real-Time Intent Feed",
        "Weekly Propensity Overview",
        "Campaign Builder + Signal Explainer",
        "Campaign Performance Tracker",
    ])

    with tab1:
        render_realtime_panel(auto_refresh=auto_refresh)

    with tab2:
        render_weekly_overview_panel()

    with tab3:
        render_campaign_builder_panel()

    with tab4:
        render_performance_panel()


if __name__ == "__main__":
    main()