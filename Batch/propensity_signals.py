import os
from pyspark.sql import SparkSession, functions as F


# =========================
# SPARK SESSION
# =========================
def create_spark():
    return (
        SparkSession.builder
        .appName("PropensitySignals")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


# =========================
# LOAD DATA
# =========================
def load_data(spark, base_path):
    base_path = "/data"
    transactions = spark.read.parquet(f"{base_path}/transactions.parquet")
    holdings = spark.read.parquet(f"{base_path}/product_holdings.parquet")
    demographics = spark.read.parquet(f"{base_path}/demographics.parquet")
    campaign = spark.read.parquet(f"{base_path}/campaign_history.parquet")

    return transactions, holdings, demographics, campaign


# =========================
# TRANSACTION FEATURES
# =========================
def build_transaction_features(txn):
    txn = txn.withColumn("txn_dt", F.to_date("transaction_ts"))

    txn_30d = txn.filter(
        F.col("txn_dt") >= F.date_sub(F.current_date(), 30)
    )

    return txn_30d.groupBy("customer_id").agg(
        F.sum(F.abs("amount")).alias("total_spend_30d"),
        F.count("*").alias("txn_count_30d"),
        F.avg(F.abs("amount")).alias("avg_txn_30d"),
        F.countDistinct("merchant_category").alias("unique_category_count_30d"),

        F.sum(F.when(F.col("merchant_category") == "home_improvement", F.abs("amount")).otherwise(0))
        .alias("home_improvement_spend_30d"),

        F.sum(F.when(F.col("merchant_category") == "electronics", F.abs("amount")).otherwise(0))
        .alias("electronics_spend_30d"),

        F.sum(F.when(F.col("merchant_category") == "travel", F.abs("amount")).otherwise(0))
        .alias("travel_spend_30d"),

        F.sum(F.when(F.col("transaction_type") == "salary", F.abs("amount")).otherwise(0))
        .alias("salary_credit_30d"),

        F.count(F.when(F.col("transaction_type") == "debit", True))
        .alias("debit_txn_count_30d"),

        F.max("txn_dt").alias("last_txn_date")
    )


# =========================
# SIGNAL BUILDING
# =========================
def build_signals(transactions, holdings, demographics, campaign):

    txn_features = build_transaction_features(transactions)

    base = (
        txn_features
        .join(holdings, "customer_id", "left")
        .join(demographics, "customer_id", "left")
        .join(campaign, "customer_id", "left")
    )

    base = base.withColumn(
        "days_since_last_txn",
        F.datediff(F.current_date(), F.col("last_txn_date"))
    )

    # 4 product families → explode
    products = ["credit_card", "personal_loan", "home_loan", "fixed_deposit"]

    df = base.withColumn(
        "product_family",
        F.explode(F.array([F.lit(p) for p in products]))
    )

    # FIXED: proper product signal instead of constant
    df = df.withColumn(
        "existing_products_flag",
        F.when(F.col("has_" + F.col("product_family")).isNotNull(), 1).otherwise(0)
    )

    df = df.withColumn(
        "campaign_response_flag",
        F.when(F.col("last_campaign_response") == "yes", 1).otherwise(0)
    )

    return df.select(
        "customer_id",
        "product_family",

        # transaction signals
        "total_spend_30d",
        "txn_count_30d",
        "avg_txn_30d",
        "unique_category_count_30d",
        "home_improvement_spend_30d",
        "electronics_spend_30d",
        "travel_spend_30d",
        "salary_credit_30d",
        "debit_txn_count_30d",
        "days_since_last_txn",

        # product signals
        "existing_products_flag",

        # demographics
        "age",
        "income_band",

        # campaign
        "campaign_response_flag",
        "campaign_touch_count"
    )


# =========================
# WRITE TO POSTGRES
# =========================
def write_to_postgres(df):

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "raw.propensity_inputs") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "10000") \
        .mode("overwrite") \
        .save()


# =========================
# MAIN PIPELINE
# =========================
def main():

    spark = create_spark()

    base_path = "/data"   # inside docker volume

    transactions, holdings, demographics, campaign = load_data(spark, base_path)

    signals = build_signals(transactions, holdings, demographics, campaign)

    # 🚀 LOAD INTO POSTGRES (RAW LAYER)
    write_to_postgres(signals)

    spark.stop()


if __name__ == "__main__":
    main()