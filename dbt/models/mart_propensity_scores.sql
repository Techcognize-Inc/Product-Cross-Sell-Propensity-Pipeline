{{ config(materialized='table') }}

with score_inputs as (
    select * from {{ ref('propensity_inputs') }}
),

weights as (
    select * from {{ ref('weight_seeds') }}
)

select
    customer_id,
    product_family,

    least(100, greatest(0,

        -- Transaction signals
        0.20 * total_spend_30d * weights.total_spend_wt +
        0.15 * txn_count_30d * weights.txn_count_wt +
        0.10 * avg_txn_30d * weights.avg_txn_wt +

        -- Behavioral signals
        0.15 * unique_category_count_30d * weights.diversity_wt +
        0.10 * salary_credit_30d * weights.income_wt +
        0.10 * debit_txn_count_30d * weights.activity_wt +

        -- Recency
        0.10 * (100 - days_since_last_txn) * weights.recency_wt +

        -- Product affinity
        0.20 * existing_products_flag * weights.affinity_wt +

        -- Demographic
        0.05 * age * weights.age_wt

    )) as propensity_score,

    current_timestamp() as score_ts

from score_inputs
join weights
  on weights.product_family = score_inputs.product_family