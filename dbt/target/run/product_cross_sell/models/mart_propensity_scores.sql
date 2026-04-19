
  
    

  create  table "airflow"."public"."mart_propensity_scores__dbt_tmp"
  
  
    as
  
  (
    

with score_inputs as (
    select * from "airflow"."public"."propensity_inputs"
),

weights as (
    select * from "airflow"."public"."weight_seeds"
),

normalized_signals as (
    select
        si.customer_id,
        si.product_family,
        
        -- Normalize transaction signals (0-100 scale)
        least(100, (si.total_spend_30d / 1000.0) * 100) as total_spend_normalized,
        least(100, (si.txn_count_30d / 20.0) * 100) as txn_count_normalized,
        least(100, (si.avg_txn_30d / 500.0) * 100) as avg_txn_normalized,
        least(100, (si.unique_category_count_30d / 5.0) * 100) as diversity_normalized,
        
        -- Income signal
        least(100, (si.salary_credit_30d / 5000.0) * 100) as income_normalized,
        
        -- Activity signals
        least(100, (si.debit_txn_count_30d / 20.0) * 100) as activity_normalized,
        
        -- Recency (invert days since last transaction)
        least(100, greatest(0, (90 - si.days_since_last_txn) / 0.9)) as recency_normalized,
        
        -- Product affinity (binary)
        (si.existing_products_flag * 100.0) as affinity_normalized,
        
        -- Demographic (normalize age to 0-100, assuming 18-70 range)
        least(100, greatest(0, ((si.age - 18) / 52.0) * 100)) as age_normalized,
        
        -- Campaign engagement bonus
        (si.campaign_response_flag * 50.0) as campaign_bonus,
        least(50, (si.campaign_touch_count / 5.0) * 50) as touch_bonus,
        
        w.total_spend_wt,
        w.txn_count_wt,
        w.avg_txn_wt,
        w.diversity_wt,
        w.income_wt,
        w.activity_wt,
        w.recency_wt,
        w.affinity_wt,
        w.age_wt
        
    from score_inputs si
    join weights w on w.product_family = si.product_family
)

select
    customer_id,
    product_family,
    
    least(100, greatest(0,
        -- Weighted signal combination
        (total_spend_normalized * total_spend_wt) +
        (txn_count_normalized * txn_count_wt) +
        (avg_txn_normalized * avg_txn_wt) +
        (diversity_normalized * diversity_wt) +
        (income_normalized * income_wt) +
        (activity_normalized * activity_wt) +
        (recency_normalized * recency_wt) +
        (affinity_normalized * affinity_wt) +
        (age_normalized * age_wt) +
        
        -- Campaign engagement bonus (adds up to 10 points)
        (campaign_bonus + touch_bonus) / 10.0
    )) as propensity_score,
    
    current_timestamp as score_ts

from normalized_signals
order by propensity_score desc
  );
  