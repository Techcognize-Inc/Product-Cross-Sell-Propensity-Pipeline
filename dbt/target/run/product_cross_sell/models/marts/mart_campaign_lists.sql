
  
    

  create  table "airflow"."public"."mart_campaign_lists__dbt_tmp"
  
  
    as
  
  (
    -- mart_campaign_lists.sql
-- Generates campaign lists for high-propensity customers with ranking and segmentation

with base_scores as (

    select
        customer_id,
        product_family,
        propensity_score,
        score_ts
    from "airflow"."public"."mart_propensity_scores"

),

filtered as (

    select *
    from base_scores
    where propensity_score >= 60   -- campaign threshold
),

ranked as (

    select
        *,
        row_number() over (
            partition by product_family
            order by propensity_score desc
        ) as rank_in_product
    from filtered
),

final as (

    select
        customer_id,
        product_family,
        propensity_score,
        score_ts,

        case
            when product_family = 'home_loan' and propensity_score >= 85 then 'HOME_LOAN_HOT'
            when product_family = 'home_loan' then 'HOME_LOAN_WARM'

            when product_family = 'credit_card' and propensity_score >= 85 then 'CC_HOT'
            when product_family = 'credit_card' then 'CC_WARM'

            when product_family = 'fixed_deposit' then 'FD_TARGET'

            when product_family = 'personal_loan' then 'PL_TARGET'
        end as campaign_segment,

        current_timestamp as generated_at

    from ranked
    where rank_in_product <= 5000   -- top 5000 per product

)

select * from final
  );
  