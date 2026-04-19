
  
    

  create  table "airflow"."public"."propensity_inputs__dbt_tmp"
  
  
    as
  
  (
    select * from "airflow"."raw"."propensity_inputs"
  );
  