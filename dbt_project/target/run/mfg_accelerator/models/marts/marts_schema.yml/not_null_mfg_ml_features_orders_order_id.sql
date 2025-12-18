select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_id
from MFG_ACCELERATOR.MARTS.mfg_ml_features_orders
where order_id is null



      
    ) dbt_internal_test