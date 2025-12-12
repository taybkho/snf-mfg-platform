
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select batch_id
from MFG_ACCELERATOR.ANALYTICS_analytics.core_batches
where batch_id is null



  
  
      
    ) dbt_internal_test