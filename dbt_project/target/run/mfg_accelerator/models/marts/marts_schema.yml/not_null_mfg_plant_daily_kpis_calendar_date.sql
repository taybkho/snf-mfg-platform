
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select calendar_date
from MFG_ACCELERATOR.ANALYTICS_analytics.mfg_plant_daily_kpis
where calendar_date is null



  
  
      
    ) dbt_internal_test