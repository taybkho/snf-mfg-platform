select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select manufacturing_site
from MFG_ACCELERATOR.MARTS.mfg_plant_daily_kpis
where manufacturing_site is null



      
    ) dbt_internal_test