select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select batch_id as from_field
    from MFG_ACCELERATOR.STAGING.stg_production_orders
    where batch_id is not null
),

parent as (
    select batch_id as to_field
    from MFG_ACCELERATOR.STAGING.stg_batches
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test