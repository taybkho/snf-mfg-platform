
    
    

with child as (
    select material_id as from_field
    from MFG_ACCELERATOR.STAGING.stg_batches
    where material_id is not null
),

parent as (
    select material_id as to_field
    from MFG_ACCELERATOR.STAGING.stg_materials
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


