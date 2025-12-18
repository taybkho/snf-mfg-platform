
    
    

select
    material_id as unique_field,
    count(*) as n_records

from MFG_ACCELERATOR.CORE.core_materials
where material_id is not null
group by material_id
having count(*) > 1


