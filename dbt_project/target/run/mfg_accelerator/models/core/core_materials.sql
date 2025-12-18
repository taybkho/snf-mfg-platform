
  
    

        create or replace transient table MFG_ACCELERATOR.CORE.core_materials
         as
        (-- core_materials.sql
-- Purpose:
--  - Create a cleaned, enriched material dimension
--  - Add simple derived attributes (e.g. is_active flag, age)

with base as (

    -- Use the staging view as the single source of truth for materials
    select
        material_id,
        material_name,
        material_type,
        unit_of_measure,
        material_status,
        created_at
    from MFG_ACCELERATOR.STAGING.stg_materials

),

enriched as (

    select
        material_id,
        material_name,
        material_type,
        unit_of_measure,
        material_status,

        created_at,

        -- Age of the material definition in days
        datediff('day', created_at, current_date()) as material_age_days,

        -- Simple flag to say if the material is considered active
        case
            when upper(material_status) = 'ACTIVE' then 1
            else 0
        end as is_active_material
    from base

)

select * from enriched
        );
      
  