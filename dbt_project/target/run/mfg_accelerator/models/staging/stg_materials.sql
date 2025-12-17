
  create or replace   view MFG_ACCELERATOR.STAGING.stg_materials
  
   as (
    -- stg_materials.sql
-- Purpose:
--  - Create a clean, standardized view of materials
--  - Apply basic typing and naming conventions
--  - This is the first "T" step after RAW

with source as (

    -- Pull data from the RAW.MATERIALS table registered as a source
    select
        material_id,
        material_name,
        material_type,
        unit_of_measure,
        status,
        created_at
    from MFG_ACCELERATOR.RAW.materials

),

renamed as (

    -- Apply any renaming / typing here
    select
        material_id                as material_id,        -- keep same, but explicitly select
        material_name              as material_name,
        material_type              as material_type,
        unit_of_measure            as unit_of_measure,
        status                     as material_status,    -- rename to avoid generic 'status'
        cast(created_at as date)   as created_at
    from source

)

select * from renamed
  );

