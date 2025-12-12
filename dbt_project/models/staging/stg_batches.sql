-- stg_batches.sql
-- Purpose:
--  - Clean and standardize batch data
--  - Ensure dates and numeric types are correct

with source as (

    select
        batch_id,
        material_id,
        manufacturing_site,
        manufacture_date,
        expiry_date,
        quantity,
        batch_status
    from {{ source('raw', 'batches') }}

),

renamed as (

    select
        batch_id                         as batch_id,
        material_id                      as material_id,
        manufacturing_site               as manufacturing_site,
        cast(manufacture_date as date)   as manufacture_date,
        cast(expiry_date as date)        as expiry_date,
        cast(quantity as number)         as quantity,
        batch_status                     as batch_status
    from source

)

select * from renamed
