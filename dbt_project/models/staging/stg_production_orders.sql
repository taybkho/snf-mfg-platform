-- stg_production_orders.sql
-- Purpose:
--  - Clean and standardize production order data
--  - Ensure date fields and statuses are typed and named consistently

with source as (

    select
        order_id,
        batch_id,
        order_type,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        order_status,
        line_id
    from {{ source('raw', 'production_orders') }}

),

renamed as (

    select
        order_id                               as order_id,
        batch_id                               as batch_id,
        order_type                             as order_type,
        cast(planned_start_date as date)       as planned_start_date,
        cast(planned_end_date as date)         as planned_end_date,
        cast(actual_start_date as date)        as actual_start_date,
        cast(actual_end_date as date)          as actual_end_date,
        order_status                           as order_status,
        line_id                                as line_id
    from source

)

select * from renamed
