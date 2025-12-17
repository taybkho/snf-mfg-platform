-- fct_production_orders.sql
-- Purpose:
--  - Fact-like table combining production orders with batch and material context
--  - Compute planned vs actual durations and simple on-time indicators

with orders as (

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
    from MFG_ACCELERATOR.STAGING.stg_production_orders

),

batches as (

    select
        batch_id,
        material_id,
        manufacturing_site,
        manufacture_date,
        expiry_date,
        quantity,
        batch_status
    from MFG_ACCELERATOR.STAGING.stg_batches

),

materials as (

    select
        material_id,
        material_name,
        material_type,
        unit_of_measure
    from MFG_ACCELERATOR.CORE.core_materials

),

joined as (

    select
        o.order_id,
        o.batch_id,
        b.material_id,

        m.material_name,
        m.material_type,
        m.unit_of_measure,

        b.manufacturing_site,
        b.manufacture_date,
        b.expiry_date,
        b.quantity,
        b.batch_status,

        o.order_type,
        o.line_id,
        o.order_status,

        o.planned_start_date,
        o.planned_end_date,
        o.actual_start_date,
        o.actual_end_date,

        -- Planned duration in days
        datediff('day', planned_start_date, planned_end_date) as planned_duration_days,

        -- Actual duration in days (null if no end date)
        case
            when actual_start_date is not null
             and actual_end_date is not null
            then datediff('day', actual_start_date, actual_end_date)
            else null
        end as actual_duration_days,

        -- Delay vs plan: how many days longer the actual duration was
        case
            when actual_start_date is not null
             and actual_end_date is not null
            then datediff('day', planned_end_date, actual_end_date)
            else null
        end as delay_vs_plan_days,

        -- Simple late flag: 1 if order completed after planned_end_date
        case
            when actual_end_date is not null
             and actual_end_date > planned_end_date
            then 1
            else 0
        end as is_late,

        -- Is the order completed?
        case
            when upper(order_status) = 'COMPLETED' then 1
            else 0
        end as is_completed
    from orders o
    left join batches b
        on o.batch_id = b.batch_id
    left join materials m
        on b.material_id = m.material_id

)

select * from joined