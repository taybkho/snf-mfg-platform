-- mfg_ml_features_orders.sql
-- Purpose:
--  - Provide ML-ready features at order grain
--  - Target use case: predict late orders or delay vs plan
-- Grain:
--  - One row per production order (order_id)

with orders as (

    select
        order_id,
        batch_id,
        material_id,

        manufacturing_site,
        line_id,

        order_type,
        order_status,

        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,

        planned_duration_days,
        actual_duration_days,
        delay_vs_plan_days,
        is_late,
        is_completed
    from MFG_ACCELERATOR.CORE.fct_production_orders

),

materials as (

    select
        material_id,
        material_type,
        unit_of_measure,
        is_active_material
    from MFG_ACCELERATOR.CORE.core_materials

),

batches as (

    select
        batch_id,
        manufacture_date,
        expiry_date,
        quantity,
        batch_status,
        shelf_life_days,
        batch_age_days,
        days_to_expiry,
        is_expired
    from MFG_ACCELERATOR.CORE.core_batches

),

joined as (

    select
        o.order_id,

        -- Keys and IDs
        o.batch_id,
        o.material_id,

        -- Plant / line / type
        o.manufacturing_site,
        o.line_id,
        o.order_type,
        o.order_status,

        -- Material features
        m.material_type,
        m.unit_of_measure,
        m.is_active_material,

        -- Batch features
        b.quantity,
        b.batch_status,
        b.shelf_life_days,
        b.batch_age_days,
        b.days_to_expiry,
        b.is_expired,

        -- Temporal features
        o.planned_start_date,
        o.planned_end_date,
        o.actual_start_date,
        o.actual_end_date,

        o.planned_duration_days,
        o.actual_duration_days,
        o.delay_vs_plan_days,

        -- Labels / targets
        o.is_late,
        o.is_completed
    from orders o
    left join materials m
        on o.material_id = m.material_id
    left join batches b
        on o.batch_id = b.batch_id

),

final as (

    select
        -- Primary key
        order_id,

        -- Categorical features
        manufacturing_site,
        line_id,
        order_type,
        order_status,
        material_type,
        unit_of_measure,
        batch_status,

        -- Material state
        is_active_material,

        -- Quantitative batch features
        quantity,
        shelf_life_days,
        batch_age_days,
        days_to_expiry,
        is_expired,

        -- Temporal planning features
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        planned_duration_days,
        actual_duration_days,

        -- Label / target variables
        delay_vs_plan_days,
        is_late,
        is_completed
    from joined
)

select * from final