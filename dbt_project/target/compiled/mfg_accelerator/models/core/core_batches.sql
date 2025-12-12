-- core_batches.sql
-- Purpose:
--  - Combine batch info with material attributes
--  - Add shelf-life and expiry-related metrics

with batches as (

    select
        batch_id,
        material_id,
        manufacturing_site,
        manufacture_date,
        expiry_date,
        quantity,
        batch_status
    from MFG_ACCELERATOR.ANALYTICS_analytics.stg_batches

),

materials as (

    select
        material_id,
        material_name,
        material_type,
        unit_of_measure,
        is_active_material
    from MFG_ACCELERATOR.ANALYTICS_analytics.core_materials

),

joined as (

    select
        b.batch_id,
        b.material_id,

        m.material_name,
        m.material_type,
        m.unit_of_measure,
        m.is_active_material,

        b.manufacturing_site,
        b.manufacture_date,
        b.expiry_date,
        b.quantity,
        b.batch_status,

        -- Shelf life in days = expiry - manufacture
        datediff('day', manufacture_date, expiry_date) as shelf_life_days,

        -- Batch age in days from manufacture date until today
        datediff('day', manufacture_date, current_date()) as batch_age_days,

        -- Days until expiry (can be negative if already expired)
        datediff('day', current_date(), expiry_date) as days_to_expiry,

        case
            when current_date() > expiry_date then 1
            else 0
        end as is_expired
    from batches b
    left join materials m
        on b.material_id = m.material_id

)

select * from joined