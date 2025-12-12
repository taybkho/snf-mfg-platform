-- mfg_plant_daily_kpis.sql
-- Purpose:
--  - Provide daily plant-level KPIs for manufacturing performance monitoring
--  - Aggregates metrics from core_batches and fct_production_orders
-- Grain:
--  - One row per (manufacturing_site, calendar_date)

with calendar_days as (

    -- Build a simple date spine based on manufacture_date and planned/actual dates
    -- This ensures we have a consistent daily grain for metrics.

    select
        -- Use the earliest relevant date from batches and orders
        least(
            min(b.manufacture_date),
            min(o.planned_start_date)
        ) as min_date,
        greatest(
            max(b.manufacture_date),
            max(o.planned_end_date)
        ) as max_date
    from MFG_ACCELERATOR.ANALYTICS_analytics.core_batches b
    cross join MFG_ACCELERATOR.ANALYTICS_analytics.fct_production_orders o

),

date_spine as (

    -- Generate a list of dates between min_date and max_date
    -- Snowflake-specific generator using TABLE(GENERATOR)
    select
        dateadd(
            day,
            seq4(),
            min_date
        ) as calendar_date
    from calendar_days,
         table(generator(rowcount => datediff('day', min_date, max_date) + 1))

),

batches_by_day as (

    -- Aggregate batch metrics at (manufacturing_site, calendar_date)
    select
        b.manufacturing_site,
        b.manufacture_date as calendar_date,

        count(*) as total_batches,
        sum(b.quantity) as total_quantity,

        sum(case when b.is_expired = 1 then 1 else 0 end) as expired_batches
    from MFG_ACCELERATOR.ANALYTICS_analytics.core_batches b
    group by
        b.manufacturing_site,
        b.manufacture_date

),

orders_by_day as (

    -- Aggregate order metrics at (manufacturing_site, calendar_date)
    -- We'll use planned_start_date as the main "date" for the order
    select
        b.manufacturing_site,
        o.planned_start_date as calendar_date,

        count(*) as total_orders,
        sum(case when o.is_completed = 1 then 1 else 0 end) as completed_orders,
        sum(case when o.is_late = 1 then 1 else 0 end) as late_orders,

        avg(o.delay_vs_plan_days) as avg_delay_vs_plan_days
    from MFG_ACCELERATOR.ANALYTICS_analytics.fct_production_orders o
    left join MFG_ACCELERATOR.ANALYTICS_analytics.core_batches b
        on o.batch_id = b.batch_id
    group by
        b.manufacturing_site,
        o.planned_start_date

),

combined as (

    -- Combine batch and order metrics on (site, date)
    select
        coalesce(b.manufacturing_site, o.manufacturing_site) as manufacturing_site,
        coalesce(b.calendar_date, o.calendar_date) as calendar_date,

        -- Batch metrics
        coalesce(b.total_batches, 0) as total_batches,
        coalesce(b.total_quantity, 0) as total_quantity,
        coalesce(b.expired_batches, 0) as expired_batches,

        -- Order metrics
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.completed_orders, 0) as completed_orders,
        coalesce(o.late_orders, 0) as late_orders,
        o.avg_delay_vs_plan_days as avg_delay_vs_plan_days
    from batches_by_day b
    full outer join orders_by_day o
        on  b.manufacturing_site = o.manufacturing_site
        and b.calendar_date = o.calendar_date

),

final as (

    select
        manufacturing_site,
        calendar_date,

        total_batches,
        total_quantity,
        expired_batches,

        total_orders,
        completed_orders,
        late_orders,
        avg_delay_vs_plan_days,

        -- On-time rate: percentage of completed orders that were not late
        case
            when completed_orders > 0 then
                1.0 - (late_orders::float / completed_orders::float)
            else null
        end as on_time_rate
    from combined
    where manufacturing_site is not null
)

select * from final