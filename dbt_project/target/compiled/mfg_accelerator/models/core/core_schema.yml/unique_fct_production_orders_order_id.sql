
    
    

select
    order_id as unique_field,
    count(*) as n_records

from MFG_ACCELERATOR.CORE.fct_production_orders
where order_id is not null
group by order_id
having count(*) > 1


