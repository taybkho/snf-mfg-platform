
    
    

select
    batch_id as unique_field,
    count(*) as n_records

from MFG_ACCELERATOR.CORE.core_batches
where batch_id is not null
group by batch_id
having count(*) > 1


