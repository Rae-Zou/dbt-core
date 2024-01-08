with 

source as (
  select * from {{ source('main_jaffle_shop', 'customers') }}
),

transformed as (
  select 
    id as customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name,
    -- add some new simple transformed columns
    first_name || ' ' || last_name as full_name
  from source
)

select * from transformed