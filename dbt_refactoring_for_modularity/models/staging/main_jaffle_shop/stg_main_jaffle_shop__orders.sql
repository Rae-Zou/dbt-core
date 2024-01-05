with 

source as (
  select * from {{ source('main_jaffle_shop', 'raw_orders') }}
),

transformed as (
  select 
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status,
    row_number() over (partition by user_id order by order_date, id) as user_order_seq,
    
  from source
)

select * from transformed