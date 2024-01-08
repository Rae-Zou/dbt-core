with 

source as (
  select * from {{ source('main_jaffle_shop', 'orders') }}
),

transformed as (
  select 
    id as order_id,
    user_id as customer_id,
    order_date as order_placed_at,
    status as order_status
  from source
)

select * from transformed