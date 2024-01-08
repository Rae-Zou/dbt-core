with 

source as (
  select * from {{ source('main_jaffle_shop', 'orders') }}
),

transformed as (
  select 
    id as order_id,
    user_id as customer_id,
    order_date as order_placed_at,
    status as order_status,
    -- add some new simple transformed columns
    case 
        when order_status NOT IN ('returned','return_pending') 
        then order_date 
    end as valid_order_date
  from source
)

select * from transformed