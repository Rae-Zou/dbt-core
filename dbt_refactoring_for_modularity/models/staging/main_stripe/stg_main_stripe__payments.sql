with

source as (
   select * from {{ source('main_stripe', 'raw_payments') }}
),

transformed as (
  select
    id as payment_id,
    order_id,
    payment_method,
    round(amount/100.0,2) as payment_amount
    
  from source
)

select * from transformed