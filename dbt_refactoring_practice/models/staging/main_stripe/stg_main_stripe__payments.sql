with

source as (
   select * from {{ source('main_stripe', 'payments') }}
),

transformed as (
  select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    amount,
    created,
    -- add some new simple transformed columns
    round(amount/100.0,2) as payment_amount
  from source
)

select * from transformed