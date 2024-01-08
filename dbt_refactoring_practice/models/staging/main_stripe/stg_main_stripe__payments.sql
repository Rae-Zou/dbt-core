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
    created
  from source
)

select * from transformed