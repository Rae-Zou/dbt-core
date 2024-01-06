with

orders as (
  select * from {{ ref('stg_main_jaffle_shop__orders') }}
  where order_status != 'fail' -- centralize the filters, it can be filtered beforehand
),
payments as (
  select * from {{ ref('stg_main_stripe__payments') }}
),
---
order_totals as (
  select
    order_id,
    sum(payment_amount) as order_value_dollars
  from payments
  group by 1

),

order_values_joined as (
  select
    orders.*,
    order_totals.order_value_dollars
  from orders
  left join order_totals
    on orders.order_id = order_totals.order_id
)

select * from order_values_joined