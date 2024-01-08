-- a reuseable Intermediate model for paid order payments
with

orders as (
  select * from {{ ref('stg_main_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_main_stripe__payments') }}
),

completed_payments as (
    select 
        order_id, 
        max(payment_created_at) as payment_finalized_date, 
        sum(payment_amount) / 100.0 as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1 
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date
    from orders
    left join completed_payments p ON orders.order_id = p.order_id 
)


select * from paid_orders