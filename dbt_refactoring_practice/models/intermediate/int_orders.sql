with

customers as (
  select * from {{ ref('stg_main_jaffle_shop__customers') }}
),

orders as (
  select * from {{ ref('stg_main_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_main_stripe__payments') }}
),

completed_payments as (
    select 
        order_id, 
        max(CREATED) as payment_finalized_date, 
        sum(AMOUNT) / 100.0 as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1 
),

paid_orders as (
    select 
        orders.*,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.customer_first_name,
        C.customer_last_name
    from orders
    left join completed_payments p ON orders.order_id = p.order_id
    left join customers C on orders.customer_id = C.customer_id 
)


select * from paid_orders