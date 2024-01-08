WITH 
-- Import CTEs
orders as (
    select * from {{ ref("stg_main_jaffle_shop__orders")}}
),

customers as (
    select * from {{ ref('stg_main_jaffle_shop__customers') }}
),

payments as (
    select * from {{ ref('stg_main_stripe__payments') }}
),
-- Logical CTEs
paid_orders as (
    select 
        orders.*,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.customer_first_name,
        C.customer_last_name
    FROM orders
    left join (
        select 
            order_id, 
            max(CREATED) as payment_finalized_date, 
            sum(AMOUNT) / 100.0 as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1
        ) p ON orders.order_id = p.order_id
    left join customers C on orders.customer_id = C.customer_id 
),
customer_orders as (
    select 
        C.customer_id
        , min(order_placed_at) as first_order_date
        , max(order_placed_at) as most_recent_order_date
        , count(order_id) AS number_of_orders
    from customers C 
    left join orders
    on orders.customer_id = C.customer_id 
    group by 1)


-- Final CTE
select
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at
    THEN 'new'
    ELSE 'return' END as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
FROM paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN 
(
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
ORDER BY p.order_id
