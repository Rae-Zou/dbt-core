WITH 
-- Import CTEs
orders as (
    select * from {{ ref('int_orders') }}
),

customers as (
    select * from {{ ref('stg_main_jaffle_shop__customers') }}
),

-- Logical CTEs
customer_orders as (
    select 
        C.customer_id
        , min(order_placed_at) as first_order_date
        , max(order_placed_at) as most_recent_order_date
        , count(order_id) AS number_of_orders
    from customers C 
    left join orders
    on orders.customer_id = C.customer_id 
    group by 1
),

-- Final CTE
final as (
    select
        p.*,
        ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
        CASE 
            WHEN c.first_order_date = p.order_placed_at
            THEN 'new'
            ELSE 'return' 
        END as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos
    FROM orders p
    left join customer_orders as c USING (customer_id)
    LEFT OUTER JOIN 
    (
        select
            p.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from orders p
        left join orders t2 
            on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ) x on x.order_id = p.order_id
    ORDER BY p.order_id
)

-- Simple select statement
select * from final
