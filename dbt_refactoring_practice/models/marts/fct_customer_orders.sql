WITH 
-- Import CTEs
customers as (
    select * from {{ ref('stg_main_jaffle_shop__customers') }}
),

orders as (
  select * from {{ ref('stg_main_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_main_stripe__payments') }}
),

-- Logical CTEs
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
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join completed_payments ON orders.order_id = completed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 
),

-- Final CTE
final as (
    select
        paid_orders.*,
        -- sales transaction sequence
        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
        -- customer sales sequence
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
        -- new v.s. returning customers
        case  
            when (
                rank() over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
                ) = 1
            ) then 'new'
            else 'return' 
        end as nvsr,
        -- customer lifetime value
         sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
            ) as customer_lifetime_value,
        -- 1st day of sale
        first_value(paid_orders.order_placed_at) over (
            partition by customer_id
            order by paid_orders.order_placed_at
            ) as fdos
    FROM paid_orders
    ORDER BY paid_orders.order_id
)

-- Simple select statement
select * from final
