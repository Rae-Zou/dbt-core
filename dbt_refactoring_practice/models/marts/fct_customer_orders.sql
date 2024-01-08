WITH 
-- Import CTEs
customers as (
    select * from {{ ref('stg_main_jaffle_shop__customers') }}
),

paid_orders as (

  select * from {{ ref('int_orders') }}

),

-- Final CTE
final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        -- sales transaction sequence
        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
        -- customer sales sequence
        ROW_NUMBER() OVER (PARTITION BY paid_orders.customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
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
         sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
            ) as customer_lifetime_value,
        -- 1st day of sale
        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
            ) as fdos
    FROM paid_orders
    left join customers on paid_orders.customer_id = customers.customer_id
)

-- Simple select statement
select * from final
