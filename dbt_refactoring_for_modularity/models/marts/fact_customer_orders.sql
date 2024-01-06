with
-- Import CTEs
orders as (
  select * from {{ ref("int_orders")}}
),

customers as (
  select * from {{ ref('stg_main_jaffle_shop__customers') }}
),
-- Logical CTEs
customer_orders as (
  select
    orders.*,
    customers.full_name,
    customers.surname,
    customers.givenname,
    --- customer level aggregations
    min(orders.order_date) over(
      partition by orders.customer_id
    ) as customers_first_order_date,

    min(orders.valid_order_date) over(
      partition by orders.customer_id
    ) as customers_first_non_returned_order_date,

    max(orders.valid_order_date) over(
      partition by orders.customer_id
    ) as customers_most_recent_non_returned_order_date,

    count(*) over (
      partition by orders.customer_id
    ) as customers_order_count,

    sum(case 
        when orders.valid_order_date is not null 
        then 1 else 0
        end) over (
      partition by orders.customer_id
    ) as customers_non_returned_order_count,

    sum(case 
        when orders.valid_order_date is not null  
        then orders.order_value_dollars else 0 
        end) over (
      partition by orders.customer_id
    ) as customers_total_lifetime_value,
    
    array_agg(distinct orders.order_id) as customers_order_ids

  from orders
  inner join customers
    on orders.customer_id = customers.customer_id
  group by 1,2,3,4,5,6,7,8,9,10

),
---
add_avg_order_values as(
  select
    *,
    customers_total_lifetime_value / customers_non_returned_order_count as customers_avg_non_returned_order_value
  from customer_orders
),
-- Final CTE
final as (
  select 
    order_id,
    customer_id,
    surname,
    givenname,
    customers_first_order_date as first_order_date,
    customers_order_count as order_count,
    customers_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status
  from add_avg_order_values 
  
)
-- Simple select statement
select * from final
