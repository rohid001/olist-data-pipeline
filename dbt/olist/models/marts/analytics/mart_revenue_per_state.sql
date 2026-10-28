-- Port Q9 project DA (sql/06_category_region_analysis.sql): revenue per state dan
-- revenue per pelanggan.
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

state_revenue as (

    select
        orders.customer_state,
        sum(order_items.price) as total_revenue,
        count(distinct orders.customer_unique_id) as customer_count,
        count(distinct orders.order_id) as order_count
    from orders
    inner join order_items on orders.order_id = order_items.order_id
    where orders.order_status = 'delivered'
    group by orders.customer_state

)

select
    customer_state,
    round(total_revenue, 2) as total_revenue,
    customer_count,
    order_count,
    round(total_revenue / customer_count, 2) as revenue_per_customer
from state_revenue
