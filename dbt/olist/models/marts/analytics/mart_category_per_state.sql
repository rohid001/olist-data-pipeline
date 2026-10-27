-- Port Q8 project DA (sql/06_category_region_analysis.sql): kategori dengan margin kasar
-- terbesar di tiap state. Margin kasar = price - freight_vlue (dataset tidak punya
-- data harga pokok, jadi ini bukan profit akuntansi).
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

category_state_revenue as (

    select
        orders.customer_state,
        products.product_category as category,
        sum(order_items.price - order_items.freight_value) as gross_margin,
        sum(order_items.price) as revenue
    from orders
    inner join order_items on orders.order_id = order_items.order_id
    inner join products on order_items.product_id = products.product_id
    where orders.order_status = 'delivered'
    group by 1, 2

),

ranked as (

    select
        *,
        rank() over (partition by customer_state order by gross_margin desc) as margin_rank
    from category_state_revenue

)

select
    customer_state,
    category,
    round(gross_margin, 2) as gross_margin,
    round(revenue, 2) as revenue
from ranked
where margin_rank = 1