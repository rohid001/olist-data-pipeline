-- Port Q4 project DA (sql/04_revenue_trend.sql): growth revenue kategori 2017 vs 2018.
-- 2016 tidak dipakai karena datanya terlalu sedikit untuk jadi baseline.
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

category_yearly as (

    select
        products.product_category as category,
        extract(year from orders.order_purchase_timestamp) as order_year,
        sum(order_items.price) as revenue
    from orders
    inner join order_items on orders.order_id = order_items.order_id
    inner join products on order_items.product_id = products.product_id
    where
        orders.order_status = 'delivered'
        and extract(year from orders.order_purchase_timestamp) in (2017, 2018)
    group by 1, 2

),

category_growth as (

    select
        category,
        sum(case when order_year = 2017 then revenue else 0 end) as revenue_2017,
        sum(case when order_year = 2018 then revenue else 0 end) as revenue_2018
    from category_yearly
    group by category

)

select
    category,
    round(revenue_2017, 2) as revenue_2017,
    round(revenue_2018, 2) as revenue_2018,
    round(100.0 * (revenue_2018 - revenue_2017) / nullif(revenue_2017, 0), 2) as growth_pct
from category_growth
-- Kategori tanpa revenue 2017 dibuang: growth % dari nol tidak bermakna
where revenue_2017 > 0