-- Port sql/07_overview_kpi.sql project DA (KPI halaman overview) ditambah Q6a
-- (sql/05_churn_indicator.sql: rata-rata jeda antar-order pelanggan repeat).
-- Hasilnya 1 baris
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

reviews as (

    select * from {{ ref('stg_order_reviews') }}

),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

delivered_items as (

    select
        orders.order_id,
        orders.customer_unique_id,
        order_items.price,
        order_items.seller_id
    from orders
    inner join order_items on orders.order_id = order_items.order_id
    where orders.order_status = 'delivered'

),

kpi as (

    select
        count(distinct order_id) as total_orders,
        count(distinct customer_unique_id) as total_customers,
        round(sum(price), 2) as total_revenue,
        round(sum(price) / count(distinct order_id), 2) as avg_order_value,
        count(distinct seller_id) as total_sellers
    from delivered_items

),

review_summary as (

    select round(avg(review_score), 2) as avg_review_score from reviews

),

order_gaps as (

    select
        order_purchase_timestamp - lag(order_purchase_timestamp) over (
            partition by customer_unique_id order by order_purchase_timestamp
        )as gap
    from customer_orders

),

gap_summary as (

    select
        -- extract(day ...) mengambil bagian hari dair interval, sama seperti query DA
        round(avg(extract(day from gap)), 1) as avg_repeat_order_gap_days,
        count(*) as repeat_order_gap_count
    from order_gaps
    where gap is not null

)

select
    kpi.total_orders,
    kpi.total_customers,
    kpi.total_revenue,
    kpi.avg_order_value,
    kpi.total_sellers,
    review_summary.avg_review_score,
    gap_summary.avg_repeat_order_gap_days,
    gap_summary.repeat_order_gap_count
from kpi
cross join review_summary
cross join gap_summary