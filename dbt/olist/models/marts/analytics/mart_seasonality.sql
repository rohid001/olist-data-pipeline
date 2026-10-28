-- Port Q5 lanjutan project DA (sql/04_revenue_trend.sql): revenue per bulan kalender,
-- digabung semua tahun. Catatan DA: Sep-Des hanya punya 1 tahun data, jadi ini bukan
-- pola musiman asli.
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

)

select
    extract(month from orders.order_purchase_timestamp)::int as month_number,
    --trim: to_char(..., 'Month') menambah spasi sampai 9 huruf, misalnya 'May      '
    trim(to_char(orders.order_purchase_timestamp, 'Month')) as month_name,
    round(sum(order_items.price), 2) as total_revenue,
    count(distinct orders.order_id) as order_count
from orders
inner join order_items on orders.order_id = order_items.order_id
where orders.order_status = 'delivered'
group by 1, 2
