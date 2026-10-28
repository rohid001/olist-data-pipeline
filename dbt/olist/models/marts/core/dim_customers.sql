-- Satu baris per customer_unique_id (pelanggan asli, 96.096 orang).
-- Satu pelanggan bisa punya beberapa customer_id (satu per order), jadi alamatnya
-- diambil dari order terbaru, ditambah ringkasan riwayat order-nya.
with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

customer_orders as (

    select
        customers.customer_unique_id,
        customers.customer_zip_code_prefix,
        customers.customer_city,
        customers.customer_state,
        row_number() over (
            partition by customers.customer_unique_id
            order by orders.order_purchase_timestamp desc, orders.order_id asc
        ) as recency_rank,
        count(*) over (partition by customers.customer_unique_id) as order_count,
        min(orders.order_purchase_timestamp) over (
            partition by customers.customer_unique_id
        ) as first_order_at,
        max(orders.order_purchase_timestamp) over (
            partition by customers.customer_unique_id
        ) as last_order_at
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id

)

select
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    order_count,
    first_order_at,
    last_order_at
from customer_orders
where recency_rank = 1
