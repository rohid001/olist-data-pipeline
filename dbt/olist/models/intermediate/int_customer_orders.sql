-- Satu baris per order berstatus delivered, lengkap dengan customer_unique_id.
-- Dasar semua mart pelanggan (RFM, buyer type, churn). Dua aturan project DA ada di sini:
-- identitas pelanggan = customer_unique_id (customer_id dibuat baru untuk tiap order),
-- dan hanya order delivered yang dihitung.
with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

totals as (

    select * from {{ ref('int_order_totals') }}

)

select
    customers.customer_unique_id,
    orders.order_id,
    orders.order_purchase_timestamp,
    totals.payment_total
from orders
inner join customers
    on orders.customer_id = customers.customer_id
left join totals
    on orders.order_id = totals.order_id
where orders.order_status = 'delivered'
