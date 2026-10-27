-- Satu baris per order. Model incremental: setiap run hanya memproses order dari
-- batch yang dimuat ke raw setelah run terakhir (dilihat dari _loaded_at), lalu
-- mengganti baris dengan order_id yang sama (delete+insert), jadi aman dijalankan ulang.

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    -- {{ this }} = tabel fct_orders yang sudah ada di warehouse
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

totals as (

    select * from {{ ref('int_order_totals') }}

)

select
    orders.order_id,
    orders.customer_id,
    customers.customer_unique_id,
    customers.customer_state,
    orders.order_status,
    orders.order_purchase_timestamp,
    date_trunc('month', orders.order_purchase_timestamp)::date as order_month,
    orders.order_approved_at,
    orders.order_delivered_carrier_date,
    orders.order_delivered_customer_date,
    orders.order_estimated_delivery_date,
    coalesce(totals.item_count, 0) as item_count,
    totals.items_price_total,
    totals.freight_total,
    totals.payment_total,
    orders._batch_id,
    orders._loaded_at
from orders
inner join customers
    on orders.customer_id = customers.customer_id
left join totals
    on orders.order_id = totals.order_id
