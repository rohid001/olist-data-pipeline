-- Satu baris per item di dalam order (order_id + order_item_id).
-- Atribut order (status, tanggal, state) diambil dar fct_orders lewat order_id.
with order_items as (

    select * from {{ ref('stg_order_items') }}

)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    _batch_id
from order_items
