-- Satu baris per order_id: jumlah item, total harga, total ongkir, dan total pembayaran.
-- 775 order tidak punya item dan 1 order tidak punya pembayaran, jadi kedua ringkasan
-- digabung dengan full outer join supaya tidak ada order yang hilang.

with items as (

    select
        order_id,
        count(*) as item_count,
        sum(price) as items_price_total,
        sum(freight_value) as freight_total
    from {{ ref('stg_order_items') }}
    group by order_id

),

payments as (

    select
        order_id,
        sum(payment_value) as payment_total
    from {{ ref('stg_order_payments') }}
    group by order_id

)

select
    coalesce(items.order_id, payments.order_id) as order_id,
    coalesce(items.item_count, 0) as item_count,
    items.items_price_total,
    items.freight_total,
    payments.payment_total
from items
full outer join payments
    on items.order_id = payments.order_id