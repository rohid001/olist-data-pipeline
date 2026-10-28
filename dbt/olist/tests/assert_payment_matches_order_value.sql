-- Total pembayaran sebuah order seharusnya sama dengan total harga + ongkir item-nya.
-- Di data asli Olist sebagain kecil order selisih (misalnya karena bunga cicialn atau
-- voucher), jadi selisih sampai 1.000 order hanya warn; lebih dari itu error.
-- Hasil saat ini: 303 baris
{{ config(severity='error', warn_if='>0', error_if='>1000') }}

select
    order_id,
    payment_total,
    items_price_total + freight_total as order_value,
    payment_total - (items_price_total + freight_total) as difference
from {{ ref('fct_orders') }}
where
    payment_total is not null
    and items_price_total is not null
    and abs(payment_total - (items_price_total + freight_total)) > 0.01
