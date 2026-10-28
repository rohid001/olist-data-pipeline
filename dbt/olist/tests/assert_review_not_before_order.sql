-- Temuan profiling Fase 1: ada review yang tanggal pembuatnya lebih awal dari tanggal
-- order-nya. Data asli Olist memang begitu, jadi test ini hanya memberi peringatan (warn)
-- dan tidak menghentikan pipeline. Hasil saat ini: 44 baris.
{{ config(severity='warn') }}

select
    reviews.review_id,
    reviews.order_id,
    reviews.review_creation_date,
    orders.order_purchase_timestamp
from {{ ref('stg_order_reviews') }} as reviews
inner join {{ ref('stg_orders') }} as orders on reviews.order_id = orders.order_id
where reviews.review_creation_date::date < orders.order_purchase_timestamp::date
