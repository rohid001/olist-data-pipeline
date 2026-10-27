-- Satu baris per produk, dengan nama kategori dalam Bahasa Inggris.
-- 610 produk tanpa kategori diberi 'uncategorized', sama seperti project DA
with products as (

    select * from {{ ref('stg_products') }}

),

translation as (

    select * from {{ ref('stg_product_category_name_translation') }}

)

select
    products.product_id,
    products.product_category_name,
    coalesce(translation.product_category_name_english, 'uncategorized') as product_category,
    products.product_name_length,
    products.product_description_length,
    products.product_photos_qty,
    products.product_weight_g,
    products.product_length_cm,
    products.product_height_cm,
    products.product_width_cm
from products
left join translation
    on products.product_category_name = translation.product_category_name