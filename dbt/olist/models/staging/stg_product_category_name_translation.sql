-- Terjemahan kategori produk dari seed: 71 baris resmi Kaggle + 2 terjemahan manual.
with source as (

    select * from {{ ref('product_category_name_translation') }}

)

select
    product_category_name,
    product_category_name_english
from source
