-- Satu baris per produk. Salah eja asli  Olist ("lenght") dibetulkan di sini.
with source as (

    select * from {{ source('raw', 'products') }}

),

typed as (

    select
        product_id,
        product_category_name,
        product_name_lenght::int as product_name_length,
        product_description_lenght::int as product_description_length,
        product_photos_qty::int as product_photos_qty,
        product_weight_g::int as product_weight_g,
        product_length_cm::int as product_length_cm,
        product_height_cm::int as product_height_cm,
        product_width_cm::int as product_width_cm,
        _batch_id,
        _loaded_at
        from source


)

select * from typed