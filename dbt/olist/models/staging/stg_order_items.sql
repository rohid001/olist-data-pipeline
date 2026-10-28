-- Satu baris per item di dalam order (order_id + order_item_id).
with source as (

    select * from {{ source('raw', 'order_items') }}

),

typed as (

    select
        order_id,
        order_item_id::int as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_date,
        price::numeric(10, 2) as price,
        freight_value::numeric(10, 2) as freight_value,
        _batch_id,
        _loaded_at
    from source

)

select * from typed
