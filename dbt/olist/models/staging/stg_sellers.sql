-- Satu baris per seller. Kode pos tetap teks supaya angka 0 di depan tidak hilang.
with source as (

    select * from {{ source('raw', 'sellers') }}

),

typed as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _batch_id,
        _loaded_at
    from source

)

select * from typed
