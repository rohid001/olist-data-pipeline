-- Satu baris per customer_id. Kode pos tetap teks supaya angka 0 di depan tidak hilang.
with source as (

    select * from {{ source('raw', 'customers') }}

),

typed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _batch_id,
        _loaded_at
    from source

)

select * from typed