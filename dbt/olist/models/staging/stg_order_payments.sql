-- Satu baris pembayaran (order_id + payment_sequential).
with source as (

    select * from {{ source('raw', 'order_payments') }}

),

typed as (

    select
        order_id, 
        payment_sequential::int as payment_sequential,
        payment_type,
        payment_installments::int as payment_installments,
        payment_value::numeric(10, 2) as payment_value,
        _batch_id,
        _loaded_at
    from source


)

select * from typed