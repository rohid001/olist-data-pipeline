-- Data referensi kode pos. Baris kembar dibiarkan di sini; belum dipakai mart mana pun.
with source as (

    select * from {{ source('raw', 'geolocation') }}

),

typed as (

    select
        geolocation_zip_code_prefix,
        geolocation_lat::double precision as geolocation_lat,
        geolocation_lng::double precision as geolocation_lng,
        geolocation_city,
        geolocation_state,
        _loaded_at
    from source

)

select * from typed