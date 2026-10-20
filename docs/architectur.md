# Arsitektur Olist Data Pipeline

Pipeline ELT batch bulanan: data Olist dipecah per bulan, dimuat apa adanya ke schema `raw`, lalu dibersihkan dan dimodelkan oleh dbt sampai schema `marts` yang dibaca Power BI. Semua angka di dokumen ini berasal dari `docs/data_profile.md` (dibuat oleh `python -m ingestion.profile_data`).

## 1. Layer dan schema

| Schema | Isi | Dibuat oleh | Materialisasi |
| --- | --- | --- | --- |
| `raw` | Salinan CSV, semua kolom bisnis bertipe `TEXT`, plus 3 kolom metadata | `ingestion/load_raw.py` (Fase 3) | tabel |
| `staging` | 1 model per tabel sumber: ganti nama, ubah tipe data, dedup review | dbt (Fase 4) | view |
| `intermediate` | Logika yang dipakai lebih dari satu mart | dbt (Fase 5) | view |
| `marts` | Tabel dimensi, fakta, dan mart analitik untuk Power BI | dbt (Fase 5) | tabel; `fct_orders` incremental |

## 2. Konvensi penamaan

- Tabel di `raw` memakai nama dari kontrak: `orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `products`, `sellers`, `geolocation`.
- Model dbt: `stg_<tabel>`, `int_<topik>`, `dim_<entitas>`, `fct_<proses>`, `mart_<topik>`.
- Kolom metadata diawali garis bawah: `_batch_id` (`YYYY-MM` atau `static`), `_source_file`, `_loaded_at`.
- Nama kolom bisnis tetap seperti di sumber (`order_id`, `customer_state`, ...) supaya cocok dengan dokumentasi Olist dan project DA.

## 3. Kontrak tabel sumber

Versi kode yang dipakai pipeline: `ingestion/data_contract.py`. Nilai di kolom Partisi dan Strategi muat sama dengan nilai `partition_type` dan `load_strategy` di kode.

| Tabel | Grain (kunci unik) | Partisi bulanan | Strategi muat | Alasan |
| --- | --- | --- | --- | --- |
| orders | `order_id` | `time_column`: `order_purchase_timestamp` | `delete_insert` | Setiap order milik tepat satu bulan |
| order_items | `order_id` + `order_item_id` | `parent_table`: orders | `delete_insert` | Item tidak punya tanggal transaksi sendiri |
| order_payments | `order_id` + `payment_sequential` | `parent_table`: orders | `delete_insert` | Pembayaran tidak punya tanggal sendiri |
| order_reviews | `review_id` + `order_id` | `time_column`: `review_creation_date` | `delete_insert` | Review baru ada saat dibuat; `review_id` saja punya 814 duplikat |
| customers | `customer_id` | `parent_table`: orders | `delete_insert` | `customer_id` berpasangan 1:1 dengan order (0 dipakai ulang) |
| products | `product_id` | `parent_table`: order_items | `upsert` | 11.869 dari 32.951 produk terjual di lebih dari 1 bulan |
| sellers | `seller_id` | `parent_table`: order_items | `upsert` | 2.344 dari 3.095 seller berjualan di lebih dari 1 bulan |
| geolocation | tidak ada | `static` | `full_replace` | Data referensi; 261.831 baris kembar penuh |
| product_category_name_translation | `product_category_name` | `dbt_seed` | `dbt_seed` | 71 baris resmi + 2 terjemahan manual dari project DA, tersimpan di Git |

## 4. Batch bulanan

- 26 batch, `2016-09` sampai `2018-10`. Satu run Airflow memproses satu batch.
- Bulan tanpa data tetap menjadi run yang **sukses** dengan 0 baris: `2016-11` tanpa order (tetapi ada 101 review); `2016-09`, `2018-09`, `2018-10` tanpa review.

## 5. Aturan idempotensi

1. Satu batch dimuat dalam **satu transaksi**: semua tabel di batch itu berhasil, atau tidak ada yang berubah.
2. `delete_insert`: `DELETE ... WHERE _batch_id = '<batch>'`, lalu `INSERT` baris batch itu.
3. `upsert`: `INSERT ... ON CONFLICT (<kunci>) DO UPDATE`. Karena itu tabel `raw.products` dan `raw.sellers` wajib punya `PRIMARY KEY`.
4. `full_replace`: `TRUNCATE`, lalu `INSERT`.
5. Bukti yang harus ditunjukkan: menjalankan batch yang sama dua kali menghasilkan jumlah baris yang sama persis di semua tabel (Fase 3 dan Fase 7).

## 6. Tipe data di `raw`

Semua kolom bisnis disimpan sebagai `TEXT`. Load tidak pernah gagal karena format tanggal atau angka; konversi tipe dikerjakan sekali di `staging` dan diuji dbt. Ini prinsip yang sama dengan project DA: muat dan audit dulu, batasi belakangan.

## 7. Data terlambat dan anomali

- 40.967 dari 99.224 review (41,3%) dibuat di bulan sesudah order-nya. Mart yang memakai review dihitung ulang penuh (materialisasi `table`), bukan incremental per bulan order.
- 33 review dibuat di bulan **sebelum** order-nya. Di Fase 6 ini menjadi test dengan severity `warn`: dicatat, tetapi tidak menghentikan pipeline.
- Dataset hanya berisi status akhir tiap order, jadi batch bulanan adalah *replay* histori, bukan perubahan data sungguhan.

## 8. Grain model marts (rencana Fase 5)

| Model | Grain |
| --- | --- |
| `fct_orders` | 1 baris per `order_id` |
| `fct_order_items` | 1 baris per `order_id` + `order_item_id` |
| `dim_customers` | 1 baris per `customer_unique_id` |
| `dim_products` | 1 baris per `product_id` |
| `dim_sellers` | 1 baris per `seller_id` |
| `mart_customer_rfm` | 1 baris per `customer_unique_id` |
| `mart_revenue_monthly` | 1 baris per bulan |
| `mart_category_per_state`, `mart_revenue_per_state` | 1 baris per `customer_state` |