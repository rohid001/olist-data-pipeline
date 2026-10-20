"""
kontrak_data.py
Kontrak data: kesepakatan tertulis tentang setiap tabel sumber Olist.
Untuk tiap tabel dicatat: file asalnya, kolom yang wajib ada, key (grain),
cara membaginya per bulan (partisi), dan cara memuatnya ke schema raw.

Dipakai oleh profil_data.py (Fase 1), partisi.py (Fase), dan muat_raw.py (Fase 3).
Kalau sumber data berubah, cukup ubah file ini.
"""

# Rentang batch bulanan: 26 bulan, dari order pertama sampai order terakhir
FIRST_MONTH = "2016-09"
LAST_MONTH = "2018-10"

# partition_type (cara membagi table per bulan):
#   "time_column"   -> bulan diambil dari kolom tanggal milik tabel itu sendiri
#   "parent_table"  -> bulan diwarisi dari tabel lain lewat kolom penghubung
#   "static"        -> tidak dipartisi, dimuat sekali
#   "dbt_seed"      -> tidak dimuat Python, dikelola dbt sebagai seed (Fase 4)
#
# Strategi muat ke schema raw (dipakai di Fase 3):
#   "delete_insert" -> hapus semua baris batch itu, lalu sisipkan ulang
#   "upsert"        -> sisipkan baris baru, perbarui baris yang keynya sudah ada
#   "full_replace"  -> kosongkan tabel, lalu isi ulang seluruhnya

CONTRACT = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "columns": [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ],
        "key": ["order_id"],
        "partition_type": "time_column",
        "time_column": "order_purchase_timestamp",
        "load_strategy": "delete_insert",
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "columns": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
        "key": ["order_id", "order_item_id"],
        "partition_type": "parent_table",
        "parent_table": "orders",
        "join_column": "order_id",
        "load_strategy": "delete_insert",
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "columns": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
        "key": ["order_id", "payment_sequential"],
        "partition_type": "parent_table",
        "parent_table": "orders",
        "join_column": "order_id",
        "load_strategy": "delete_insert",
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "columns": [
            "review_id", "order_id", "review_score", "review_comment_title",
            "review_comment_message", "review_creation_date", "review_answer_timestamp",
        ],
        # review_id saja TIDAK unik (814 duplikat); pasangan review_id + order_id unik
        "key": ["review_id", "order_id"],
        "partition_type": "time_column",
        "time_column": "review_creation_date",
        "load_strategy": "delete_insert",
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "columns": [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
        "key": ["customer_id"],
        # customer_id berpasangan 1:1 dengan order, jadi ikut batch order-nya
        "partition_type": "parent_table",
        "parent_table": "orders",
        "join_column": "customer_id",
        "load_strategy": "delete_insert",
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "columns": [
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ],
        "key": ["product_id"],
        # satu produk bisa terjual di banyak bulan, jadi dimuat dengan upsert
        "partition_type": "parent_table",
        "parent_table": "order_items",
        "join_column": "product_id",
        "load_strategy": "upsert",
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        "key": ["seller_id"],
        "partition_type": "parent_table",
        "parent_table": "order_items",
        "join_column": "seller_id",
        "load_strategy": "upsert",
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "columns": [
            "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
            "geolocation_city", "geolocation_state",
        ],
        "key": None,  # tidak punya kunci; banyak baris kembar
        "partition_type": "static",
        "load_strategy": "full_replace",
    },
    "product_category_name_translation": {
        "file": "product_category_name_translation.csv",
        "columns": ["product_category_name", "product_category_name_english"],
        "key": ["product_category_name"],
        "partition_type": "dbt_seed",
        "load_strategy": "dbt_seed",
    },
}

if __name__ == "__main__":
    # Uji: kontrak terbaca dan setiap tabel punya isian yang lengkap
    for table, spec in CONTRACT.items():
        key = ", ".join(spec["key"]) if spec["key"] else "-"
        print(f"{table:<35} {spec['partition_type']:<13} {spec['load_strategy']:<14} key:{key}")
    print(f"{len(CONTRACT)} tables, batches {FIRST_MONTH} to {LAST_MONTH}")