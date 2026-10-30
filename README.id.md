# Olist Data Pipeline (ringkasan Bahasa Indonesia)

*English: [README.md](README.md) (versi lengkap)*

Pipeline ELT batch bulanan untuk dataset [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(99.441 order, September 2016 – Oktober 2018). Project ini melanjutkan project Data Analyst saya,
[olist-sql-dashboard](https://github.com/rohid001/olist-sql-dashboard): query SQL yang dulu dijalankan
satu per satu kini menjadi pipeline otomatis yang teruji, yaitu ingestion Python → PostgreSQL → dbt →
Airflow → Power BI, dan seluruhnya dinyalakan dengan `docker compose up`.

## Hasil utama

| Metrik | Sebelum (project DA) | Sesudah (pipeline ini) |
| --- | --- | --- |
| Langkah untuk memperbarui data dashboard | 15 eksekusi manual + refresh Power BI | `docker compose up -d` dan unpause 2 DAG, lalu terjadwal per bulan |
| SQL di dalam Power BI | 351 baris di 12 query Native SQL | 0 baris; 12 query membaca tabel `marts` |
| Kecocokan dengan hasil DA | dicek manual per query | 10 mart dibandingkan otomatis: 9 identik, 1 selisih yang dijelaskan |
| Batch bulanan | tidak ada | 26 dari 26 run Airflow sukses, termasuk bulan kosong (November 2016) |
| Durasi backfill penuh | tidak terukur | 7 menit 59,9 detik di PC (sekitar 18,5 detik per bulan) |
| Idempotensi | load ulang = data dobel | menjalankan ulang satu bulan: 0 perubahan jumlah baris |
| Test otomatis | 0 | 71 data test dbt + 9 unit test pytest; 0 error, 2 warning yang disengaja; sqlfluff 0 pelanggaran |
| CI | tidak ada | GitHub Actions di setiap push, sekitar 1 menit 16 detik |
| Akses BI | akun pemilik database | user `bi_reader` yang hanya bisa membaca `marts` |

## Isi project

- **Ingestion Python**: partisi bulanan dan load ke schema `raw` yang idempotent (delete-insert dan upsert).
- **dbt**: 27 model berlapis (staging, intermediate, marts), model incremental, 71 test, dan exposure dashboard.
- **Airflow 3**: DAG bulanan dengan backfill 26 bulan, retry, dan cek kesegaran data.
- **CI GitHub Actions**: pytest, lint SQL, dan `dbt build` di atas sampel 445 order.
- **Power BI**: dashboard 5 halaman dari project DA, kini membaca schema `marts` lewat user read-only.

Cara menjalankan, arsitektur, dan keterbatasan project ada di [README.md](README.md).

Dibuat oleh [Rohid Aji Sunan](https://github.com/rohid001).