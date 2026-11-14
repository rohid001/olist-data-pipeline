# Olist Data Pipeline (ringkasan Bahasa Indonesia)

*English: [README.md](README.md) (versi lengkap)*

**Dashboard online:** <https://olist-data-pipeline.streamlit.app> · **dbt docs online:** <https://rohid001.github.io/olist-data-pipeline/dbt/>

Pipeline ELT batch bulanan untuk dataset [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(99.441 order dari September 2016 – Oktober 2018; order berstatus delivered yang dipakai analisis berakhir Agustus 2018). Project ini melanjutkan project Data Analyst saya,
[olist-sql-dashboard](https://github.com/rohid001/olist-sql-dashboard): query SQL yang dulu dijalankan
satu per satu kini menjadi pipeline otomatis yang teruji, yaitu ingestion Python → PostgreSQL → dbt →
Airflow → Power BI dan Streamlit, dan seluruhnya dinyalakan dengan `docker compose up`. Jalur kedua
memutar ulang riwayat order sebagai event lewat Apache Kafka, untuk menunjukkan cara warehouse yang sama
menangani event duplikat dan event yang datang terlambat.

## Hasil utama

| Metrik | Sebelum (project DA) | Sesudah (pipeline ini) |
| --- | --- | --- |
| Langkah untuk memperbarui data dashboard | 15 eksekusi manual + refresh Power BI | `docker compose up -d` dan unpause 2 DAG, lalu terjadwal per bulan |
| SQL di dalam Power BI | 351 baris di 12 query Native SQL | 0 baris; 12 query membaca tabel `marts` |
| Kecocokan dengan hasil DA | dicek manual per query | 10 mart dibandingkan otomatis: 9 identik, 1 selisih yang dijelaskan |
| Batch bulanan | tidak ada | 26 dari 26 run Airflow sukses, termasuk bulan kosong (November 2016) |
| Durasi backfill penuh | tidak terukur | 7 menit 59,9 detik di PC (sekitar 18,5 detik per bulan) |
| Idempotensi | load ulang = data dobel | menjalankan ulang satu bulan: 0 perubahan jumlah baris |
| Test otomatis | 0 | 82 data test dbt + 20 test pytest (16 unit test, 4 smoke test dashboard); 0 error, 2 warning yang disengaja; sqlfluff 0 pelanggaran |
| Event streaming | tidak ada | 392.856 event order diputar ulang lewat Kafka, dengan 7.846 duplikat dan 19.492 event terlambat yang disengaja: tabel hasil stream cocok dengan data batch (0 selisih), consumer yang mati mendadak tidak menghilangkan atau menggandakan satu event pun, dan memutar ulang seluruh riwayat untuk kedua kalinya (853.484 pesan di topic) tetap menyisakan tepat 392.856 baris |
| CI/CD | tidak ada | GitHub Actions di setiap push dan pull request: pre-commit, test, lint, `dbt build`, dan golden test 11 mart dashboard; dbt docs terbit otomatis ke GitHub Pages |
| Akses BI | akun pemilik database | user `bi_reader` yang hanya bisa membaca `marts` |
| Akses publik ke hasil | file Power BI yang hanya bisa dibuka di Windows | dashboard Streamlit publik dan situs dbt docs, tanpa instalasi |

## Isi project

- **Ingestion Python**: partisi bulanan dan load ke schema `raw` yang idempotent (delete-insert dan upsert).
- **dbt**: 30 model berlapis (staging, intermediate, marts), model incremental, 82 test, dan 2 exposure (dashboard Power BI dan Streamlit).
- **Kafka**: producer yang memutar ulang 4 tahap setiap order sebagai event (sebagian sengaja terlambat atau dikirim dua kali), consumer at-least-once yang membuang duplikat lewat `event_id`, dan model dbt incremental berdasarkan waktu tiba.
- **Airflow 3**: DAG bulanan dengan backfill 26 bulan, retry, dan cek kesegaran data.
- **CI/CD GitHub Actions**: pre-commit, pytest (termasuk smoke test dashboard Streamlit), lint SQL, event sampel lewat Kafka, `dbt build` di atas sampel 445 order, golden test isi mart, ringkasan dampak di pull request, dan penerbitan dbt docs otomatis.
- **Power BI**: dashboard 5 halaman dari project DA, kini membaca schema `marts` lewat user read-only.
- **Streamlit**: 5 halaman yang sama sebagai dashboard web publik di Streamlit Community Cloud, membaca snapshot `marts` dalam format Parquet hasil `scripts/export_marts.py`.

Cara menjalankan, arsitektur, dan keterbatasan project ada di [README.md](README.md).

Dibuat oleh [Rohid Aji Sunan](https://github.com/rohid001).
