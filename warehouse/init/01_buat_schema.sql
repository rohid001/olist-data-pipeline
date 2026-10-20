-- Dijalankan otomatis oleh image postgres SEKALI saja,
-- yaitu saat volume data masih kosong (container pertama kali dibuat).

-- 4 layer warehouse, sesuai arsitektur di Roadmap
CREATE SCHEMA IF NOT EXISTS raw;           -- salinan data sumber + kolom metadata
CREATE SCHEMA IF NOT EXISTS staging;       -- hasil dbt: rename, cast tipe, dedup
CREATE SCHEMA IF NOT EXISTS intermediate;  -- hasil dbt: logika yang dipakai berulang
CREATE SCHEMA IF NOT EXISTS marts;         -- hasil dbt: tabel siap pakai untuk Power BI