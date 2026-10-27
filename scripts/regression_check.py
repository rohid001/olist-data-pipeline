"""
regressio_check.py
Membandingkan isi setiap mart di warehouse dengan hasil query project DA, yaitu CSV yang
dulu di-export dari DBeaver ke data/processed/ di repo olist-sql-dashboard.
Sebuah mart dianggap cocok bila jumlah barisnya sama, kuncinya sama, dan semua nilainya
sama setelah angka dibulatkan 2 desimal. Hasilnya juga ditulis ke
docs/regression_report.md
Jalankan dari rrot repo:
    python -m scripts.regression_check --baseline-dir "<folder data/processed project DA>"
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

from ingestion.config import ROOT_DIR, get_engine

REPORT_FILE = ROOT_DIR / "docs" / "regression_report.md"

#----------------------------------------------------------------Tahap A
# Pasangan mart -> CSV project DA. "rename" mengubah nam kolom DA (bahasa Indonesia)
# menjadi nama kolom mart; kolom yang namanya sama tidak perlu ditulis
BASELINES = {
    "mart_customer_rfm": {
        "file": "q1_rfm.csv",
        "key": ["customer_unique_id"],
        "rename": {}
    },
    "mart_buyer_type": {
        "file": "q2_buyer_type.csv",
        "key": ["customer_type"],
        "rename": {"jumlah_pelanggan": "customer_count", "pct_revenue": "revenue_pct",
                   "pct_pelanggan": "customer_pct"},
    },
    "mart_revenue_monthly": {
        "file": "q3_monthly_revenue.csv",
        "key": ["month"],
        "rename": {"bulan": "month", "revenue_bulan_sebelumnya": 
            "previous_month_revenue"},
    },
    "mart_seasonality": {
        "file": "q5_seasonality.csv",
        "key": ["month_number"],
        "rename": {"bulan_ke": "month_number", "nama_bulan": "month_name",
                   "jumlah_order": "order_count"},
    },
    "mart_category_growth": {
        "file": "q4_category_growth.csv",
        "key": ["category"],
        "rename": {},
    },
    "mart_customer_churn": {
        "file": "q6b_churn_status.csv",
        "key": ["customer_unique_id"],
        "rename": {"hari_sejak_order_terakhir": "days_since_last_order"},
    },
    "mart_review_churn": {
        "file": "q7_review_churn.csv",
        "key": ["churn_status"],
        "rename": {"rata_rata_reviews_score": "avg_review_score",
                   "jumlah_pelanggan": "customer_count"},
    },
    "mart_category_per_state": {
        "file": "q8_category_per_state.csv",
        "key": ["customer_state", "category"],
        "rename": {"margin_kasar": "gross_margin"},
    },
    "mart_revenue_per_state": {
        "file": "q9_revenue_per_state.csv",
        "key": ["customer_state"],
        "rename": {"jumlah_pelanggan": "customer_count", "jumlah_order": "order_count"},
    },
}

# Hasil sql/07_overview_kpi.sql dan q6a project DA. Keduanya tidak pernah di-export ke CSV,
# jadi angkanya ditulis di sini. Nilainya sama dengan kartu KPI di dashboard DA.
OVERVIEW_BASELINE = {
    "total_orders": 96478,
    "total_customers": 93358,
    "total_revenue": 13221498.11,
    "avg_order_value": 137.04,
    "total_sellers": 2970,
    "avg_review_score": 4.09,
    "avg_repeat_order_gap_days": 78.8,
    "repeat_order_gap_count": 3120,
}

def read_baseline(baseline_dir: Path, mart: str) -> pd.DataFrame:
    """Baca CSV project DA untuk satu mart, lalu samakan nama kolomnya dengan mart."""
    spec = BASELINES[mart]
    # dtype=str: biarkan semua sebagai teks dulu; konversi dilakukan di normalize_column
    df = pd.read_csv(baseline_dir / spec["file"], dtype=str, encoding="utf-8-sig")
    return df.rename(columns=spec["rename"])

def read_mart(engine, mart: str) -> pd.DataFrame:
    """Ambil seluruh isi  satu tabel mart dari schema marts."""
    return pd.read_sql(f"select * from marts.{mart}", engine)

def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah."""
    parser = argparse.ArgumentParser(description="Compare dbt marts with the DA project results.")
    parser.add_argument("--baseline-dir", type=Path, required=True,
                        help="data/processed folder of the olist-sql-dashboard repo")
    return parser.parse_args()

#----------------------------------------------------------------Tahap B
def normalize_column(values: pd.Series) -> pd.Series:
    """Ubah satu kolom ke bentuk yang bisa dibandingkan, apa pun format asalnya.
    
    Angka -> dibulatkan 2 desimal, tanggal -> teks 'YYYY-MM-DD HH:MM:SS', teks -> tanpa
    spasi di awal dan akhir. CSV DBeavar dan hasil query bisa beda format
    (misalnya '2016-09-15 12:16:38.000' vs datetime), tapi isinya tetap sama.
    """
    text = values.astype("string").str.strip()
    filled = text.notna().sum()
    
    numbers = pd.to_numeric(text, errors="coerce")
    if numbers.notna().sum() == filled:
        return numbers.astype("Float64").round(2)
    
    # Hanya kolom yang semua isinya diawali pola tanggal YYYY-MM-DD yang diubah ke tanggal
    if text.str.match(r"\d{4}-\d{2}-\d{2}").sum() == filled:
        dates = pd.to_datetime(text, format="ISO8601")
        return dates.dt.strftime("%Y-%m-%d %H:%M-%S")
    
    return text

def compare(baseline: pd.DataFrame, mart: pd.DataFrame, key: list) -> dict:
    """Bandingkan dua tabel berdasarkan kunci; kembalikan ringkasan perbeedaannya."""
    missing_columns = [c for c in baseline.columns if c not in mart.columns]
    if missing_columns:
        raise KeyError(f"Columns not found in mart: {', '.join(missing_columns)}")
    
    expected = baseline.apply(normalize_column)
    actual = mart[list(baseline.columns)].apply(normalize_column)
    if key:
        expected = expected.set_index(key)
        actual = actual.set_index(key)
    expected = expected.sort_index()
    actual = actual.sort_index()
    
    common = expected.index.intersection(actual.index)
    left = expected.loc[common]
    right = actual.loc[common]
    # Sel dianggap sama bila nilainya sama atau keduanya kosong (NULL)
    different = ~((left == right) | (left.isna() & right.isna()))
    
    return {
        "baseline_rows": len(expected),
        "mart_rows": len(actual),
        "only_in_baseline": len(expected.index.difference(actual.index)),
        "only_in_mart": len(actual.index.difference(expected.index)),
        "different_cells": int(different.to_numpy().sum()),
    }
    
def is_match(result: dict) -> bool:
    """Cocok = jumlah baris sama, tidak ada kunci yang hilang/lebih, dan 0 sel berbeda."""
    return (result["baseline_rows"] == result["mart_rows"]
            and result["only_in_baseline"] == 0
            and result["only_in_mart"] == 0
            and result["different_cells"] == 0)

#----------------------------------------------------------------Tahap C
# Selisih sudah diselediki dan diterima. Mart di sini tetap dilaporkan berbeda.
# tetepi tidak membuat regression check gagal. Penjelasannya ikut ditulis ke laporan,
# jadi ditulis dalam Bahasa Inggris seperti isi laporan lainnya.
KNOWN_DIFFERENCES = {
    "mart_review_churn": ("The DA view order_reviews_dedup uses DISTINCT ON without a tiebreaker, so for the "
                          "789 deduplicated review_ids the kept order is arbitrary. stg_order_reviews alwas keeps "
                          "the smallest order_id, so a few reviews are linked to a different customer."),
}

def get_status(mart: str, result: dict)-> str:
    """MATCH, KNOWN DIFFERENCE (selisih yang sudah dijelaskan), atau DIFFERENT."""
    if is_match(result):
        return "MATCH"
    return "KNOWN DIFFERENCE" if mart in KNOWN_DIFFERENCES else "DIFFERENT"

def check_all(engine, baseline_dir: Path) -> dict:
    """Jalankan perbandingan untuk 9 mart ber-CSV dan mart_overview_kpi."""
    results = {}
    for mart, spec in BASELINES.items():
        try:
            baseline = read_baseline(baseline_dir, mart)
        except FileNotFoundError:
            results[mart] = {"source": spec["file"], "error": "baseline file not found"}
            continue
        current = read_mart(engine, mart)
        results[mart] = {"source": spec["file"], **compare(baseline, current, spec["key"])}
        
    overview = pd.DataFrame([OVERVIEW_BASELINE])
    results["mart_overview_kpi"] = {
        "source": "OVERVIEW_BASELINE",
        **compare(overview, read_mart(engine, "mart_overview_kpi"), []),
    }
    return results
    
def write_report(results: dict) -> None:
    """Tulis hasil perbandingan ke docs/regression_report.md"""
    lines = [
        "# Regression check: dbt marts vs porject DA",
        "",
        "Generated by 'python -m scripts.regression_check'. Numbers are compared after "
        "rounding to 2 decimels; text is compared after trimming spaces.",
        "",
        "| Mart | DA baseline | DA rows | Mart rows | Only in DA | Only in mart "
        "| Different cells | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for mart, r in results.items():
        if "error" in r:
            lines.append(f"| '{mart}' | '{r['source']}' | - | - | - | - | - | {r['error']}|")
            continue
        status = get_status(mart, r)
        lines.append(
            f"| '{mart}' | '{r['source']}' | {r['baseline_rows']:,} | {r['mart_rows']:,} "
            f"| {r['only_in_baseline']:,} | {r['only_in_mart']:,} | {r['different_cells']} "
            f"| {status} |"
        )
    explained = [m for m, r in results.items() if "error" not in r and get_status(m, r) == "KNOWN DIFFERENCE"]
    if explained:
        lines += ["", "## Known difference", ""]
        lines += [f"- '{m}': {KNOWN_DIFFERENCES}" for m in explained]
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")

if __name__ == "__main__":
    
    args = parse_args()
    if not args.baseline_dir.is_dir():
        print(f"Baseline folder not found: {args.baseline_dir}")
        sys.exit(1)
    results = check_all(get_engine(), args.baseline_dir)
    
    for mart, r in results.items():
        if "error" in r:
            print(f"{mart:<26} {r['error']} ({r['source']})")
            continue
        status = get_status(mart, r)
        print(f"{mart:<26} {r['baseline_rows']:>7,} vs {r['mart_rows']:>7,} rows "
              f"{r['different_cells']:>6,} different cells  {status}")
    
    write_report(results)
    statuses = [get_status(m, r) if "error" not in r else "ERROR" for m, r in results.items()]
    print(f"{statuses.count('MATCH')} of {len(results)} marts match the DA baseline; "
          f"known differences: {statuses.count('KNOWN DIFFERENCE')}")
    print(f"Report written to {REPORT_FILE.relative_to(ROOT_DIR).as_posix()}")
    # Gagal (exit 1) bila ada mart yang berbeda tanpa penjelasan atau CSV-nya tidak ada
    failed = statuses.count("DIFFERENT") + statuses.count("ERROR")
    sys.exit(1 if failed else 0)
        