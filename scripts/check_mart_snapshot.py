"""
check_mart_snapshot.py
Golden test untuk CI: membandingkan isi 11 mart dashboard (hasil dbt build dia atas sampel
445 order) dengan snapshot yang disimpan di tests/fixtures/expected_marts/.
Bila logika model berubah tanpa disengaja, angkanya berbeda dan CI gagal. Bila
perbahannya memang disengaja, buat ulang snapshot dengan --update lalu comit bersama perubahan model.
Jalankan dari root repo, terhadap database berisi sampel CI:
    python -m scripts.check_mart_snapshot           # bandingkan
    python -m scripts.check_mart_snapshot --update  # tulis ulang snapshot
"""
import argparse
import sys

import pandas as pd

from ingestion.config import ROOT_DIR, get_engine
from scripts.export_marts import MARTS, read_mart
from scripts.regression_check import compare, is_match

SNAPSHOT_DIR = ROOT_DIR / "tests" / "fixtures" / "expected_marts"

#-----------------------------------------------------------------Tahap A
def write_snapshot(engine) -> None:
    """Simpan isi setiap mart sebagai CSV. CSV dipilih (bukan Parquet) supaya perubahan
    angka terlihat langsung di diff pull request."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    for mart in MARTS:
        df = read_mart(engine, mart)
        df.to_csv(SNAPSHOT_DIR / f"{mart}.csv", index=False, lineterminator="\n")
        print(f"{mart:<26} {len(df):>5,} rows saved")

#-----------------------------------------------------------------Tahap B
def check_snapshot(engine) -> list:
    """Bandingkan setiap mart dengan snapshot-nya; kembalikan daftar mart yang berbeda."""
    failed = []
    for mart in MARTS:
        expected = pd.read_csv(SNAPSHOT_DIR / f"{mart}.csv", dtype=str)
        try:
            # Kunci kosong: kedua tabel sudah diurutkan dengan cara yang sama oleh read_mart,
            # jadi baris dibandingkan sesuai aturannya
            result = compare(expected, read_mart(engine, mart), [])
        except KeyError as error:
            # Ada kolom snapshot yang hilang dari mart
            print(f"{mart:<26} {error}")
            failed.append[mart]
            continue
        status = "OK" if is_match(result) else "DIFFERENT"
        print(f"{mart:<26} {result['baseline_rows']:>5,} vs {result['mart_rows']:>5,} rows "
              f"{result['different_cells']:>4,} different cells {status}")
        if status != "OK":
            failed.append(mart)
    return failed

def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah."""
    parser = argparse.ArgumentParser(description="Comapare dashboard marts with the CI snapshot.")
    parser.add_argument("--update", action="store_true", help="rewrite the snapshot from the database")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    engine = get_engine()
    if args.update:
        write_snapshot(engine)
        print(f"Snapshot written to {SNAPSHOT_DIR.relative_to(ROOT_DIR).as_posix()}")
        sys.exit(0)

    failed = check_snapshot(engine)
    if failed:
        print(f"{len(failed)} of {len(MARTS)} mart differ from the snapshot: {', '.join(failed)}")
        print("If the change is intended, run with --update and commit the new snapshot.")
        sys.exit(1)
    print(f"All {len(MARTS)} marts match the snapshot")
