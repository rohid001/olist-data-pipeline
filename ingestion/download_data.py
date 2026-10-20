"""
download_data.py
Mengunduh dataset Olist dari Kaggle lewat kagglehub, lalu menyalin 9 CSV
yang tercantum di data_contract.py ke data/source/.
jalankan dari root repo:
python -m ingestion.download_data
python -m ingestion.download_data --from-dri <folder> (salin dari folder lokal, tanpa Kaggle)
"""

import argparse
import shutil

from pathlib import Path
from ingestion.config import ROOT_DIR
from ingestion.data_contract import CONTRACT

DATASET_HANDLE = "olistbr/brazilian-ecommerce"
SOURCE_DIR = ROOT_DIR / "data" / "source"

def download_datase() -> Path:
    """Unduh dataset (atau pakai cache kagglehub bila sudah pernah diunduh), kembalikan foldernya"""
    #import di dalam fungsi: mode --from-dir tetap jalan walau Kaggle tidak bisa diakses
    import kagglehub
    
    return Path(kagglehub.dataset_download(DATASET_HANDLE))

def copy_source_files(download_dir: Path) -> list:
    """Salin semua file yang tercantum di kontrak dari folder unduhan ke data/source/."""
    missing = [spec["file"] for spec in CONTRACT.values() if not (download_dir / spec["file"]).exists()]
    if missing:
        raise FileNotFoundError(f"Not found in {download_dir}: {', '.join(missing)}")
    
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    copied = []
    for spec in CONTRACT.values():
        target = SOURCE_DIR / spec["file"]
        shutil.copy2(download_dir / spec["file"], target)
        copied.append(target)
    return copied

def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah."""
    parser = argparse.ArgumentParser(description="Download Olist CSVs into data/source.")
    parser.add_argument("--from-dir", type=Path, help="copy from this local folder instead of Kaggle")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    # Uji Tahap A: dapatkan folder berisi CSV (dari Kaggle atau folder lokal)
    download_dir = args.from_dir if args.from_dir else download_datase()
    print("Source folder:", download_dir)
    print(f"{len(list(download_dir.glob('*.csv')))} CSV files found")
    
    # Uji Tahap B: salin 9 file sesuai kontrak ke data/source
    copied = copy_source_files(download_dir)
    for path in copied:
        print(f"{path.name:<42} {path.stat().st_size / 1_000_000:>6.1f} MB")
    print(f"{len(copied)} files copied to {SOURCE_DIR.relative_to(ROOT_DIR).as_posix()}")