"""
test_config.py
Unit test untuk get_settings: pengaturan harus dibaca dari environtment variable
saja (seperti di CI, tanpa file .env), dan error yang jelas bila ada yang kosong.
Jalankan dari root repo: python -m pytest
"""
import pytest

from ingestion import config

SETTINGS = {
    "WAREHOUSE_HOST": "localhost",
    "WAREHOUSE_PORT": "5432",
    "WAREHOUSE_DB": "olist_dw",
    "WAREHOUSE_USER": "olist",
    "WAREHOUSE_PASSWORD": "test_password",
}

@pytest.fixture
def no_env_file(monkeypatch, tmp_path):
    """Arahkan ENV_FILE ke file yang tidak ada, supaya .env asli di laptop tidak ikut
    terbaca."""
    monkeypatch.setattr(config, "ENV_FILE", tmp_path/ ".env")
    
def test_settings_from_environmen_only(no_env_file, monkeypatch):
    for name, value in SETTINGS.items():
        monkeypatch.setenv(name, value)
    settings = config.get_settings()
    assert settings["port"] == 5432
    assert settings["database"] == "olist_dw"

def test_missing_setting_is_reported(no_env_file, monkeypatch):
    for name, value in SETTINGS.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("WAREHOUSE_PASSWORD")
    with pytest.raises(ValueError, match="WAREHOUSE_PASSWORD"):
        config.get_settings()
        
