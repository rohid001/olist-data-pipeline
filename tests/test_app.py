"""
test_app.py
Smoke test dashboard Streamlit dengan AppTest: aplikasi dijalankan tanpa browser, lalu 
dicek bahwa tidak ada error dan KPI utamanya sama dengan hasil pipeline.
Dilewati otomatis bila streamlit belum terpasang (pip install -r app/requirements.txt).
Jalankan dari root repo: python -m pytest
"""
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest # noqa: E402

# Path relatif terhadap file test ini
APP_FILE = "../app/streamlit_app.py"

@pytest.fixture(scope="module")
def app():
    """Jalankan aplikasi sekali untuk semua test di file ini."""
    return AppTest.from_file(APP_FILE, default_timeout=60).run()

def test_app_runs_without_errors(app):
    assert not app.exception
    
def test_five_pages(app):
    labels = [tab.label for tab in app.tabs]
    assert labels == ["Overview", "Customer RFM", "Revenue Trend", "Churn Risk", "Category & Region"]
    
def test_overview_kpis_match_pipeline(app):
    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics["Total revenue"] == "R$ 13.22M"
    assert metrics["Orders"] == "96,478"
    assert metrics["Top category nationwide"] == "health_beauty (14/27 states)"
    
def test_category_slider_filters_table(app):
    app.slider[0].set_value(50_000).run()
    growth_table = app.dataframe[1].value
    assert (growth_table["revenue_2017"] >= 50_000).all()