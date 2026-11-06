"""
streamlit_app.py
Dashboard publik Olist: 5 halaman yang sama dengan dashboard Power BI, dibaca dari file
Parquet hasil scripts/export_marts.py (snapshot schema marts). Teks di layar berbahasa
Inggris karena ditujukan untuk perekrut; komentar tetap Bahasa Indonesa
Jalankan dari root repo:
    streamlit run app/streamlit_app.py
"""
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Path realitf terhadap file ini, bukan terhadap folder tempat perintah dijalankan
# Di Streamlit Community Cloud working directory selalu root repo.
DATA_DIR = Path(__file__).parent / "data"
REPO_URL = "https://github.com/rohid001/olist-data-pipeline"
DOCS_URL = "https://rohid001.github.io/olist-data-pipeline/dbt/"

# Nama segmen di mart masih mengikuti project DA; diterjemahkan hanya untuk tampilan
SEGMENT_LABELS = {"At Risk (dulu loyal)": "At Risk (formerly loyal)"}

#---------------------------------------------------------------------------------Tahap A
@st.cache_data
def load(mart:str) -> pd.DataFrame:
    """Baca satu mart dari app/data/<mart>.parquet. Hasilnya di-cache selama app berjalan."""
    return pd.read_parquet(DATA_DIR / f"{mart}.parquet")

def money(value: float) -> str:
    """13221498.11 -> 'R$ 13.22M', 5067633.16 -> 'R$ 5.07M', 137.04 -> 'R$ 137.04'."""
    return f"R$ {value / 1e6:,.2f}M" if value >= 1e6 else f"R$ {value:,.2f}"

def show_overview() -> None:
    """Halaman 1: KPI utama, tren revenue bulanan, kategori dan state unggulan."""
    kpi = load("mart_overview_kpi").iloc[0]
    cols = st.columns(5)
    cols[0].metric("Total revenue", money(kpi["total_revenue"]))
    cols[1].metric("Orders", f"{int(kpi['total_orders']):,}")
    cols[2].metric("Customers", f"{int(kpi['total_customers']):,}")
    cols[3].metric("Avg order value", money(kpi["avg_order_value"]))
    cols[4].metric("Avg review score", f"{kpi['avg_review_score']:.2f} / 5")
    
    monthly = load("mart_revenue_monthly").assign(month=lambda df: pd.to_datetime(df["month"]))
    chart = alt.Chart(monthly).mark_line(point=True).encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("revenue:Q", title="Revenue (R$)"),
        tooltip=[alt.Tooltip("month:T", format="%b %Y"), alt.Tooltip("revenue:Q", format=",.2f")],
    )
    st.subheader("Monthly revenue")
    st.altair_chart(chart, width="stretch")
    
    wins = load("mart_category_win_count").sort_values("state_count", ascending=False).iloc[0]
    state_count = load("mart_category_per_state")["customer_state"].nunique()
    top_state = load("mart_revenue_per_state").sort_values("total_revenue", ascending=False).iloc[0]
    cols = st.columns(2)
    cols[0].metric("Top category nationwide",
                   f"{wins['category']} ({wins['state_count']}/{state_count} states)")
    cols[1].metric("Top state by revenue",
                   f"{top_state['customer_state']} - {money(top_state['total_revenue'])}")
    
#---------------------------------------------------------------------------------Tahap B
def show_customer_rfm() -> None:
    """Halaman 2: distribusi segmen RFM, one-time vs repeat buyer, dan Top Champions."""
    rfm = load("mart_customer_rfm").replace({"rfm_segment": SEGMENT_LABELS})
    segments = (rfm["rfm_segment"].value_counts().rename_axis("segment").reset_index(name="customers"))
    st.subheader("Customers per RFM segment")
    st.altair_chart(
        alt.Chart(segments).mark_bar().encode(
            x=alt.X("customers:Q", title="Customers"),
            y=alt.Y("segment:N", sort="-x", title=None),
            tooltip=["segment", alt.Tooltip("customers:Q", format=",")],
        ),
        width="stretch",
    )
    
    buyers = load("mart_buyer_type")
    st.subheader("One-time vs repeat buyers")
    cols = st.columns(len(buyers))
    for col, row in zip(cols, buyers.itertuples()):
        col.metric(row.customer_type, f"{row.customer_pct:.1f}% of customers",
                   f"{row.revenue_pct:.1f}% of revenue", delta_color="off", delta_arrow="off")
    
    st.subheader("Top 10 Champions by monetary value")
    champions = rfm[rfm["rfm_segment"] == "Champions"].nlargest(10, "monetary")
    columns = ["customer_unique_id", "recency_days", "frequency", "monetary", "rfm_code"]
    st.dataframe(champions[columns], hide_index=True, width="stretch")
    
def show_revenue_trend() -> None:
    """Halaman 3: growth MoM, kategori dengan pertumbuhan tertinggi, dan pola musiman."""
    monthly = load("mart_revenue_monthly").assign(month=lambda df: pd.to_datetime(df["month"]))
    # 2016 hanya berisi 3 bulan dengan sedikit order (Desember 2016: R$ 10.90), jadi growth
    # pada Januari 2017 terhadap Desember 2016 mencapai ratusan ribu persen dan menenggelamkan
    # grafik. Mulai dari Februari 2017.
    growth = monthly[monthly["month"] >= "2017-02-01"]
    st.subheader("Month-over-month revenue growth (from Feb 2017)")
    st.altair_chart(
        alt.Chart(growth).mark_bar().encode(
            x=alt.X("yearmonth(month):O", title="Month"),
            y=alt.Y("growth_mom_pct:Q", title="Growth vs previous month (%)"),
            # Hijau untuk naik, merah untuk turun
            color=alt.condition("datum.growth_mom_pct >= 0",
                                alt.value("#2E7D32"), alt.value("#C62828")),
            tooltip=[alt.Tooltip("month:T", format="%b %Y"), "growth_mom_pct"],
        ),
        width="stretch",
    )
    
    st.subheader("Fastest-growing categories, 2017 -> 2018")
    min_revenue = st.slider("Minimum 2017 revenue (R$)", 0, 50_000, 10_000, step=5_000,
                            help="Exludes categories whose growth comes from a tiny 2017 base.")
    categories = load("mart_category_growth")
    top = categories[categories["revenue_2017"] >= min_revenue].nlargest(10, "growth_pct")
    st.dataframe(top, hide_index=True, width="stretch")
    
    seasonality = load("mart_seasonality")
    st.subheader("Revenue by calender month (all years combined)")
    st.altair_chart(
        alt.Chart(seasonality).mark_bar().encode(
            x=alt.X("month_name:N", sort=alt.SortField("month_number"), title=None),
            y=alt.Y("total_revenue:Q", title="Revenue (R$)"),
            tooltip=["month_name", alt.Tooltip("total_revenue:Q", format=",.2f"), "order_count"],
        ),
        width="stretch"
    )
    st.caption("September-December has only one full year of data, January-August has two, "
               "so this is not a true seasonal pattern.")
    
#---------------------------------------------------------------------------------Tahap C
def show_churn_risk() -> None:
    """Halaman 4: status churn, review score per status, dan repeat buyer yang berhenti."""
    churn = load("mart_customer_churn")
    status = churn["churn_status"].value_counts()
    reviews = load("mart_review_churn").set_index("churn_status")
    cols = st.columns(4)
    cols[0].metric("Active customers", f"{status['Active']:,}")
    cols[1].metric("At Risk customers", f"{status['At Risk']:,}",
                   f"{status['At Risk'] / status.sum():.2%} of all",
                   delta_color="off", delta_arrow="off")
    cols[2].metric("Avg review, Active", f"{reviews.loc['Active', 'avg_review_score']:.2f}")
    cols[3].metric("Avg review, At Risk", f"{reviews.loc['At Risk', 'avg_review_score']:.2f}")
    st.caption("At Risk = no order for more than 180 days. Most of them are one-time buyers.")
    
    lapsed = churn[(churn["churn_status"] == "At Risk") & (churn["total_orders"] > 1)]
    st.subheader(f"Repeat buyers who stoped ordering: {len(lapsed):,} customers")
    st.write("These customers came back at least once, so they are the best retention target.")
    st.dataframe(lapsed.nlargest(10, "total_orders"), hide_index=True, width="stretch")
    
def show_category_region() -> None:
    """Halaman 5: revenue per state dan kategori dengan margin terbesar di tiap state."""
    states = load("mart_revenue_per_state")
    st.subheader("Revenue per state")
    st.altair_chart(
        alt.Chart(states).mark_bar().encode(
            x=alt.X("customer_state:N", sort="-y", title="State"),
            y=alt.Y("total_revenue:Q", title="Revenue (R$)"),
            tooltip=["customer_state", alt.Tooltip("total_revenue:Q", format=",.2f"),
                     "customer_count", "revenue_per_customer"],
        ),
        width="stretch",
    )
    st.subheader("Top category per state by gross margin")
    per_state = load("mart_category_per_state").sort_values("gross_margin", ascending=False)
    st.dataframe(per_state, hide_index=True, width="stretch")

st.set_page_config(page_title="Olist Data Pipeline Dashboard", layout="wide")
st.title("Olist E-Commerce Dashboard")
st.caption("Delivered orders, Sep 2016 - Aug 2018. Data snapshot from the 'marts' schema of the "
           f"[Olist Data Pipeline]({REPO_URL}) . [dbt docs and lineage]({DOCS_URL})")
pages = st.tabs(["Overview", "Customer RFM", "Revenue Trend", "Churn Risk", "Category & Region"])
with pages[0]:
    show_overview()
with pages[1]:
    show_customer_rfm()
with pages[2]:
    show_revenue_trend()
with pages[3]:
    show_churn_risk()
with pages[4]:
    show_category_region()