import streamlit as st
import plotly.express as px
from api_client import fetch_fuel_data
from data import (
    apply_filters,
    fuel_type_breakdown,
    fuel_group_breakdown,
    monthly_trend_by_fuel_group,
    top_brands,
    rto_summary,
    ev_adoption_trend,
    MONTH_ORDER,
)

st.set_page_config(page_title="Fuel Registration Dashboard", layout="wide")
st.title("Maharashtra Vehicle Fuel Registration Dashboard")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    df = fetch_fuel_data()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

all_rtos = sorted(df["rto"].unique())
selected_rtos = st.sidebar.multiselect("RTO", all_rtos, placeholder="All RTOs")

all_years = sorted(df["year"].unique())
selected_years = st.sidebar.multiselect("Year", all_years, default=all_years)

all_months = [m for m in MONTH_ORDER if m in df["month"].unique()]
selected_months = st.sidebar.multiselect("Month", all_months, default=all_months)

filtered = apply_filters(df, selected_rtos, selected_years, selected_months)

if filtered.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# ── KPI row ────────────────────────────────────────────────────────────────────
total = int(filtered["count"].sum())
unique_rtos = filtered["rto"].nunique()
unique_brands = filtered["brand"].nunique()
ev_total = int(filtered[filtered["fuel"].isin(["PURE EV", "STRONG HYBRID EV"])]["count"].sum())
ev_share = round(ev_total / total * 100, 1) if total else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Registrations", f"{total:,}")
k2.metric("RTOs Covered", unique_rtos)
k3.metric("Brands", unique_brands)
k4.metric("EV Share", f"{ev_share}%")

st.divider()

# ── Row 1: fuel category pie + top brands bar ──────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Registrations by Fuel Category")
    group_df = fuel_group_breakdown(filtered)
    fig = px.pie(group_df, names="Category", values="Registrations", hole=0.4)
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 Brands")
    brand_df = top_brands(filtered, 10)
    fig = px.bar(
        brand_df, x="Registrations", y="Brand", orientation="h",
        color="Registrations", color_continuous_scale="Blues",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: monthly trend by fuel group ─────────────────────────────────────────
st.subheader("Monthly Registrations by Fuel Category")
trend_df = monthly_trend_by_fuel_group(filtered)
fig = px.bar(
    trend_df, x="period", y="count", color="group", barmode="stack",
    labels={"count": "Registrations", "period": "Month", "group": "Category"},
)
fig.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)

# ── Row 3: EV adoption trend + top RTOs ───────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("EV Adoption Trend")
    ev_df = ev_adoption_trend(filtered)
    fig = px.line(
        ev_df, x="period", y="ev_share_%",
        markers=True, labels={"ev_share_%": "EV Share (%)", "period": "Month"},
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Top 15 RTOs by Registrations")
    rto_df = rto_summary(filtered, 15)
    fig = px.bar(
        rto_df, x="Registrations", y="RTO", orientation="h",
        color="Registrations", color_continuous_scale="Greens",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Row 4: fuel type detail table ─────────────────────────────────────────────
with st.expander("Fuel Type Breakdown (detailed)"):
    ft_df = fuel_type_breakdown(filtered)
    st.dataframe(ft_df, use_container_width=True, hide_index=True)
