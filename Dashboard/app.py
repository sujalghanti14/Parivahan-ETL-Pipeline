import streamlit as st
from api_client import fetch_fuel_data
import duckdb
import plotly.express as px
import pandas as pd

st.set_page_config(layout="wide")

st.title("Tata Motors Market Analysis Dashboard")

df = fetch_fuel_data()

st.sidebar.header("🔧 Filters")

# Year filter
all_years = sorted(df["date"].dt.year.unique().tolist())
selected_years = st.sidebar.multiselect(
    "📅 Select Year(s)",
    options=all_years,
    default=all_years,
    placeholder="All years selected"
)

# Month-Year filter — only show periods belonging to selected years
df_year_filtered = df[df["date"].dt.year.isin(selected_years)] if selected_years else df
all_month_years = (
    df_year_filtered
    .drop_duplicates("date")
    .sort_values("date")["month_year"]
    .tolist()
)
selected_month_years = st.sidebar.multiselect(
    "🗓️ Select Month-Year(s)",
    options=all_month_years,
    default=all_month_years,
    placeholder="All months selected"
)


# Apply Year + Month-Year filters
if selected_month_years:
    df_filtered = df[df["month_year"].isin(selected_month_years)].copy()
else:
    df_filtered = df.copy()

st.sidebar.markdown("---")

# Initialize DuckDB connection
# RTA_Data      → Year + Month-Year filtered  (KPIs, pie charts, bar charts)
# RTA_Data_All  → full unfiltered data        (time-series line charts)
conn = duckdb.connect(":memory:")
# conn.register("RTA_Data", df_filtered)
conn.register("RTA_Maharashtra", df)

# KPI Cards Section
col1, col2, col3, col4 = st.columns(4)

# Query 1: 4W Vehicles
total_vehicles = conn.execute("SELECT SUM(Count) as total FROM RTA_Maharashtra").fetchall()[0][0]
with col1:
    with st.container(border=True):
        st.metric("4W Vehicles", int(total_vehicles))

# Query 2: 4W EV Vehicles
total_ev_vehicles = conn.execute("SELECT SUM(Count) as total FROM RTA_Maharashtra WHERE fuel = 'EV'").fetchall()[0][0]
with col2:
    with st.container(border=True):
        st.metric("4W EV Vehicles", int(total_ev_vehicles))

# Query 3: Tata Motors Vehicles
tata_vehicles = conn.execute("SELECT SUM(Count) as total FROM RTA_Maharashtra WHERE brand = 'TATA MOTORS'").fetchall()[0][0]
with col3:
    with st.container(border=True):
        st.metric("Tata Motors Vehicles", int(tata_vehicles))

# Query 4: 4W EV Vehicles in Tata Motors
tata_ev_vehicles = conn.execute("SELECT SUM(Count) as total FROM RTA_Maharashtra WHERE brand = 'TATA MOTORS' AND fuel = 'EV'").fetchall()[0][0]
with col4:
    with st.container(border=True):
        st.metric("Tata Motors EV Vehicles", int(tata_ev_vehicles))


# Pie Charts Side by Side
col1, col2 = st.columns([1, 1.5])

# Pie Chart for Fuel Category
with col1:
    with st.container(border=True):
        st.subheader("⚡ Fuel Type Distribution")
        fuel_data = conn.execute("""
            SELECT fuel, SUM(Count) as total_vehicles
            FROM RTA_Maharashtra
            GROUP BY fuel
        """).df()

        fig = px.pie(fuel_data, values='total_vehicles', names='fuel', 
                     title='Vehicles by Fuel Category',
                     hole=0)

        fig.update_traces(
            textposition='outside',
            textinfo='label+percent+value'
        )
        fig.update_layout(showlegend=False)

        st.plotly_chart(fig, use_container_width=True)

# Pie Chart for Brands
with col2:
    with st.container(border=True):
        st.subheader("🏢 Brand Distribution (Top 6)")
        brands_all = conn.execute("""
            SELECT brand, SUM(Count) as total_vehicles
            FROM RTA_Maharashtra
            GROUP BY brand
            ORDER BY total_vehicles DESC
        """).df()

        # Get top 6 brands and combine rest as Others
        top_6 = brands_all.head(6)
        others_total = brands_all.iloc[6:]['total_vehicles'].sum()

        if others_total > 0:
            others_row = pd.DataFrame({'brand': ['Others'], 'total_vehicles': [others_total]})
            brands_data = pd.concat([top_6, others_row], ignore_index=True)
        else:
            brands_data = top_6

        fig_brands = px.pie(brands_data, values='total_vehicles', names='brand', 
                     title='Vehicles by Brand (Top 6 + Others)',
                     hole=0)

        fig_brands.update_traces(
            textposition='outside',
            textinfo='label+percent+value'
        )
        fig_brands.update_layout(showlegend=False)

        st.plotly_chart(fig_brands, use_container_width=True)


# Line Chart for 4W Registration by Month
st.subheader("📈 4W Registrations by Month")
monthly_year_data = conn.execute("""
    SELECT month_year, date, SUM(Count) as total_vehicles
    FROM RTA_Maharashtra
    GROUP BY month_year, date
    ORDER BY date
""").df()

# Convert to datetime for continuous axis
monthly_year_data['date'] = pd.to_datetime(monthly_year_data['date'])
monthly_year_data['Year'] = monthly_year_data['date'].dt.year.astype(str)
monthly_year_data['Month'] = monthly_year_data['date'].dt.strftime('%b')

# Create month order for proper sorting
month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
monthly_year_data['month_num'] = monthly_year_data['Month'].apply(lambda x: month_order.index(x) if x in month_order else 12)
monthly_year_data = monthly_year_data.sort_values('month_num')

# Create line chart with separate lines for each year
fig_line = px.line(monthly_year_data, x='Month', y='total_vehicles', color='Year',
                   title='4W Registrations by Month',
                   markers=True,
                   labels={'total_vehicles': 'Number of Vehicles', 'Month': 'Month', 'Year': 'Year'},
                   category_orders={'Month': month_order})

fig_line.update_layout(
    hovermode='x unified',
    height=500,
    showlegend=True
)

st.plotly_chart(fig_line, use_container_width=True)

# Line Chart for Fuel Category by Month
st.subheader("⚡ Fuel Type Registrations by Month")
fuel_monthly_data = conn.execute("""
    SELECT month_year, date, fuel, SUM(Count) as total_vehicles
    FROM RTA_Maharashtra
    GROUP BY month_year, date, fuel
    ORDER BY date
""").df()

# Convert to datetime for continuous axis
fuel_monthly_data['date'] = pd.to_datetime(fuel_monthly_data['date'])

# Create line chart for fuel category
fig_fuel_line = px.line(fuel_monthly_data, x='date', y='total_vehicles', 
                        color='fuel',
                        title='Fuel Type Registrations by Month (Jan 2025 - Mar 2026)',
                        markers=True,
                        labels={'total_vehicles': 'Number of Vehicles', 'date': 'Date', 'fuel': 'Fuel Type'})

fig_fuel_line.update_xaxes(tickformat="%b %y")
fig_fuel_line.update_layout(
    hovermode='x unified',
    height=500,
    showlegend=True
)

st.plotly_chart(fig_fuel_line, use_container_width=True)

# Line Chart for Top 5 Brands by Month
st.subheader("🏢 Top 5 Brands Registrations by Month")

# Get top 5 brands
top_5_brands = conn.execute("""
    SELECT brand, SUM(Count) as total
    FROM RTA_Maharashtra
    GROUP BY brand
    ORDER BY total DESC
    LIMIT 5
""").df()

top_5_brand_names = top_5_brands['brand'].tolist()

# Get monthly data for top 5 brands
brands_monthly_data = conn.execute(f"""
    SELECT month_year, date, brand, SUM(Count) as total_vehicles
    FROM RTA_Maharashtra
    WHERE brand IN ({','.join([f"'{brand}'" for brand in top_5_brand_names])})
    GROUP BY month_year, date, brand
    ORDER BY date
""").df()

# Convert to datetime for continuous axis
brands_monthly_data['date'] = pd.to_datetime(brands_monthly_data['date'])

# Create line chart for top 5 brands
fig_brands_line = px.line(brands_monthly_data, x='date', y='total_vehicles', 
                          color='brand',
                          title='Top 5 Brands Registrations by Month (Jan 2025 - Mar 2026)',
                          markers=True,
                          labels={'total_vehicles': 'Number of Vehicles', 'date': 'Date', 'brand': 'Brand'})

fig_brands_line.update_xaxes(tickformat="%b %y")
fig_brands_line.update_layout(
    hovermode='x unified',
    height=500,
    showlegend=True
)

st.plotly_chart(fig_brands_line, use_container_width=True)

# Horizontal Bar Chart for Top 10 Makers
st.subheader("📊 Registrations of Top 10 4W Makers")
top_10_makers = conn.execute("""
    SELECT brand, SUM(Count) as total_vehicles
    FROM RTA_Maharashtra
    GROUP BY brand
    ORDER BY total_vehicles DESC
    LIMIT 10
""").df()

# Sort in ascending order for better visualization (bottom to top)
top_10_makers = top_10_makers.sort_values('total_vehicles', ascending=True)

# Create horizontal bar chart
fig_bar = px.bar(top_10_makers, x='total_vehicles', y='brand', 
                 orientation='h',
                 title='Registrations of Top 10 4W Makers',
                 labels={'total_vehicles': 'Total Registrations', 'brand': 'Maker'},
                 color='total_vehicles',
                 color_continuous_scale='Blues')

fig_bar.update_traces(
    textposition='outside',
    texttemplate='%{x:,.0f}'
)

fig_bar.update_layout(
    height=500,
    showlegend=True,
    hovermode='y unified'
)

st.plotly_chart(fig_bar, use_container_width=True)



# Stacked Bar Chart for Area with Brand Breakdown
st.subheader("📍 4W Registrations by Area and Brand")
area_brand_data = conn.execute("""
    SELECT rto, brand, SUM(Count) as total_vehicles
    FROM RTA_Maharashtra
    GROUP BY rto, brand
    ORDER BY rto
""").df()

# Filter for specific brands or show top brands
brands_to_show = ['TATA MOTORS', 'MARUTI SUZUKI', 'HYUNDAI MOTOR INDIA LTD']
area_brand_filtered = area_brand_data[area_brand_data['brand'].isin(brands_to_show)].copy()

# Add "Others" category for remaining brands
others_data = area_brand_data[~area_brand_data['brand'].isin(brands_to_show)].groupby('rto')['total_vehicles'].sum().reset_index()
others_data['brand'] = 'Others'
area_brand_filtered = pd.concat([area_brand_filtered, others_data], ignore_index=True)

# Sort by total vehicles and keep only top 10 areas
area_totals = area_brand_filtered.groupby('rto')['total_vehicles'].sum().sort_values(ascending=True)
top_10_areas = area_totals.iloc[-10:].index.tolist()  # top 10 by total (ascending sort → last 10)
area_brand_filtered = area_brand_filtered[area_brand_filtered['rto'].isin(top_10_areas)]
area_order = top_10_areas  # already sorted ascending (smallest → largest)

# Create stacked horizontal bar chart
fig_stacked = px.bar(area_brand_filtered, x='total_vehicles', y='rto', color='brand',
                     orientation='h',
                     title='4W Registrations by Area and Brand',
                     labels={'total_vehicles': 'Total Count', 'rto': 'Area', 'brand': 'Brand'},
                     category_orders={'rto': area_order})

fig_stacked.update_layout(
    barmode='stack',
    height=600,
    hovermode='y unified'
)

st.plotly_chart(fig_stacked, use_container_width=True)


# View Raw Data (shows filtered data)
with st.sidebar.expander("🗂️ View Raw Data"):
    st.dataframe(df_filtered, hide_index=True)

