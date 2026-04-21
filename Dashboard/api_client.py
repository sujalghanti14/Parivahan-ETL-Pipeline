import os
import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "RTA_Maharashtra"

_supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)


@st.cache_data(ttl=300)
def fetch_fuel_data() -> pd.DataFrame:
    response = _supabase.table(TABLE_NAME).select("rto,brand,fuel,count,month,year").execute()
    df = pd.DataFrame(response.data)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)

    # Convert month abbreviations like "FEB" → 2 if not already numeric
    month_numeric = pd.to_numeric(df['month'], errors='coerce')
    if month_numeric.isna().any():
        month_numeric = pd.to_datetime(df['month'], format='%b').dt.month
    df['date'] = pd.to_datetime(dict(year=df['year'].astype(int), month=month_numeric.astype(int), day=1))
    df['month_year'] = df['date'].dt.strftime('%b-%y').str.upper()

    # Define EV types
    ev_types = ["STRONG HYBRID EV", "PURE EV", "PLUG-IN HYBRID EV", "ELECTRIC(BOV)","PETROL(E20)/HYBRID"]
    # Create new category column
    df["fuel"] = df["fuel"].apply(
        lambda x: "EV" if x in ev_types else "ICE")
    
    # Define maker mapping
    maker_mapping = {
        "MAHINDRA & MAHINDRA LIMITED": "MAHINDRA & MAHINDRA",
        "MAHINDRA ELECTRIC AUTOMOBILE LTD": "MAHINDRA & MAHINDRA",
        
        "MARUTI SUZUKI INDIA LTD": "MARUTI SUZUKI",
        "MARUTI UDYOG LTD": "MARUTI SUZUKI",
        
        "TATA ADVANCED SYSTEMS LTD": "TATA MOTORS",
        "TATA MOTORS LTD": "TATA MOTORS",
        "TATA MOTORS PASSENGER VEHICLES LTD": "TATA MOTORS",
        "TATA PASSENGER ELECTRIC MOBILITY LTD": "TATA MOTORS",
        
        "TOYOTA HIACE GL COMMUTER": "TOYOTA",
        "TOYOTA KIRLOSKAR MOTOR PVT LTD": "TOYOTA"
        }


    # Create new consolidated maker column
    df["brand"] = df["brand"].map(maker_mapping).fillna(df["brand"])

    df["brand"] = df["brand"].astype("category")
    df["fuel"] = df["fuel"].astype("category")
    df["rto"] = df["rto"].astype("category")


    return df
 

# print(fetch_fuel_data().sample(10))