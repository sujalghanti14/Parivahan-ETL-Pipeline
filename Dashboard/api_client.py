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
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype(int)
    return df
 