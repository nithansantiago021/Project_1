import streamlit as st
from database import run_query
from components import maps, tab1, tab2, tab3 # Import your modules

# SETUP
st.set_page_config(page_title="Global Air Tracker", layout="wide")

# SIDEBAR
st.sidebar.title("Airport Control Center")
all_airports_df = run_query("SELECT iata_code, name, city FROM airport ORDER BY name")
airport_options = {"All Airports (Global View)": "ALL"}
airport_options.update({f"{row['name']} ({row['iata_code']})": row['iata_code'] for _, row in all_airports_df.iterrows()})

selected_airport_label = st.sidebar.selectbox("Select Airport", options=list(airport_options.keys()))
selected_iata = airport_options[selected_airport_label]

# RENDER MAP
maps.render_map(selected_iata)

# DATA TABS
tabs = st.tabs(["Operations", "Airlines", "Aircraft"])

with tabs[0]:
    tab1.render_tab1(selected_iata)
with tabs[1]:
    tab2.render_tab2(selected_iata)
with tabs[2]:
    tab3.render_tab3(selected_iata)
