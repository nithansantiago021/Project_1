import streamlit as st
import plotly.express as px
import sys
import os
#  "Go up one folder from where this file is, and look for imports there."
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import run_query
import queries

def render_tab3(selected_iata):
    if selected_iata == "ALL":
        st.subheader("Aircraft Model Distribution")

        sql = queries.get_query1()
        df1 = run_query(sql)
        fig = px.pie(df1, values='total_flights', names='aircraft_model', hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Top 10 Widely used aircraft")
        df2 = df1.head(10)
        df2.columns = ["Aircraft Models", "Total Physical Flights"]
        st.dataframe(df2, hide_index=True, use_container_width=True)

    else:
        st.subheader(f"Most Used Aircraft Models at {selected_iata}")
        sql = queries.get_query1_specific(selected_iata)
        df1 = run_query(sql)
        
        fig = px.pie(df1, values='total_flights', names='aircraft_model', hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader(f"Top 10 Widely used aircraft by airlines in {selected_iata}")
        df2 = df1.head(10)
        df2.columns = ["Model", "Total Physical Flights"]
        st.dataframe(df2, hide_index=True, use_container_width=True)