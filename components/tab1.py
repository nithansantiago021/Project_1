from pdb import run
import streamlit as st
import plotly.express as px
import sys
import os

# "Go up one folder from where this file is, and look for imports there."
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import run_query
import queries


def render_tab1(selected_iata):
    if selected_iata == "ALL":
        st.subheader("Cancelled Flights")
        
        # We call the function from queries.py to get the SQL string
        sql = queries.get_query9()
        
        # Then pass that string to our run_query function
        df = run_query(sql)
        df.columns = ["Flight Numbers", "Aircraft Registration", "Origin Airport", "Destination Airport", "Scheduled Departure"] 

        df["Aircraft Registration"] = df["Aircraft Registration"].fillna("Not Available")
        df["Scheduled Departure"] = df["Scheduled Departure"].fillna("Not Available")

        st.dataframe(df, hide_index=True, use_container_width=True)

        st.subheader("All Destinations Heatmap")
        sql = queries.get_query_all_destinations()
        df = run_query(sql)
        df.columns = ["Airport", "City", "Arriving Flights"]

        fig = px.bar(
            df,
            x='Arriving Flights',
            y='Airport',
            orientation='h',
            text='Arriving Flights',
            color='Arriving Flights',
            color_continuous_scale='Reds'
        )

        fig.update_traces(
            texttemplate='%{text}',
            textposition='outside',
            marker_line_color='black',
            marker_line_width=1
        )

        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="Arriving Flights",
            yaxis_title="",
            coloraxis_showscale=False,
            height=800,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.subheader(f"Recent Arrival from {selected_iata}")
        sql = queries.get_query6_specific(selected_iata)
        df = run_query(sql)
        df.columns = ["Flight Numbers", "Aircraft Registration","IATA Code", "Origin Airport", "Arrival Time"]

        df["Aircraft Registration"] = df["Aircraft Registration"].fillna("")
        df["Arrival Time"] = df["Arrival Time"].fillna("")

        st.dataframe(df, hide_index=True, use_container_width=True)

        st.subheader(f"Top 5 International Destinations from {selected_iata}")
        sql = queries.get_query3_specific(selected_iata)
        df = run_query(sql)
        st.bar_chart(df.set_index('destination_name')['total_flights'])
        
        st.subheader(f"Outbound-Only Routes from {selected_iata}")
        sql1 = queries.get_query7_spl(selected_iata)
        df1 = run_query(sql1)
        df1.columns = ["Destination Airport","IATA CODE"]
        st.dataframe(df1, hide_index=True, use_container_width=True)