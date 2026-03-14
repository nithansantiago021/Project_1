import streamlit as st
import plotly.express as px
import sys
import os

# "Go up one folder from where this file is, and look for imports there."
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import run_query
import queries

def render_tab2(selected_iata):
    if selected_iata == "ALL":
        st.subheader("Airline Performance")
        sql1 = queries.get_query8()
        
        df2 = run_query(sql1)

        # Clean up any missing airline names just in case
        df2["Airline Name"] = df2["Airline Name"].fillna("Unknown Carrier")

        status_colours = {
            'On-Time Flights': '#2ecc71',
            'Delayed Flights': '#f1c40f',
            'Cancelled Flights': '#e74c3c'
        }

        fig_perf = px.bar(df2, 
                          y = 'Airline Name', x = ['On-Time Flights', 'Delayed Flights', 'Cancelled Flights'], 
                          orientation= 'h',
                          color_discrete_map=status_colours, 
                          labels={'value': 'Percentage of Flights','variable': 'Flight Status'}
                          )
        fig_perf.update_layout(
            barmode = 'stack',
            barnorm='percent',
            yaxis={'categoryorder':'total ascending'},
            legend_title_text='Status',
            height = 500,
            margin = dict(l=0,r=0,t=30,b=0)
        )
        st.plotly_chart(fig_perf, use_container_width=True)

        st.subheader("Most Used Aircraft Model by Airline")
        sql2 = queries.get_query_airline_aircraft_model()
        df3 = run_query(sql2)
        df3.columns = ["Airline", "Most Used Model", "Flights"]
        st.dataframe(df3, hide_index=True, use_container_width=True)
            
    else:
        st.subheader(f"Airline Reliability in {selected_iata}")
        sql1 = queries.get_query11_specific(selected_iata)
        df2 = run_query(sql1)
        df2.columns = ["Airline Name", "Delay Rate"]

        # Build the Heatmap Bar Chart
        fig_delays = px.bar(
            df2, 
            x='Delay Rate', 
            y='Airline Name', 
            orientation='h',
            text='Delay Rate',               # Puts the actual percentage number on the bar
            color='Delay Rate',              # Drives the color gradient
            color_continuous_scale='Reds'    # Uses a heat scale (light red to dark red)
        )

        # Clean up the layout and sort it so the worst offender is at the top
        fig_delays.update_traces(
            texttemplate='%{text:.1f}%',     # Formats the text label to 1 decimal place with a % sign
            textposition='outside',          # Puts the label neatly at the end of the bar
            marker_line_color='black',       # Adds a crisp border to the bars
            marker_line_width=1
        )

        fig_delays.update_layout(
            yaxis={'categoryorder': 'total ascending'}, # Sorts bars automatically
            xaxis_title="Delay Rate (%)",
            yaxis_title="",                             # Hides the Y-axis title for a cleaner look
            coloraxis_showscale=False,                  # Hides the redundant color legend
            height=400,
            margin=dict(l=0, r=0, t=10, b=0)
        )

        st.plotly_chart(fig_delays, use_container_width=True)