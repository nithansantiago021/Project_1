from pandas import options
import streamlit as st
import plotly.express as px
from ..database import run_query
from .. import queries

def render_tab2():
    # --- Airport & Data View Selection ---
    col1, col2 = st.columns([1, 1])
    with col1:
        try:
            sql = queries.get_airport_list()
            all_airports_df = run_query(sql)
            airport_options = {"All Airports (Global View)": "ALL"}
            if not all_airports_df.empty:
                airport_options.update({
                    f"{row['name']} ({row['iata_code']})": row['iata_code']
                    for _, row in all_airports_df.iterrows()
                })
            
            selected_airport_label = st.selectbox(
                "Filter by Airport", 
                options=list(airport_options.keys()),
                key='tab2_airport_select' # Unique key for this tab
            )
            selected_iata = airport_options[selected_airport_label]

        except Exception as e:
            st.error(f"Could not load airport list: {e}")
            selected_iata = "ALL" # Fallback

    with col2:
        options = st.selectbox("Select from options", options=["Airline Performance","Most used Aircraft Model by Airline"])
    
    st.divider()
    # --- Data Display ---
    if options == "Airline Performance":
        st.subheader(f"Airline Performance Leaderboard: {selected_iata}")
        
        # Call the query (passing selected_iata ensures it works for 'ALL' or specific)
        sql_query = queries.get_query8(selected_iata)
        df_perf = run_query(sql_query)

        if not df_perf.empty:
            # 1. Standardize column names for the chart labels
            # This maps the SQL snake_case to the Pretty Title Case you want in the legend
            df_perf = df_perf.rename(columns={
                'on_time_flights': 'On-Time',
                'delayed_flights': 'Delayed',
                'cancelled_flights': 'Cancelled',
                'airline_name': 'Airline Name'
            })

            # 2. Define colors using the new names
            status_colours = {
                'On-Time': '#2ecc71',
                'Delayed': '#f1c40f',
                'Cancelled': '#e74c3c'
            }

            # 3. Build the Plotly figure
            # Note: 'x' must match the new column names exactly
            fig_perf = px.bar(
                df_perf, 
                y='Airline Name', 
                x=['On-Time', 'Delayed', 'Cancelled'], 
                orientation='h',
                color_discrete_map=status_colours, 
                labels={'value': 'Percentage', 'variable': 'Flight Status'}
            )

            fig_perf.update_layout(
                barmode='stack',
                barnorm='percent', # This turns counts into 0-100% bars
                yaxis={'categoryorder': 'total ascending'},
                legend_title_text='Status',
                height=500,
                margin=dict(l=0, r=0, t=30, b=0)
            )
            
            st.plotly_chart(fig_perf, use_container_width=True)
        else:
            st.info("No performance data found for this selection.")
        
    elif options == "Most used Aircraft Model by Airline":
        st.subheader("Most Used Aircraft Model by Airline")
        sql2 = queries.get_query_airline_aircraft_model()
        df3 = run_query(sql2)
        df3.columns = ["Airline", "Most Used Model", "Flights"]
        st.dataframe(df3, hide_index=True, use_container_width=True)
            
        