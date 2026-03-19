import streamlit as st
import plotly.express as px
from ..database import run_query
from .. import queries

def render_tab3():
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
                key='tab3_airport_select' # Unique key to avoid widget conflicts
            )
            selected_iata = airport_options[selected_airport_label]

        except Exception as e:
            st.error(f"Could not load airport list: {e}")
            selected_iata = "ALL" # Fallback

    with col2:
        options = st.selectbox(
            "Select Data View", 
            options=["Aircraft Model Distribution", "Top 10 Widely Used Aircraft", "Route Diversity: Models per City Pair"],
            key='tab3_data_view_select' # Unique key
        )
    
    st.divider()

    # --- Data Display ---
    if options == "Aircraft Model Distribution":
        # Initialize session state for drill-down
        if 'selected_mfr' not in st.session_state:
            st.session_state.selected_mfr = None

        # --- TOP LEVEL: MANUFACTURER SHARE ---
        if st.session_state.selected_mfr is None:
            st.subheader("Aircraft Manufacturer Market Share")
            df = run_query(queries.get_manufacturer_share(selected_iata))
            df.columns = ["Aircraft Manufacturer", "Total Flights"]
            if not df.empty:
                # Calculate Percentage for the table
                total = df['Total Flights'].sum()
                df['Percentage'] = ((df['Total Flights'] / total) * 100).round(1).astype(str) + '%'
                
                col1, col2 = st.columns([2, 1])
                with col1:
                    fig = px.bar(df, y='Total Flights', x='Aircraft Manufacturer')
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.write("### Data Breakdown")
                    st.dataframe(df[['Aircraft Manufacturer', 'Percentage']], hide_index=True)
                    
                    # Selection to drill down
                    mfr_list = ["-- Select to see models --"] + df['Aircraft Manufacturer'].tolist()
                    choice = st.selectbox("View models for:", mfr_list)
                    if choice != "-- Select to see models --":
                        st.session_state.selected_mfr = choice
                        st.rerun()
            else:
                st.info("No data found.")

        # --- DRILL DOWN: MODELS FOR SPECIFIC MANUFACTURER ---
        else:
            mfr = st.session_state.selected_mfr
            if st.button(f"⬅ Back to All Manufacturers"):
                st.session_state.selected_mfr = None
                st.rerun()

            st.subheader(f"Models for {mfr}")
            df_models = run_query(queries.get_models_by_manufacturer(mfr, selected_iata))
            df_models.columns = ["Aircraft Model", "Total Flights"]

            if not df_models.empty:
                total = df_models['Total Flights'].sum()
                df_models['Percentage'] = ((df_models['Total Flights'] / total) * 100).round(1).astype(str) + '%'

                col1, col2 = st.columns([2, 1])
                with col1:
                    fig = px.bar(df_models, x='Total Flights', y='Aircraft Model')
                    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.write(f"### {mfr} Models")
                    st.dataframe(df_models[['Aircraft Model', 'Percentage']], hide_index=True)


    elif options == "Top 10 Widely Used Aircraft":
        st.subheader(f"Top 10 Widely Used Aircraft at {selected_airport_label.split(' (')[0]}")
        sql = queries.get_query1(selected_iata)
        df = run_query(sql)
        
        if not df.empty:
            df_top10 = df.head(10)
            df_top10.columns = ["Aircraft Model", "Number of Flights"]
            st.dataframe(df_top10, hide_index=True, use_container_width=True)
        else:
            st.info("No aircraft data to display for this selection.")
    
    elif options == "Route Diversity: Models per City Pair":
        st.subheader("Route Diversity: Models per City Pair")
        st.caption("City pairs served by more than 2 different aircraft models.")

        sql = queries.get_query10(selected_iata)
        df = run_query(sql)

        if not df.empty:
            # Create a display string for the chart axis
            df['Route'] = df['origin_name'] + " ➔ " + df['destination_name']

            # 1. Graphical Representation (Horizontal Bar)
            fig = px.bar(
                df,
                x='aircraft_model_count',
                y='Route',
                orientation='h',
                labels={'aircraft_model_count': 'Unique Aircraft Models', 'Route': 'City Pair'},
                color='aircraft_model_count',
                color_continuous_scale='blugrn'
            )
            
            fig.update_layout(
                yaxis={'categoryorder': 'total ascending'}, # Busiest at the top
                height=400,
                margin=dict(l=0, r=0, t=10, b=0)
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # 2. Detailed Data Table
            with st.expander("View Raw Diversity Data"):
                df.columns = ["Origin Airport", "Destination Airport", "Unique Aircraft Models", "Routes"]
                st.dataframe(df[["Origin Airport", "Destination Airport", "Unique Aircraft Models"]], hide_index= True, use_container_width=True)
        else:
            st.info("No routes found with more than 2 different aircraft models for this selection.")