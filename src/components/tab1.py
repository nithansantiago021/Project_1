from pdb import run
from pandas import options
import streamlit as st
import plotly.express as px
from ..database import run_query
from .. import queries


def render_tab1():
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
                key='tab1_airport_select' # Unique key for this tab
            )
            selected_iata = airport_options[selected_airport_label]

        except Exception as e:
            st.error(f"Could not load airport list: {e}")
            selected_iata = "ALL" # Fallback

    with col2:
        options = st.selectbox("Select Data View", 
                               options=["Cancelled Flights", "Top 3 Destinations", "Recent Arrivals", 
                                        "More than 5 outbound flights in a day", "Outbound-Only Routes",
                                        "Flight Classification: Domestic vs. International", "Delay Sources"])
    
    st.divider()
    # --- Data Display ---
    if options == "Cancelled Flights":
        st.subheader("Cancelled Flights")
        
        # We call the function from queries.py to get the SQL string
        sql = queries.get_query9(selected_iata)
        
        # Then pass that string to our run_query function
        df = run_query(sql)
        df.columns = ["Flight Numbers", "Aircraft Registration", "Origin Airport", "Destination Airport", "Scheduled Departure"] 
        df.fillna("Not Available", inplace=True)
             
        st.dataframe(df, hide_index=True, use_container_width=True)
    
    elif options == "Top 3 Destinations":
        st.subheader(f"Top 3 Arrival Sources for {selected_iata}")
        sql = queries.get_query4(selected_iata)
        df = run_query(sql)

        # Ensure the column names match your SQL output
        df.columns = ["Airport", "City", "Arriving Flights"]

        if not df.empty:
            # 1. Create 3 columns for the KPI cards
            kpi1, kpi2, kpi3 = st.columns(3)

            # 2. Map the data to the cards
            # We use .iloc[0], .iloc[1], etc., to grab the top 3 rows safely
            with kpi1:
                st.metric(
                    label=f"🥇 {df.iloc[0]['Airport']}", 
                    value=f"{df.iloc[0]['Arriving Flights']} Flights",
                    help=f"City: {df.iloc[0]['City']}"
                )
                st.caption(f"📍 {df.iloc[0]['City']}")

            if len(df) > 1:
                with kpi2:
                    st.metric(
                        label=f"🥈 {df.iloc[1]['Airport']}", 
                        value=f"{df.iloc[1]['Arriving Flights']} Flights",
                        help=f"City: {df.iloc[1]['City']}"
                    )
                    st.caption(f"📍 {df.iloc[1]['City']}")

            if len(df) > 2:
                with kpi3:
                    st.metric(
                        label=f"🥉 {df.iloc[2]['Airport']}", 
                        value=f"{df.iloc[2]['Arriving Flights']} Flights",
                        help=f"City: {df.iloc[2]['City']}"
                    )
                    st.caption(f"📍 {df.iloc[2]['City']}")

            st.divider() # Visual break before the next section
        else:
            st.info("No arrival data found for this selection.")


    elif options == "Recent Arrivals":
        st.subheader(f"Recent Arrival from {selected_iata}")
        sql = queries.get_query6(selected_iata)
        df = run_query(sql)
        df.columns = ["Flight Numbers", "Departure From","Arrival To", "Arrival Time"]
        df.fillna("", inplace=True)

        st.dataframe(df, hide_index=True, use_container_width=True)
    
    elif options == "More than 5 outbound flights in a day":
        st.subheader(f"More than 5 outbound flights in a day from {selected_iata}")
        sql = queries.get_query3(selected_iata)
        df = run_query(sql)
        if not df.empty:
            st.bar_chart(df.set_index('destination_name')['outbound_flights'])
        else:
            st.info("No routes found with more than 5 flights for this selection.")


    elif options == "Outbound-Only Routes":
        st.subheader(f"Outbound-Only Routes from {selected_iata}")
        sql1 = queries.get_query7(selected_iata)
        df1 = run_query(sql1)
        if not df1.empty:
            df1.columns = ["Depature Airport", "Departure Airport IATA","Arrival Airport","Arrival Airport IATA"]
            df1.fillna("", inplace=True)
            st.dataframe(df1, hide_index=True, use_container_width=True)
        else:
            st.info("No outbound-only routes found")
        
    elif options == "Flight Classification: Domestic vs. International":
        st.subheader("Flight Classification: Domestic vs. International")
    
        # Fetch Data
        sql = queries.get_query5(selected_iata)
        df = run_query(sql)

        if not df.empty:
            col1, col2 = st.columns([2, 3])

            with col1:
                # 1. Prepare Chart Data
                type_counts = df['flight_type'].value_counts().reset_index()
                type_counts.columns = ['Type', 'Count']
                
                # 2. Render Pie Chart
                fig = px.pie(
                    type_counts, 
                    values='Count', 
                    names='Type', 
                    color='Type',
                    color_discrete_map={'Domestic': '#3498db', 'International': '#9b59b6'}
                )
                fig.update_layout(showlegend=True, height=350, margin=dict(t=0, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # 3. Show Raw Data
                st.dataframe(
                    df[['flight_number', 'origin', 'destination', 'flight_type']], 
                    hide_index=True, 
                    use_container_width=True,
                    height=350
                )
        else:
            st.info("No flight data available for the current selection.")
    
    elif options == "Delay Sources":
        st.subheader(f"Delay Sources for {selected_iata}")
        st.caption("Which airports send the most delayed flights to your selection?")

        sql = queries.get_query11(selected_iata)
        df = run_query(sql)

        if not df.empty:
            # Create the chart
            fig = px.bar(
                df,
                x='percent_delayed',
                y='origin_airport',
                orientation='h',
                color='percent_delayed',
                color_continuous_scale='Reds',
                labels={'percent_delayed': 'Delay %', 'origin_airport': 'Origin Airport'},
                text=df['percent_delayed'].round(1).astype(str) + '%'
            )

            fig.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                xaxis_range=[0, 100],
                coloraxis_showscale=False,
                height=450
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # Show detailed table in an expander
            with st.expander("View Full Source Data"):
                st.dataframe(df, hide_index=True, use_container_width=True)
        else:
            st.info("Not enough arrival data to calculate delay percentages.")