import streamlit as st
import plotly.graph_objects as go
import sys
import os

# Tells Python to look one folder up to find database.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import run_query

def render_map(selected_iata):
    # --- THE MAP (Dynamic Switch) ---
    if selected_iata == "ALL":
        st.header("Global Flight Network")

        # 1. Query all unique routes and their start/end coordinates
        all_routes_query = """
        SELECT 
            o.latitude AS origin_lat, o.longitude AS origin_lon,
            d.latitude AS dest_lat, d.longitude AS dest_lon
        FROM flights f
        JOIN airport o ON f.origin_iata = o.iata_code
        JOIN airport d ON f.destination_iata = d.iata_code
        GROUP BY o.latitude, o.longitude, d.latitude, d.longitude;
        """
        df_all_routes = run_query(all_routes_query)

        # 2. Query all active airports to plot as dots
        all_nodes_query = """
        SELECT DISTINCT a.name, a.latitude, a.longitude 
        FROM airport a 
        JOIN flights f ON a.iata_code = f.origin_iata OR a.iata_code = f.destination_iata;
        """
        df_nodes = run_query(all_nodes_query)

        # 3. Create the Line Coordinates with 'None' interleaving
        route_lons, route_lats = [], []
        for _, row in df_all_routes.iterrows():
            route_lons.extend([row['origin_lon'], row['dest_lon'], None])
            route_lats.extend([row['origin_lat'], row['dest_lat'], None])

        fig = go.Figure()

        # --- ADD GLOBAL DOTTED LINES ---
        fig.add_trace(go.Scattergeo(
            lon=route_lons, lat=route_lats, mode='lines',
            # Using rgba for transparency so overlapping lines glow instead of clutter
            line=dict(width=1, color='rgba(0, 191, 255, 0.4)', dash='dot'), 
            name='Active Routes', hoverinfo='none'
        ))

        # --- ADD ALL AIRPORT DOTS ---
        fig.add_trace(go.Scattergeo(
            lon=df_nodes['longitude'], lat=df_nodes['latitude'], text=df_nodes['name'],
            mode='markers', marker=dict(size=5, color='gold', opacity=0.8), name='Airports'
        ))

        # --- MAP STYLING (Flat, Global View) ---
        fig.update_geos(
            showcountries=True, countrycolor="white",
            showland=True, landcolor="#2b2b2b",      
            showocean=True, oceancolor="#121212",    
            resolution=50, projection_type="natural earth" 
        )
        
        fig.update_layout(
            height=650, margin={"r":0,"t":0,"l":0,"b":0}, 
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.header(f"Flight Routes for {selected_iata}")

        # 1. Get the Coordinates of the Selected "Anchor" Airport
        anchor_query = f"SELECT latitude, longitude FROM airport WHERE iata_code = '{selected_iata}'"
        anchor_df = run_query(anchor_query)
        
        if not anchor_df.empty:
            anchor_lat = anchor_df['latitude'].iloc[0]
            anchor_lon = anchor_df['longitude'].iloc[0]
        else:
            anchor_lat, anchor_lon = 0, 0 

        # 2. Get the Destination and Origin Data
        dept_query = f"""
        SELECT a.name, a.latitude, a.longitude, COUNT(f.flight_number) as count
        FROM flights f JOIN airport a ON f.destination_iata = a.iata_code
        WHERE f.origin_iata = '{selected_iata}' GROUP BY a.name, a.latitude, a.longitude
        """
        arr_query = f"""
        SELECT a.name, a.latitude, a.longitude, COUNT(f.flight_number) as count
        FROM airport a JOIN flights f ON f.origin_iata = a.iata_code
        WHERE f.destination_iata = '{selected_iata}' GROUP BY a.name, a.latitude, a.longitude
        """
        df_dept = run_query(dept_query)
        df_arr = run_query(arr_query)
        
        # 3. Create the Line Coordinates (Interleaving 'None' to break the lines)
        dept_lons, dept_lats = [], []
        for _, row in df_dept.iterrows():
            dept_lons.extend([anchor_lon, row['longitude'], None])
            dept_lats.extend([anchor_lat, row['latitude'], None])
            
        arr_lons, arr_lats = [], []
        for _, row in df_arr.iterrows():
            arr_lons.extend([anchor_lon, row['longitude'], None])
            arr_lats.extend([anchor_lat, row['latitude'], None])

        fig = go.Figure()

        # --- ADD DOTTED LINES ---
        # Departures (Outbound) - Blue Dotted
        fig.add_trace(go.Scattergeo(
            lon=dept_lons, lat=dept_lats, mode='lines',
            line=dict(width=1.5, color='#00bfff', dash='dot'),
            name='Departures (Blue)', hoverinfo='none'
        ))

        # Arrivals (Inbound) - Red Dotted
        fig.add_trace(go.Scattergeo(
            lon=arr_lons, lat=arr_lats, mode='lines',
            line=dict(width=1.5, color='#ff4500', dash='dot'),
            name='Arrivals (Red)', hoverinfo='none'
        ))

        # --- ADD AIRPORT DOTS ---
        fig.add_trace(go.Scattergeo(
            lon=df_dept['longitude'], lat=df_dept['latitude'], text=df_dept['name'] + ' (Destination)',
            mode='markers', marker=dict(size=8, color='#00bfff', opacity=0.8), name='Destination Airports'
        ))

        fig.add_trace(go.Scattergeo(
            lon=df_arr['longitude'], lat=df_arr['latitude'], text=df_arr['name'] + ' (Origin)',
            mode='markers', marker=dict(size=8, color='#ff4500', opacity=0.8), name='Origin Airports'
        ))

        fig.add_trace(go.Scattergeo(
            lon=[anchor_lon], lat=[anchor_lat], text=[f"{selected_iata} (Selected)"],
            mode='markers', marker=dict(size=14, color='gold', symbol='star'), name='Selected Airport'
        ))

        # --- MAP STYLING (Flat, Global View) ---
        fig.update_geos(
            showcountries=True, countrycolor="white",
            showland=True, landcolor="#2b2b2b",      
            showocean=True, oceancolor="#121212",    
            resolution=50, projection_type="natural earth" 
        )
        
        fig.update_layout(
            height=650, margin={"r":0,"t":0,"l":0,"b":0}, 
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig, use_container_width=True)