import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

# Build the engine using env variables
engine = create_engine(f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}")

@st.cache_data
def run_query(query):
    return pd.read_sql(query, engine)