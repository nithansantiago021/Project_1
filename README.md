# Air Tracker: Flight Analytics

### Project Overview

This is a part of GUVI DS project it is designed to assist aviation enthusiasts, analysts, and organizations in understanding: 
  - airport networks,
  - flight patterns,
  - flight operations while exploring detailed flight information interactively. 

**Key Objectives Met:**
- **Automated Data Extraction:** Successfully integrated with the AeroDataBox API to fetch flight schedules.
- **Database Design:** MySQL database schema (`flights`, `airport`, `aircraft`, `airport_delays`) to ensure data integrity and query efficiency.
- **Interactive UI/UX:** Developed a Streamlit application with Plotly visualizations, featuring custom parameter filtering and responsive layouts.

---

### Tech Stack
- **Language:** Python 3.11.14
- **Database:** MySQL 
- **Data Processing:** Pandas, NumPy
- **Visualization & UI:** Streamlit, Plotly Express
- **External Data Source:** AeroDataBox API (via RapidAPI)

---

### Project Structure
```bash
Project_1 /
│
├── streamlit_app.py          # Main entry point (Imports from src.components)
├── requirements.txt          # Dependencies (pandas, streamlit, sqlalchemy, etc.)
├── .env                      # API keys and Database credentials (create it in the root folder)
├── .gitignore                # Prevents .env and __pycache__ from being uploaded
│
└── src/                      # All Source Code
    ├── __init__.py           # Makes 'src' a package
    ├── pipeline.py           # ETL logic (API -> Cleaning -> MySQL)
    ├── database.py           # Database connection logic
    ├── queries.py            # SQL queries for fetching data
    └── components/           # UI modules (nested inside src)
        ├── __init__.py       # Makes 'components' a sub-package
        ├── maps.py           # Map visualizations
        ├── tab1.py           # UI for Tab 1
        ├── tab2.py           # UI for Tab 2
        └── tab3.py           # UI for Tab 3
```

---

### How to Run the Project Locally

#### Prerequisites
1. Python 3.8+ installed.
2. A local MySQL server running.
3. An active API key from AeroDataBox (RapidAPI)

### Setup Instructions
**1. Clone the repository:**
```bash
git clone https://github.com/nithansantiago021/Project_1.git
cd Global-Flight-Analytics-dashboard
```
**2. Install required dependencies:**
```bash
pip install -r requirements.txt
```
**3. Configure Connection to SQL:**
  - update the database connection credentials in your python script or `.env` file

**4. Run the Streamlit Application:**
```bash
streamlit run dashboard.py
```

