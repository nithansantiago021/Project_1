# Project_1
## Air Tracker: Flight Analytics

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
**3. Configure the Database:**
  - Execute the `schema.sql` file in your MySQL environment to build the normalised tables.
  - update the database connection credentials in your python script or `.env` file

**4. Run the Streamlit Application:**
```bash
streamlit run dashboard.py
```
