I have replaced my postgre username and password with placeholders in the DB_URL


# End-to-End Data Pipeline (KaStack Intern Task)

This project is a complete, end-to-end data pipeline built for the KaStack Data Engineer internship task. It extracts data from multiple sources (CSVs and a self-built API), loads it into a Postgres database, transforms it into analytics-ready tables, and visualizes it in Grafana. The entire pipeline is orchestrated by Prefect.

## 🏛️ Project Architecture

This pipeline follows a modern **Extract, Load, Transform (ELT)** architecture.

1.  **Extract:** Data is pulled from two CSV files and one live FastAPI endpoint.
2.  **Load:** Raw, cleaned data is loaded directly into a **PostgreSQL** database.
3.  **Transform:** SQL scripts are run *inside* Postgres to create fast, performant, pre-aggregated analytics tables.
4.  **Orchestration:** **Prefect 3** manages and schedules the entire pipeline to run every hour.
5.  **Visualization:** **Grafana** connects to the Postgres analytics tables to display the final dashboard.

---

## 🛠️ Tech Stack

* **Python:** Main language for scripts.
* **FastAPI:** To serve one of the data files as a live JSON API.
* **Pandas:** For data extraction and cleaning in Python.
* **PostgreSQL:** As the data warehouse.
* **Prefect 3:** For workflow orchestration and scheduling.
* **Grafana:** For dashboarding and visualization.
* **SQLAlchemy:** To connect Python to Postgres.

---

## 🚀 How to Run

### 1. Prerequisites

* Python 3.9+
* PostgreSQL server installed and running.
* Grafana installed and running.

### 2. Setup

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/https-dhanesh/KaStack_Task.git
    cd kastack_project
    ```

2.  **Create Virtual Environment:**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Create Postgres Database:**
    * Open `pgAdmin` or `psql`.
    * Create a new database named `olist_db`.

5.  **Configure Pipeline:**
    * Open the `pipeline.py` file.
    * Go to line 15 (or wherever `DB_URL` is) and **update the `DB_URL` string** with your Postgres username and password:
        ```python
        DB_URL = "postgresql://YOUR_USER:YOUR_PASSWORD@localhost:5432/olist_db"
        ```

### 3. Running the Full System (3 Terminals)

You must have 3 separate terminals running. Activate the `(venv)` in each one.

**Terminal 1: Start the Prefect Server (The "Brain")**
```bash
prefect server start
```
*(Keep this running. Access the dashboard at `http://127.0.0.1:4200`)*

**Terminal 2: Start the FastAPI (The "Data Source")**
```bash
uvicorn api:app --reload
```
*(Keep this running. Access the API at `http://127.0.0.1:8000/api/v1/customers`)*

**Terminal 3: Start the Prefect Worker (The "Hands")**
```bash

$env:PREFECT_API_URL="[http://127.0.0.1:4200/api](http://127.0.0.1:4200/api)"

prefect work-pool create my-process-pool --type process

prefect deploy ./pipeline.py:olist_etl_flow -n "Olist Hourly ETL" --cron "0 * * * *" -p "my-process-pool"

prefect worker start -p "my-process-pool"

```
*(Keep this running. It will now automatically run the pipeline every hour and you can trigger manual runs from the dashboard.)*

### 4. Setup Grafana

1.  Open Grafana (`http://localhost:3000`).
2.  Add a new **PostgreSQL** Data Source.
3.  Use these settings:
    * **Host:** `localhost:5432`
    * **Database:** `olist_db`
    * **User:** `YOUR_USER`
    * **Password:** `YOUR_PASSWORD`
    * **SSL Mode:** `disable`
4.  Click "Save & Test".
5.  Create a new dashboard and use the SQL queries from `pipeline.py` (in the `create_analytics_tables` function) to build your charts.

---

## 📸 Screenshots

### Prefect Dashboard

*This screenshot shows the "Olist Hourly ETL" deployment is active, scheduled, and ready.*

![Prefect Dashboard](prefect_screenshot.png)

### Grafana Dashboard

*This dashboard visualizes the analytics-ready tables, providing quick insights into sales and delivery performance.*

![Grafana Dashboard](grafana_screenshot.png)