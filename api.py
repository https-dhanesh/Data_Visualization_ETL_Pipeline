import pandas as pd
from fastapi import FastAPI

DATA_FILE_PATH = "olist_customers_dataset.csv"

app = FastAPI()

try:
    customers_df = pd.read_csv(DATA_FILE_PATH)
except FileNotFoundError:
    customers_df = pd.DataFrame()

@app.get("/")
def read_root():
    return {"message": "Data API is running. Go to /api/v1/customers"}

@app.get("/api/v1/customers")
def get_customers():
    
    if customers_df.empty:
        return {"error": "Customer data is not loaded."}

    return customers_df.to_dict(orient="records")