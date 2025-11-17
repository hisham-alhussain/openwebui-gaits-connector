# docker/mock_gaits/app.py
from fastapi import FastAPI, Header, HTTPException
import pandas as pd
from pathlib import Path
import json, hashlib, datetime

app = FastAPI()
DATA_FILE = Path("/data/master.xlsx")   # mount a volume with a sample file

def _load_df() -> pd.DataFrame:
    return pd.read_excel(DATA_FILE, dtype=str)

def _hash_row(row: pd.Series) -> str:
    return hashlib.sha256(
        json.dumps(row.drop(labels=["LastUpdated"]).to_dict(), sort_keys=True).encode()
    ).hexdigest()

@app.get("/projects")
def list_projects(api_key: str = Header(...)):
    if api_key != "MOCK_KEY":
        raise HTTPException(status_code=401, detail="Invalid API key")
    df = _load_df()
    return df.to_dict(orient="records")

@app.get("/projects/delta")
def delta_projects(since: str, api_key: str = Header(...)):
    if api_key != "MOCK_KEY":
        raise HTTPException(status_code=401, detail="Invalid API key")
    df = _load_df()
    mask = pd.to_datetime(df["LastUpdated"]) > pd.to_datetime(since)
    delta = df[mask]
    return delta.to_dict(orient="records")