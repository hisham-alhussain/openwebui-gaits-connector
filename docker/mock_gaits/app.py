"""Mock GAITS API used for early development of the Open‑WebUI connector.

The service exposes two endpoints that mimic the real GAITS contract:

* GET /projects                – returns the full master list.
* GET /projects/delta?since=… – returns rows whose ``LastUpdated`` is newer
  than the supplied ``since`` timestamp.

Both endpoints expect an ``api_key`` header.  In the sandbox we simply
check it against the constant ``MOCK_KEY``.
"""

from pathlib import Path
import hashlib
import json

from fastapi import FastAPI, Header, HTTPException
import pandas as pd

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
MOCK_KEY = "MOCK_KEY"                 # will be overridden by CI env vars if needed
DATA_FILE = Path("/data/master.xlsx")  # volume‑mounted by docker‑compose

# ----------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------
app = FastAPI()


def _load_df() -> pd.DataFrame:
    """Read the Excel file into a pandas DataFrame (all columns as strings)."""
    return pd.read_excel(DATA_FILE, dtype=str)


def _hash_row(row: pd.Series) -> str:
    """
    Compute a deterministic SHA‑256 hash for a row **excluding**
    the ``LastUpdated`` column (that column is used only for delta logic).
    """
    # Drop the volatile timestamp column, then JSON‑dump the remaining dict
    payload = row.drop(labels=["LastUpdated"]).to_dict()
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


# ----------------------------------------------------------------------
# End‑points
# ----------------------------------------------------------------------
@app.get("/projects")
def list_projects(api_key: str = Header(...)):
    """Return the full master‑list."""
    if api_key != MOCK_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    df = _load_df()
    return df.to_dict(orient="records")


@app.get("/projects/delta")
def delta_projects(since: str, api_key: str = Header(...)):
    """Return only rows whose ``LastUpdated`` is newer than *since*."""
    if api_key != MOCK_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Load the whole sheet and filter on the timestamp column
    df = _load_df()
    try:
        mask = pd.to_datetime(df["LastUpdated"]) > pd.to_datetime(since)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Unable to parse 'since' timestamp: {exc}",
        )
    delta = df[mask]
    return delta.to_dict(orient="records")