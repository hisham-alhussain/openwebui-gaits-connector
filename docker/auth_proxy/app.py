# docker/auth_proxy/app.py
from fastapi import FastAPI, Header, HTTPException

app = FastAPI()

# In‑memory mapping – replace later with Azure AD / Okta call
ROLE_MAP = {
    "abdulrahman@example.com": ["EXECUTIVE", "PROJECT_OWNER"],
    "director1@example.com":    ["EXECUTIVE"],
    # any other email gets an empty list (no special rights)
}

@app.get("/whoami")
def whoami(email: str = Header(...)):
    if not email:
        raise HTTPException(status_code=401, detail="Missing email header")
    roles = ROLE_MAP.get(email.lower(), [])
    return {"email": email.lower(), "roles": roles}