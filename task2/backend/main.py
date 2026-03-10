import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
from clickhouse_driver import Client
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")

JWKS_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"


def _init_clickhouse() -> None:
    client = Client(host=CLICKHOUSE_HOST)
    client.execute("CREATE DATABASE IF NOT EXISTS reports")
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.user_reports
        (
            username          String,
            prosthesis_serial String,
            report_date       Date,
            avg_response_ms   Float64,
            movement_count    UInt32,
            processed_at      DateTime
        ) ENGINE = MergeTree()
        ORDER BY (username, report_date)
        """
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_clickhouse()
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Authorization"],
)

security = HTTPBearer()


def _get_jwks() -> dict:
    response = httpx.get(JWKS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def get_current_username(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    token = credentials.credentials
    try:
        jwks = _get_jwks()
        unverified_header = jwt.get_unverified_header(token)
        key = next(
            (k for k in jwks["keys"] if k["kid"] == unverified_header["kid"]),
            None,
        )
        if key is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown key")

        payload: dict[str, Any] = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            options={"verify_aud": False},
        )
        username: str = payload.get("preferred_username", "")
        if not username:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No username in token")
        return username
    except JWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc


@app.get("/reports")
def get_report(username: str = Depends(get_current_username)) -> dict:
    client = Client(host=CLICKHOUSE_HOST)
    rows = client.execute(
        """
        SELECT prosthesis_serial, report_date, avg_response_ms, movement_count, processed_at
        FROM reports.user_reports
        WHERE username = %(username)s
        ORDER BY report_date
        """,
        {"username": username},
    )

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No processed data yet for this user. Please try again later.",
        )

    data = [
        {
            "prosthesis_serial": row[0],
            "report_date": str(row[1]),
            "avg_response_ms": row[2],
            "movement_count": row[3],
            "processed_at": str(row[4]),
        }
        for row in rows
    ]

    return {"username": username, "report": data}
