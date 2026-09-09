"""Secure Odoo login automation service.

Credentials are read from HashiCorp Vault (preferred) or environment variables.
The service never returns the password or browser cookies to the caller.
"""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

LOGIN_URL = os.getenv("ODOO_LOGIN_URL", "https://124570667-saas-19-4-all.runbot317.odoo.com/web/login")
SESSION_FILE = Path(os.getenv("SESSION_FILE", "/data/odoo-session.json"))
SESSION_TTL = int(os.getenv("SESSION_TTL_SECONDS", "3600"))
API_KEY = os.getenv("AUTOMATION_API_KEY", "")

app = FastAPI(title="Odoo Login Automation", version="1.0.0")
_login_lock = asyncio.Lock()
_last_login: dict[str, Any] = {"status": "never"}


class LoginResult(BaseModel):
    status: str
    logged_in_at: int
    expires_at: int
    session_file: str


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if not API_KEY:
        raise HTTPException(503, "AUTOMATION_API_KEY is not configured")
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(401, "invalid API key")


def _vault_credentials() -> tuple[str, str] | None:
    """Read KV v2 fields login/password from HashiCorp Vault when configured."""
    addr, token, path = os.getenv("VAULT_ADDR"), os.getenv("VAULT_TOKEN"), os.getenv("VAULT_SECRET_PATH")
    if not (addr and token and path):
        return None
    url = f"{addr.rstrip('/')}/v1/{path.lstrip('/')}"
    try:
        response = httpx.get(url, headers={"X-Vault-Token": token}, timeout=10)
        response.raise_for_status()
        data = response.json().get("data", {})
        # KV v2 nests the actual values under data.
        data = data.get("data", data)
        return str(data["login"]), str(data["password"])
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"could not read credentials from Vault: {exc}") from exc


def credentials() -> tuple[str, str]:
    vault = _vault_credentials()
    if vault:
        return vault
    login, password = os.getenv("ODOO_LOGIN"), os.getenv("ODOO_PASSWORD")
    if not login or not password:
        raise RuntimeError("configure VAULT_* or ODOO_LOGIN and ODOO_PASSWORD")
    return login, password


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError("ODOO_LOGIN_URL must be an absolute HTTPS URL")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/session")
async def session_status(_: None = Depends(require_api_key)) -> dict[str, Any]:
    return {**_last_login, "session_file_exists": SESSION_FILE.exists()}


@app.post("/login", response_model=LoginResult, dependencies=[Depends(require_api_key)])
async def login() -> LoginResult:
    global _last_login
    _validate_url(LOGIN_URL)
    async with _login_lock:
        try:
            username, password = credentials()
            SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                try:
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
                    await page.locator('input[name="login"]').fill(username)
                    await page.locator('input[name="password"]').fill(password)
                    await page.locator('button[type="submit"]').click()
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                    if "/web/login" in page.url:
                        raise RuntimeError("Odoo rejected the credentials or requires an additional challenge")
                    await context.storage_state(path=str(SESSION_FILE))
                finally:
                    await browser.close()
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            _last_login = {"status": "failed", "error": str(exc), "at": int(time.time())}
            raise HTTPException(502, "login automation failed") from exc

        now = int(time.time())
        _last_login = {"status": "logged_in", "logged_in_at": now, "expires_at": now + SESSION_TTL}
        return LoginResult(status="logged_in", logged_in_at=now, expires_at=now + SESSION_TTL, session_file=str(SESSION_FILE))


@app.on_event("shutdown")
def cleanup() -> None:
    # Keep the session file available for the next process/container restart.
    pass
