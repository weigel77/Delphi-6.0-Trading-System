"""Validate hosted Delphi live Apollo and Kairos execution through the hosted shell routes."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

import requests


def _print_block(title: str, payload: Dict[str, Any]) -> None:
    print(title)
    print(json.dumps(payload, indent=2, default=str))


def _login(session: requests.Session, base_url: str, email: str, password: str) -> Dict[str, Any]:
    login_url = f"{base_url.rstrip('/')}/hosted/login"
    response = session.post(
        login_url,
        data={"email": email, "password": password, "next": "/hosted/apollo"},
        allow_redirects=False,
        timeout=60,
    )
    return {
        "status_code": response.status_code,
        "location": response.headers.get("Location"),
        "cookies": list(session.cookies.keys()),
        "ok": response.status_code in {302, 303} and "delphi_hosted_access_token" in session.cookies,
    }


def _call_json(session: requests.Session, method: str, url: str) -> Dict[str, Any]:
    response = session.request(method, url, headers={"Accept": "application/json"}, timeout=120)
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}
    return {"status_code": response.status_code, "ok": response.ok, "payload": payload}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate hosted Delphi 5.0 live Apollo and Kairos execution.")
    parser.add_argument("--base-url", required=True, help="Hosted Delphi base URL, e.g. http://127.0.0.1:5015")
    parser.add_argument("--email", required=True, help="Hosted Bill email")
    parser.add_argument("--password", required=True, help="Hosted Bill password")
    args = parser.parse_args()

    session = requests.Session()

    login_result = _login(session, args.base_url, args.email, args.password)
    _print_block("login", login_result)
    if not login_result["ok"]:
        return 1

    apollo_result = _call_json(session, "POST", f"{args.base_url.rstrip('/')}/hosted/actions/apollo/run")
    _print_block("apollo_run", apollo_result)

    kairos_result = _call_json(session, "POST", f"{args.base_url.rstrip('/')}/hosted/actions/kairos/run")
    _print_block("kairos_run", kairos_result)

    if not apollo_result["ok"] or not apollo_result["payload"].get("ok"):
        return 2
    if not kairos_result["ok"] or not kairos_result["payload"].get("ok"):
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())