from __future__ import annotations

import argparse
import json
import socket
import ssl
from pathlib import Path
import sys
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import get_app_config


def resolve_local(hostname: str) -> dict[str, Any]:
    try:
        records = socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
        addresses = sorted({item[4][0] for item in records if item and len(item) > 4})
        return {"ok": True, "addresses": addresses}
    except OSError as exc:
        return {"ok": False, "error": str(exc)}


def resolve_doh(provider: str, hostname: str) -> dict[str, Any]:
    if provider == "google":
        url = f"https://dns.google/resolve?name={hostname}&type=A"
        headers = {"Accept": "application/json"}
    else:
        url = f"https://cloudflare-dns.com/dns-query?name={hostname}&type=A"
        headers = {"Accept": "application/dns-json"}
    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=10.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
        answers = [item.get("data") for item in payload.get("Answer", []) if item.get("data")]
        return {
            "ok": payload.get("Status") == 0,
            "status": payload.get("Status"),
            "answers": answers,
            "raw": payload,
        }
    except Exception as exc:  # pragma: no cover - network diagnostic
        return {"ok": False, "error": str(exc)}


def probe_https(url: str) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlopen(request, timeout=10.0, context=ssl.create_default_context()) as response:
            body = response.read(256).decode("utf-8", errors="ignore")
            return {
                "ok": True,
                "status_code": response.getcode(),
                "body_excerpt": body,
            }
    except URLError as exc:
        return {"ok": False, "error": str(exc.reason)}
    except Exception as exc:  # pragma: no cover - network diagnostic
        return {"ok": False, "error": str(exc)}


def build_report(supabase_url: str) -> dict[str, Any]:
    parsed = urlparse(supabase_url)
    hostname = parsed.hostname or ""
    auth_url = f"{supabase_url.rstrip('/')}/auth/v1/settings" if supabase_url else ""
    rest_url = f"{supabase_url.rstrip('/')}/rest/v1/" if supabase_url else ""
    local_dns = resolve_local(hostname) if hostname else {"ok": False, "error": "No hostname configured."}
    google_dns = resolve_doh("google", hostname) if hostname else {"ok": False, "error": "No hostname configured."}
    cloudflare_dns = resolve_doh("cloudflare", hostname) if hostname else {"ok": False, "error": "No hostname configured."}
    auth_probe = probe_https(auth_url) if hostname and (local_dns.get("ok") or google_dns.get("ok") or cloudflare_dns.get("ok")) else {"ok": False, "error": "Skipped because hostname does not resolve."}
    rest_probe = probe_https(rest_url) if hostname and (local_dns.get("ok") or google_dns.get("ok") or cloudflare_dns.get("ok")) else {"ok": False, "error": "Skipped because hostname does not resolve."}

    verdict = "ok"
    if not hostname:
        verdict = "missing-url"
    elif not google_dns.get("ok") and not cloudflare_dns.get("ok"):
        verdict = "global-nxdomain"
    elif not local_dns.get("ok"):
        verdict = "local-dns-failure"
    elif not auth_probe.get("ok") or not rest_probe.get("ok"):
        verdict = "https-endpoint-failure"

    return {
        "supabase_url": supabase_url,
        "hostname": hostname,
        "auth_url": auth_url,
        "rest_url": rest_url,
        "local_dns": local_dns,
        "google_doh": google_dns,
        "cloudflare_doh": cloudflare_dns,
        "auth_probe": auth_probe,
        "rest_probe": rest_probe,
        "verdict": verdict,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Supabase DNS and endpoint reachability for Delphi hosted mode.")
    parser.add_argument("--url", default="", help="Optional Supabase base URL override.")
    args = parser.parse_args()

    config = get_app_config()
    supabase_url = args.url.strip() or config.supabase_url or ""
    report = build_report(supabase_url)
    print(json.dumps(report, indent=2))
    return 0 if report["verdict"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())