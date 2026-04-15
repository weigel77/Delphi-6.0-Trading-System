from __future__ import annotations

import argparse
import http.client
import json
import os
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path


ROUTES_TO_TEST = [
    "/",
    "/apollo",
    "/hosted",
    "/hosted/login",
    "/hosted/performance",
    "/hosted/journal",
    "/hosted/open-trades",
    "/hosted/manage-trades",
    "/hosted/apollo",
    "/hosted/kairos",
]


def build_env(port: int, host: str, launch_url: str) -> dict[str, str]:
    env = dict(os.environ)
    env["DELPHI_RUNTIME_TARGET"] = "hosted"
    env["APP_PORT"] = str(port)
    env["APP_HOST"] = host
    env["HOSTED_PUBLIC_BASE_URL"] = launch_url
    return env


def run_python_json(python_executable: str, code: str, env: dict[str, str], cwd: Path) -> dict[str, object]:
    completed = subprocess.run(
        [python_executable, "-c", code],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    stdout = completed.stdout.strip()
    if completed.returncode != 0:
        return {
            "ok": False,
            "returncode": completed.returncode,
            "stdout": stdout,
            "stderr": completed.stderr.strip(),
        }
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "returncode": completed.returncode,
            "stdout": stdout,
            "stderr": completed.stderr.strip(),
        }


def wait_for_port(host: str, port: int, *, timeout_seconds: float = 15.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
            candidate.settimeout(1.0)
            if candidate.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.2)
    return False


def probe_route(host: str, port: int, route: str) -> dict[str, object]:
    connection = http.client.HTTPConnection(host, port, timeout=10)
    try:
        connection.request("GET", route, headers={"Host": f"{host}:{port}"})
        response = connection.getresponse()
        body = response.read()
        return {
            "route": route,
            "status": response.status,
            "location": response.getheader("Location") or "",
            "body_length": len(body),
        }
    except Exception as exc:
        return {
            "route": route,
            "status": "EXCEPTION",
            "error": str(exc),
        }
    finally:
        connection.close()


def probe_login_failure(host: str, port: int, *, email: str, password: str) -> dict[str, object]:
    body = urllib.parse.urlencode({"email": email, "password": password, "next": "/hosted"})
    connection = http.client.HTTPConnection(host, port, timeout=15)
    try:
        connection.request(
            "POST",
            "/hosted/login",
            body=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": f"{host}:{port}",
            },
        )
        response = connection.getresponse()
        html = response.read().decode("utf-8", errors="ignore")
        marker = 'hosted-shell-error-message">'
        message = ""
        index = html.find(marker)
        if index >= 0:
            start = index + len(marker)
            end = html.find("</div>", start)
            message = html[start:end].strip()
        return {
            "status": response.status,
            "location": response.getheader("Location") or "",
            "message": message,
        }
    except Exception as exc:
        return {
            "status": "EXCEPTION",
            "error": str(exc),
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Delphi 5.0 hosted mode locally on an isolated port.")
    parser.add_argument("--port", type=int, default=5015)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--login-email", default="hosted-validator@example.com")
    parser.add_argument("--login-password", default="invalid-password")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    python_executable = sys.executable
    launch_url = f"http://{args.host}:{args.port}"
    env = build_env(args.port, args.host, launch_url)

    config_summary = run_python_json(
        python_executable,
        "from app import app, get_runtime_profile; import json; profile = get_runtime_profile(app); print(json.dumps({'runtime_target': app.config.get('RUNTIME_TARGET'), 'host': profile.host, 'port': profile.port, 'launch_url': profile.launch_url, 'use_https': profile.use_https, 'session_cookie_name': app.config.get('SESSION_COOKIE_NAME') or '', 'supabase_url_present': bool(__import__('config').get_app_config().supabase_url), 'supabase_publishable_present': bool(__import__('config').get_app_config().supabase_publishable_key), 'supabase_secret_present': bool(__import__('config').get_app_config().supabase_secret_key), 'allowed_emails_configured': bool(__import__('config').get_app_config().hosted_private_allowed_emails)}))",
        env,
        repo_root,
    )
    connectivity = run_python_json(
        python_executable,
        "from app import app; import json; result = app.extensions['supabase_integration'].check_connectivity(); print(json.dumps({'ok': result.ok, 'endpoint': result.endpoint, 'status_code': result.status_code, 'detail': result.detail}))",
        env,
        repo_root,
    )

    process = subprocess.Popen(
        [python_executable, "app.py"],
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    startup_ok = wait_for_port(args.host, args.port)
    startup_output = ""
    route_results: list[dict[str, object]] = []
    login_result: dict[str, object] = {}
    try:
        if startup_ok:
            route_results = [probe_route(args.host, args.port, route) for route in ROUTES_TO_TEST]
            login_result = probe_login_failure(args.host, args.port, email=args.login_email, password=args.login_password)
        else:
            time.sleep(1.0)
    finally:
        process.terminate()
        try:
            startup_output, _ = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            startup_output, _ = process.communicate(timeout=10)

    report = {
        "startup": {
            "ok": startup_ok,
            "host": args.host,
            "port": args.port,
            "launch_url": launch_url,
            "output": startup_output.strip(),
        },
        "config_summary": config_summary,
        "supabase_connectivity": connectivity,
        "routes": route_results,
        "login_probe": login_result,
    }
    print(json.dumps(report, indent=2))
    return 0 if startup_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())