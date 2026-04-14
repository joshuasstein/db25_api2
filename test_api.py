"""
API test script for db25_api2.

Usage:
    python test_api.py [--base-url URL] [--username USER] [--password PASS]

Defaults:
    base_url  : http://127.0.0.1:8001
    username  : tester
    password  : password

The script:
  1. Obtains a JWT via POST /login.
  2. Runs every endpoint defined in api.yaml — list endpoints first, then
     uses real IDs/names harvested from those responses to drive detail and
     sub-resource endpoints.
  3. Verifies auth rejection (no token → 401) for a sample endpoint.
  4. Verifies 404 for non-existent IDs/names.
  5. Prints a detailed failure report and a pass/fail summary.
"""

import argparse
import csv
import io
import json
import sys
import textwrap
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    name: str
    passed: bool
    method: str
    url: str
    status_code: Optional[int] = None
    expected_status: Optional[int] = None
    error: str = ""
    response_preview: str = ""
    elapsed_ms: float = 0.0

results: list[TestResult] = []


def record(result: TestResult) -> TestResult:
    results.append(result)
    status = "PASS" if result.passed else "FAIL"
    mark = "✓" if result.passed else "✗"
    print(f"  {mark} [{status}] {result.name}  ({result.elapsed_ms:.0f} ms)")
    if not result.passed:
        print(f"        URL     : {result.method} {result.url}")
        if result.status_code is not None:
            print(f"        Status  : {result.status_code} (expected {result.expected_status})")
        if result.error:
            print(f"        Error   : {result.error}")
        if result.response_preview:
            preview = textwrap.indent(result.response_preview[:800], "        | ")
            print(f"        Response:\n{preview}")
    return result


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _preview(resp: requests.Response) -> str:
    """Return a short, readable preview of a response body."""
    ct = resp.headers.get("Content-Type", "")
    try:
        if "json" in ct:
            return json.dumps(resp.json(), indent=2)
        return resp.text
    except Exception:
        return resp.text


def get(session: requests.Session, url: str, *, params=None) -> requests.Response:
    t0 = time.monotonic()
    resp = session.get(url, params=params, timeout=30)
    resp.elapsed_ms = (time.monotonic() - t0) * 1000  # type: ignore[attr-defined]
    return resp


def post(session: requests.Session, url: str, *, json_body=None) -> requests.Response:
    t0 = time.monotonic()
    resp = session.post(url, json=json_body, timeout=30)
    resp.elapsed_ms = (time.monotonic() - t0) * 1000  # type: ignore[attr-defined]
    return resp


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------

def check(
    name: str,
    resp: requests.Response,
    expected_status: int = 200,
    *,
    check_nonempty: bool = False,
) -> tuple[bool, Any]:
    """
    Assert status code.  If the response is CSV and check_nonempty=True,
    also assert at least one data row exists.
    Returns (passed, parsed_data).
    parsed_data is a list-of-dicts for CSV or a dict/list for JSON, else None.
    """
    elapsed = getattr(resp, "elapsed_ms", 0.0)
    url = resp.url
    method = resp.request.method if resp.request else "GET"

    if resp.status_code != expected_status:
        record(TestResult(
            name=name, passed=False,
            method=method, url=url,
            status_code=resp.status_code, expected_status=expected_status,
            response_preview=_preview(resp),
            elapsed_ms=elapsed,
        ))
        return False, None

    ct = resp.headers.get("Content-Type", "")
    parsed: Any = None

    if "csv" in ct:
        try:
            reader = csv.DictReader(io.StringIO(resp.text))
            parsed = list(reader)
        except Exception as e:
            record(TestResult(
                name=name, passed=False,
                method=method, url=url,
                status_code=resp.status_code, expected_status=expected_status,
                error=f"CSV parse failed: {e}",
                response_preview=resp.text[:400],
                elapsed_ms=elapsed,
            ))
            return False, None
        if check_nonempty and len(parsed) == 0:
            record(TestResult(
                name=name, passed=False,
                method=method, url=url,
                status_code=resp.status_code, expected_status=expected_status,
                error="Response body is empty (no CSV rows)",
                response_preview=resp.text[:400],
                elapsed_ms=elapsed,
            ))
            return False, parsed
    elif "json" in ct:
        try:
            parsed = resp.json()
        except Exception as e:
            record(TestResult(
                name=name, passed=False,
                method=method, url=url,
                status_code=resp.status_code, expected_status=expected_status,
                error=f"JSON parse failed: {e}",
                response_preview=resp.text[:400],
                elapsed_ms=elapsed,
            ))
            return False, None
        if check_nonempty and (parsed is None or parsed == [] or parsed == {}):
            record(TestResult(
                name=name, passed=False,
                method=method, url=url,
                status_code=resp.status_code, expected_status=expected_status,
                error="Response body is empty",
                elapsed_ms=elapsed,
            ))
            return False, parsed

    record(TestResult(
        name=name, passed=True,
        method=method, url=url,
        status_code=resp.status_code, expected_status=expected_status,
        elapsed_ms=elapsed,
    ))
    return True, parsed


def first_value(rows: list[dict], key: str) -> Optional[str]:
    """Return the first non-empty value of `key` from a list of CSV row dicts."""
    for row in rows or []:
        v = row.get(key)
        if v not in (None, "None", ""):
            return v
    return None


# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------

def run_tests(base_url: str, username: str, password: str) -> None:
    base_url = base_url.rstrip("/")

    # -----------------------------------------------------------------------
    # Section helpers
    # -----------------------------------------------------------------------
    def section(title: str) -> None:
        print(f"\n{'─' * 60}")
        print(f"  {title}")
        print(f"{'─' * 60}")

    # -----------------------------------------------------------------------
    # 1. Authentication
    # -----------------------------------------------------------------------
    section("Authentication")

    anon = requests.Session()
    authed = requests.Session()

    # 1a. Valid login
    resp = post(anon, f"{base_url}/login", json_body={"username": username, "password": password})
    ok, data = check("POST /login — valid credentials", resp)
    if not ok:
        print("\n  Cannot continue without a token. Aborting.")
        _print_summary()
        sys.exit(1)

    token = (data or {}).get("access_token")
    if not token:
        record(TestResult(
            name="POST /login — token present in response",
            passed=False, method="POST", url=resp.url,
            error="'access_token' missing from response",
            response_preview=_preview(resp),
        ))
        print("\n  Cannot continue without a token. Aborting.")
        _print_summary()
        sys.exit(1)

    record(TestResult(
        name="POST /login — token present in response",
        passed=True, method="POST", url=resp.url,
        status_code=200, expected_status=200,
    ))

    authed.headers.update({"Authorization": f"Bearer {token}"})

    # 1b. Invalid credentials
    resp = post(anon, f"{base_url}/login", json_body={"username": "nobody", "password": "wrong"})
    check("POST /login — invalid credentials → 401", resp, expected_status=401)

    # 1c. Auth guard — request without token should be rejected
    resp = get(anon, f"{base_url}/v1/systems")
    check("GET /v1/systems — no token → 401", resp, expected_status=401)

    # -----------------------------------------------------------------------
    # 2. Lookup / reference tables  (list + first-item detail)
    # -----------------------------------------------------------------------

    # Helper: test a list endpoint + fetch the first item by integer ID
    def test_list_and_detail_by_id(
        resource: str,
        id_col: str,
        detail_path_template: str,
    ) -> Optional[str]:
        """
        Fetches /v1/<resource>, checks it, then fetches the detail for the
        first ID found.  Returns the first ID string (or None).
        """
        list_url = f"{base_url}/v1/{resource}"
        resp = get(authed, list_url)
        ok, rows = check(f"GET /v1/{resource} — list", resp, check_nonempty=True)
        first_id = first_value(rows, id_col) if ok else None

        if first_id:
            detail_url = f"{base_url}{detail_path_template.format(id=first_id)}"
            resp2 = get(authed, detail_url)
            check(f"GET {detail_path_template.format(id=first_id)} — detail", resp2)

            # 404 for non-existent ID
            bad_url = f"{base_url}{detail_path_template.format(id=99999999)}"
            resp3 = get(authed, bad_url)
            check(
                f"GET {detail_path_template.format(id=99999999)} — not found → 404",
                resp3, expected_status=404,
            )

        return first_id

    section("Reference tables — units, aggregations, alignments, durations, labels")
    test_list_and_detail_by_id("units",        "unitID",        "/v1/units/{id}")
    test_list_and_detail_by_id("aggregations", "aggregationID", "/v1/aggregations/{id}")
    test_list_and_detail_by_id("alignments",   "alignmentID",   "/v1/alignments/{id}")
    test_list_and_detail_by_id("durations",    "durationID",    "/v1/durations/{id}")
    test_list_and_detail_by_id("labels",       "labelID",       "/v1/labels/{id}")

    section("Reference tables — manufacturers, module_models, modules")
    test_list_and_detail_by_id("manufacturers",  "manufacturerID",  "/v1/manufacturers/{id}")
    test_list_and_detail_by_id("module_models",  "module_modelID",  "/v1/module_models/{id}")
    test_list_and_detail_by_id("modules",        "moduleID",        "/v1/modules/{id}")

    section("Reference tables — projects, sites")
    test_list_and_detail_by_id("projects", "projectID", "/v1/projects/{id}")
    test_list_and_detail_by_id("sites",    "siteID",    "/v1/sites/{id}")

    section("Reference tables — loads")
    test_list_and_detail_by_id("loads", "loadID", "/v1/loads/{id}")

    # -----------------------------------------------------------------------
    # 3. Systems
    # -----------------------------------------------------------------------
    section("Systems")

    resp = get(authed, f"{base_url}/v1/systems")
    ok, sys_rows = check("GET /v1/systems — list", resp, check_nonempty=True)
    system_name = first_value(sys_rows, "system_name") if ok else None

    if system_name:
        sn = system_name

        resp = get(authed, f"{base_url}/v1/systems/{sn}")
        check(f"GET /v1/systems/{sn} — detail", resp)

        resp = get(authed, f"{base_url}/v1/systems/{sn}/number_of_modules")
        check(f"GET /v1/systems/{sn}/number_of_modules", resp)

        resp = get(authed, f"{base_url}/v1/systems/{sn}/dc_capacity")
        check(f"GET /v1/systems/{sn}/dc_capacity", resp)

        resp = get(authed, f"{base_url}/v1/systems/{sn}/measurements/last_measurement_date")
        ok2, date_data = check(f"GET /v1/systems/{sn}/measurements/last_measurement_date", resp)

        resp = get(authed, f"{base_url}/v1/systems/{sn}/subsystems")
        check(f"GET /v1/systems/{sn}/subsystems", resp)

        resp = get(authed, f"{base_url}/v1/systems/{sn}/date_range")
        ok3, range_data = check(f"GET /v1/systems/{sn}/date_range", resp)

        # Measurements — need a date window.  Use date_range if available,
        # otherwise fall back to last_measurement_date ± 1 day.
        start_dt = end_dt = None
        if ok3 and isinstance(range_data, dict):
            start_dt = range_data.get("start_date")
            end_dt   = range_data.get("end_date")
        elif ok2 and isinstance(date_data, dict):
            last = date_data.get("last_measurement_date")
            if last:
                # Use a 1-day window ending on last measurement
                import datetime
                try:
                    dt = datetime.datetime.fromisoformat(last.replace("Z", "+00:00"))
                    end_dt   = dt.isoformat()
                    start_dt = (dt - datetime.timedelta(days=1)).isoformat()
                except ValueError:
                    pass

        if start_dt and end_dt:
            resp = get(authed, f"{base_url}/v1/systems/{sn}/measurements",
                       params={"start_date": start_dt, "end_date": end_dt})
            check(f"GET /v1/systems/{sn}/measurements (date range)", resp)
        else:
            print(f"  ⚠ Skipping /v1/systems/{sn}/measurements — could not determine date range")

        # 404 for a bogus system name
        resp = get(authed, f"{base_url}/v1/systems/__no_such_system__")
        check("GET /v1/systems/__no_such_system__ — not found → 404", resp, expected_status=404)

    else:
        print("  ⚠ No systems found — skipping system sub-resource tests")

    # -----------------------------------------------------------------------
    # 4. Testpads
    # -----------------------------------------------------------------------
    section("Testpads")

    resp = get(authed, f"{base_url}/v1/testpads")
    ok, tp_rows = check("GET /v1/testpads — list", resp, check_nonempty=True)
    testpad_name = first_value(tp_rows, "testpad_name") if ok else None

    if testpad_name:
        tn = testpad_name

        resp = get(authed, f"{base_url}/v1/testpads/{tn}")
        check(f"GET /v1/testpads/{tn} — detail", resp)

        resp = get(authed, f"{base_url}/v1/testpads/{tn}/date_range")
        ok_dr, tp_range = check(f"GET /v1/testpads/{tn}/date_range", resp)

        start_dt = end_dt = None
        if ok_dr and isinstance(tp_range, dict):
            start_dt = tp_range.get("start_date")
            end_dt   = tp_range.get("end_date")

        if start_dt and end_dt:
            resp = get(authed, f"{base_url}/v1/testpads/{tn}/measurements",
                       params={"start_date": start_dt, "end_date": end_dt})
            check(f"GET /v1/testpads/{tn}/measurements (date range)", resp)
        else:
            # Try a generic recent window even without a confirmed range
            import datetime
            now = datetime.datetime.utcnow()
            end_dt_fb   = now.isoformat() + "Z"
            start_dt_fb = (now - datetime.timedelta(days=30)).isoformat() + "Z"
            resp = get(authed, f"{base_url}/v1/testpads/{tn}/measurements",
                       params={"start_date": start_dt_fb, "end_date": end_dt_fb})
            check(f"GET /v1/testpads/{tn}/measurements (last 30 days)", resp)

        # 404 for bogus testpad name
        resp = get(authed, f"{base_url}/v1/testpads/__no_such_testpad__")
        check("GET /v1/testpads/__no_such_testpad__ — not found → 404", resp, expected_status=404)

    else:
        print("  ⚠ No testpads found — skipping testpad sub-resource tests")

    # -----------------------------------------------------------------------
    # 5. Summary
    # -----------------------------------------------------------------------
    _print_summary()


def _print_summary() -> None:
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    total  = len(results)

    print(f"\n{'═' * 60}")
    print(f"  RESULTS: {passed}/{total} passed", end="")
    print(f"  ·  {failed} failed" if failed else "  ·  all passed")
    print(f"{'═' * 60}")

    if failed:
        print("\n  Failed tests:")
        for r in results:
            if not r.passed:
                print(f"    ✗ {r.name}")
                if r.status_code and r.expected_status:
                    print(f"        got {r.status_code}, expected {r.expected_status}")
                if r.error:
                    print(f"        {r.error}")
        sys.exit(1)
    else:
        sys.exit(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test all db25_api2 endpoints.")
    parser.add_argument("--base-url",  default="http://127.0.0.1:8001", help="API base URL")
    parser.add_argument("--username",  default="tester",   help="Login username")
    parser.add_argument("--password",  default="password", help="Login password")
    args = parser.parse_args()

    print(f"Target : {args.base_url}")
    print(f"User   : {args.username}")

    run_tests(args.base_url, args.username, args.password)
