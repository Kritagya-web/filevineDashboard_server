#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import requests
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Tuple, Any

from sqlalchemy import create_engine, text
from auth_refresh import get_dynamic_headers

# =========================
# Configuration
# =========================
API_BASE_URL = "https://calljacob.api.filevineapp.com"

DB_USER     = "postgres"
DB_PASSWORD = "kritagya"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "postgres"
DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL, echo=False)

# =========================
# SQL
# =========================
# Upsert the core columns we care about; keep project_name fresh to satisfy NOT NULL.
UPSERT_PROJECT_CORE = text("""
INSERT INTO projects (project_id, project_name, date_of_incident, total_meds, last_updated)
VALUES (:project_id, :project_name, :date_of_incident, :total_meds, NOW())
ON CONFLICT (project_id) DO UPDATE SET
  project_name     = EXCLUDED.project_name,
  date_of_incident = EXCLUDED.date_of_incident,
  total_meds       = EXCLUDED.total_meds,
  last_updated     = NOW();
""")

READ_PROJECT_CORE = text("""
SELECT project_name, date_of_incident, total_meds
FROM projects
WHERE project_id = :pid
""")

# =========================
# Utility helpers
# =========================
COMM_KEYWORDS  = re.compile(r"\b(spoke|call|text|message|vm)\b", re.IGNORECASE)
EMAIL_PATTERN  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")

def mmddyyyy_to_iso(s: Optional[str]) -> Optional[str]:
    """Convert MM-DD-YYYY -> YYYY-MM-DD; returns None for 'N/A'/None/bad."""
    if not s or s == "N/A":
        return None
    try:
        m, d, y = s.split("-")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except Exception:
        return None

def format_date(datestr: Optional[str]) -> str:
    """Format ISO-like date -> MM-DD-YYYY; returns 'N/A' if invalid."""
    if not datestr:
        return "N/A"
    try:
        dt = datetime.fromisoformat(datestr[:10])
        return dt.strftime("%m-%d-%Y")
    except Exception:
        return "N/A"

def fetch_json(endpoint: str) -> Any:
    """GET helper with retry for 401/429; returns parsed JSON (dict/list) or {}."""
    headers = get_dynamic_headers()
    try:
        r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        if r.status_code == 401:
            headers = get_dynamic_headers()
            r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        elif r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"⚠️  Rate limited at {endpoint}, sleeping {retry_after}s…")
            time.sleep(retry_after)
            return fetch_json(endpoint)
        r.raise_for_status()
        data = r.json()
        return data if data is not None else {}
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Request failed for {endpoint}: {e}")
        return {}

# =========================
# Vitals: robust field picking
# =========================
def _pick_vital(
    vitals: List[Dict[str, Any]],
    *,
    friendly_names: Optional[List[str]] = None,
    fieldname_prefixes: Optional[List[str]] = None,
    want_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Prefer friendlyName match (case-insensitive, colon-agnostic),
    else match by fieldName prefix (no numeric suffixes), then filter by type.
    """
    candidates: List[Dict[str, Any]] = []

    if friendly_names:
        targets = {re.sub(r"[:\s]+$", "", n).strip().lower() for n in friendly_names}
        for it in vitals:
            fn = (it.get("friendlyName") or "").strip()
            norm = re.sub(r"[:\s]+$", "", fn).lower()
            if norm in targets:
                candidates.append(it)

    if not candidates and fieldname_prefixes:
        for it in vitals:
            fname = (it.get("fieldName") or "")
            for pref in fieldname_prefixes:
                if fname.startswith(pref.rstrip("*")):
                    candidates.append(it)
                    break

    if want_types and candidates:
        want = {t.lower() for t in want_types}
        typed = [it for it in candidates if (it.get("fieldType") or "").lower() in want]
        if typed:
            candidates = typed

    return candidates[0] if candidates else None

def sol_dol_meds_policy_limits(pid: int) -> Dict[str, Any]:
    """
    Returns a dict containing:
      SOL, DOL, Personal Injury Type, Total Meds, Policy Limits, Liability Decision, Last Offer
    Values are normalized: dates -> MM-DD-YYYY, numbers left as string (we parse meds later).
    """
    vitals = fetch_json(f"/core/projects/{pid}/vitals") or {}
    if isinstance(vitals, dict):  # in case API wraps items
        vitals = vitals.get("items") or vitals.get("data") or []
    if not isinstance(vitals, list):
        vitals = []

    def get_val(item: Optional[Dict[str, Any]]) -> Any:
        if not item:
            return "N/A"
        ft = (item.get("fieldType") or "").lower()
        val = item.get("value")
        if val in (None, ""):
            return "N/A"
        if ft == "dateonly":
            return format_date(val)
        # IMPORTANT: do NOT coerce to float here; keep raw string for precise Decimal parse later
        return val

    out: Dict[str, Any] = {}

    out["SOL"] = get_val(_pick_vital(
        vitals,
        friendly_names=["SOL"],
        fieldname_prefixes=["sol"],
        want_types=["DateOnly"]
    ))

    out["DOL"] = get_val(_pick_vital(
        vitals,
        friendly_names=["DOL", "DOL:", "Incident Date"],
        fieldname_prefixes=["incidentDate"],
        want_types=["DateOnly"]
    ))

    out["Personal Injury Type"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Personal Injury Type"],
        fieldname_prefixes=["personalinjurytype"]
    ))

    out["Total Meds"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Total Meds", "Total Meds:"],
        fieldname_prefixes=["sumOfamountbilled"],
        want_types=["Currency", "Decimal", "Number"]
    ))

    out["Policy Limits"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Policy Limits", "Policy Limits:"],
        fieldname_prefixes=["policylimits"]
    ))

    out["Liability Decision"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Liability Decision", "Liability Decision:"],
        fieldname_prefixes=["liabilitydecision"]
    ))

    out["Last Offer"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Last Offer", "Last Offer:"],
        fieldname_prefixes=["lastoffer"]
    ))

    return out

# =========================
# Intake date
# =========================
def get_intake_date(pid: int) -> str:
    """
    Returns 'MM-DD-YYYY' or 'N/A' from the appropriate intake form based on projectTypeCode.
    Adjust the key below if your form uses a different field name.
    """
    project_data = fetch_json(f"/core/projects/{pid}")
    if not project_data or not isinstance(project_data, dict):
        return "N/A"

    project_type_code = (project_data.get("projectTypeCode") or "").strip()
    endpoint_mapping = {
        "PIMaster": "intake2",
        "LOJE 2.0": "lOJEIntake20Demo",
        "LOJE 2.2": "lOJEIntake20Demo",
        "WC": "wCIntake",
    }
    endpoint = endpoint_mapping.get(project_type_code, "lOJEIntake20Demo")
    form = fetch_json(f"/core/projects/{pid}/Forms/{endpoint}")
    if not isinstance(form, dict):
        return "N/A"

    # If your org uses 'incidentDate_1' instead, add an OR branch.
    raw = form.get("dateOfIntake") or form.get("incidentDate_1")
    return format_date(raw) if raw else "N/A"

# =========================
# Decimal helpers (for meds)
# =========================
def _parse_currency_decimal(v: Optional[Any]) -> Optional[Decimal]:
    """
    Parse currency/number into Decimal(2dp) or None.
    Accepts: '19970.00000000', '19,970.00', '$19,970.00', 19970, 19970.0, Decimal().
    """
    if v in (None, "", "N/A", "None"):
        return None
    try:
        if isinstance(v, Decimal):
            d = v
        else:
            s = str(v).strip().replace(",", "").replace("$", "")
            d = Decimal(s)
        return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None

def _normalize_date(v: Optional[Any]) -> Optional[str]:
    """Return 'YYYY-MM-DD' or None."""
    if v in (None, "", "N/A"):
        return None
    try:
        return str(v)[:10]
    except Exception:
        return None

def _normalize_dec(v: Optional[Any]) -> Optional[Decimal]:
    """Return Decimal(2dp) or None."""
    if isinstance(v, Decimal):
        return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return _parse_currency_decimal(v)

# =========================
# Read/Compute/Diff
# =========================
def _get_current_project_core(pid: int) -> Dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(READ_PROJECT_CORE, {"pid": pid}).fetchone()
        if not row:
            return {"project_name": None, "date_of_incident": None, "total_meds": None}
        tm = _parse_currency_decimal(row[2]) if row[2] is not None else None
        return {
            "project_name": row[0],
            "date_of_incident": row[1],  # likely date or ISO string
            "total_meds": tm
        }

def _compute_project_core_from_api(pid: int) -> Dict[str, Any]:
    pj = fetch_json(f"/core/projects/{pid}") or {}
    project_name = pj.get("projectOrClientName") or "N/A"

    # date_of_incident
    doi_mmddyyyy = get_intake_date(pid)  # 'MM-DD-YYYY' or 'N/A'
    doi_iso = mmddyyyy_to_iso(doi_mmddyyyy) if doi_mmddyyyy and doi_mmddyyyy != "N/A" else None

    # total_meds (get raw from vitals -> Decimal here)
    vitals = sol_dol_meds_policy_limits(pid) or {}
    tm_raw = vitals.get("Total Meds")
    tm_dec = _parse_currency_decimal(tm_raw)

    return {
        "project_name": project_name,
        "date_of_incident": doi_iso,
        "total_meds": tm_dec,
    }

def _diff_core(old: Dict[str, Any], new: Dict[str, Any]) -> Tuple[bool, Dict[str, Tuple[Any, Any]]]:
    changes: Dict[str, Tuple[Any, Any]] = {}

    old_name = (old.get("project_name") or "").strip() if old.get("project_name") else None
    new_name = (new.get("project_name") or "").strip() if new.get("project_name") else None
    if (old_name or None) != (new_name or None):
        changes["project_name"] = (old.get("project_name"), new.get("project_name"))

    old_doi = _normalize_date(old.get("date_of_incident"))
    new_doi = _normalize_date(new.get("date_of_incident"))
    if old_doi != new_doi:
        changes["date_of_incident"] = (old.get("date_of_incident"), new.get("date_of_incident"))

    old_tm = _normalize_dec(old.get("total_meds"))
    new_tm = _normalize_dec(new.get("total_meds"))
    if old_tm != new_tm:
        changes["total_meds"] = (old.get("total_meds"), new.get("total_meds"))

    return (len(changes) > 0, changes)

# =========================
# Updater
# =========================
def update_project_core_fields(pid: int) -> Tuple[bool, bool, Optional[Dict[str, Tuple[Any, Any]]]]:
    """
    Update date_of_incident and total_meds (and project_name) for one project if they differ.
    Returns: (success, updated, changes_dict)
    """
    try:
        print(f"\n🔍 Checking project {pid}...")
        old = _get_current_project_core(pid)
        new = _compute_project_core_from_api(pid)

        has_change, changes = _diff_core(old, new)
        if not has_change:
            print(f"✅ Project {pid}: No changes needed")
            return True, False, None

        print(f"🔄 Project {pid}: Changes detected:")
        for field, (o, n) in changes.items():
            print(f"   - {field}: {o} → {n}")

        payload = {
            "project_id": pid,
            "project_name": new["project_name"] or "N/A",
            "date_of_incident": _normalize_date(new["date_of_incident"]),
            "total_meds": _normalize_dec(new["total_meds"]),  # Decimal(2dp) or None
        }
        # Debug payload
        # print("UPSERT payload:", payload)

        with engine.begin() as conn:
            conn.execute(UPSERT_PROJECT_CORE, payload)

        print(f"💾 Project {pid}: Updated")
        return True, True, changes

    except Exception as e:
        print(f"❌ Project {pid}: Failed to update - {e}")
        import traceback; traceback.print_exc()
        return False, False, None

# =========================
# (Optional) Pull ALL project IDs from Filevine
# =========================
def get_all_filevine_project_ids(limit: Optional[int] = None) -> List[int]:
    """
    Fetch ALL project IDs from /core/projects (paged). If limit is provided, stop after reaching it.
    """
    projects: List[int] = []
    offset = 0
    page_sz = 100
    attempts = 0
    max_attempts = 5

    while True:
        try:
            headers = get_dynamic_headers()
            resp = requests.get(
                f"{API_BASE_URL}/core/projects",
                headers=headers,
                params={"offset": offset, "limit": page_sz},
                timeout=30,
            )
            if resp.status_code == 401:
                headers = get_dynamic_headers()
                resp = requests.get(
                    f"{API_BASE_URL}/core/projects",
                    headers=headers,
                    params={"offset": offset, "limit": page_sz},
                    timeout=30,
                )
            resp.raise_for_status()

            body = resp.json() or {}
            items = body.get("items", [])
            if not items:
                break

            for pj in items:
                pid = pj.get("projectId", {}).get("native")
                if pid:
                    projects.append(int(pid))
                    if limit is not None and len(projects) >= limit:
                        break

            if limit is not None and len(projects) >= limit:
                break

            offset += page_sz
            attempts = 0

        except requests.exceptions.RequestException as e:
            attempts += 1
            if attempts >= max_attempts:
                print(f"⚠️  Giving up after {attempts} failed attempts: {e}")
                break
            backoff = min(60, 2 ** attempts)
            print(f"⚠️  Error fetching projects (attempt {attempts}), retrying in {backoff}s: {e}")
            time.sleep(backoff)

    return projects if limit is None else projects[:limit]

# =========================
# Batch runner
# =========================
def update_core_fields_batch(project_ids: List[int], batch_size: int = 20, pause_s: int = 5):
    if not project_ids:
        print("⚠️  No projects to update.")
        return

    updated, nochange, failed = 0, 0, 0

    for i in range(0, len(project_ids), batch_size):
        batch = project_ids[i:i+batch_size]
        print(f"\n📦 Processing batch {i//batch_size + 1} "
              f"(projects {i+1}-{min(i+batch_size, len(project_ids))})...")

        for pid in batch:
            ok, did_update, _ = update_project_core_fields(pid)
            if not ok:
                failed += 1
            elif did_update:
                updated += 1
            else:
                nochange += 1

        if i + batch_size < len(project_ids):
            print(f"⏳ Waiting {pause_s} seconds before next batch…")
            time.sleep(pause_s)

    print("\n" + "="*52)
    print("📊 Core Fields Update Summary")
    print(f"✅ Updated: {updated}")
    print(f"➖ No change: {nochange}")
    print(f"❌ Failed: {failed}")

# =========================
# Main
# =========================
def main():
    print("🚀 Starting core fields updater…")

    # --- Option A: test specific project IDs (recommended while validating) ---
    # project_ids = [1699079, 1743170]  # <-- put your specific project_id(s) here for testing

    # --- Option B: when ready for all ---
    project_ids = get_all_filevine_project_ids(limit=None)

    print(f"📊 Projects to process: {project_ids}")
    update_core_fields_batch(project_ids, batch_size=1, pause_s=0)

    print("🎉 Done!")

if __name__ == "__main__":
    main()
