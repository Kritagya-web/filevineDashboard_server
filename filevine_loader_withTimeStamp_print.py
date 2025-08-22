#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print-only data loader for Filevine projects.

- Preserves your fetch/transform functions.
- Removes all DB creation/upserts and any SQLAlchemy usage.
- For each project, prints the table schema and the row that would be inserted.

Usage:
  python print_only_loader.py --type "LOJE 2.0" --limit 10
"""

import time
import json
import requests
from pprint import pprint
from datetime import datetime
from auth_refresh import get_dynamic_headers
import re
from typing import Optional, List, Dict, Tuple

# --- Configuration ---
API_BASE_URL   = "https://calljacob.api.filevineapp.com"
COMM_KEYWORDS  = re.compile(r"\b(spoke|call|text|message|vm)\b", re.IGNORECASE)
EMAIL_PATTERN  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
TARGET_ROLES   = {"Case Manager", "Supervisor", "Paralegal", "Attorney"}

# --- Table Schemas (for display only) ---
TABLE_SCHEMAS: Dict[str, str] = {
    "projects": """
projects (
  project_id             BIGINT       PRIMARY KEY,
  project_name           TEXT         NOT NULL,
  phase_name             TEXT,
  incident_date          DATE NULL,
  sol_due_date           DATE NULL,
  total_meds             NUMERIC(14,2),
  policy_limits          TEXT,
  personal_injury_type   TEXT,
  liability_decision     TEXT,
  last_offer             TEXT,
  date_of_incident       DATE NULL,
  client_contact_count   INTEGER,
  latest_client_contact  TIMESTAMP NULL,
  project_type_code      TEXT,
  last_updated           TIMESTAMP DEFAULT NOW()
)""".strip(),
    "negotiation": """
negotiation (
  project_id            BIGINT    PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  negotiator            TEXT,
  settlement_date       DATE NULL,
  settled               TEXT,
  settled_amount        NUMERIC(14,2),
  last_offer            TEXT,
  last_offer_date       DATE NULL,
  date_assigned_to_nego DATE NULL,
  last_updated          TIMESTAMP DEFAULT NOW()
)""".strip(),
    "insurance_info": """
insurance_info (
  project_id                BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  defendant_insurance_name  TEXT,
  client_insurance_name     TEXT,
  last_updated              TIMESTAMP DEFAULT NOW()
)""".strip(),
    "breakdown_info": """
breakdown_info (
  project_id              BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  lien_negotiator_name    TEXT,
  lien_negotiator_company TEXT,
  lien_negotiator_title   TEXT,
  lien_negotiator_dept    TEXT,
  date_assigned           DATE NULL, 
  date_completed          DATE NULL,
  last_updated            TIMESTAMP DEFAULT NOW()
)""".strip(),
    "lit_case_review": """
lit_case_review (
  project_id               BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  trial_date               DATE NULL,
  date_complaint_filed     DATE NULL,
  date_attorney_assigned   DATE NULL,
  settlement_amount        TEXT,
  settlement_date          DATE NULL,
  dismissal_filed_on       DATE NULL,
  last_updated             TIMESTAMP DEFAULT NOW()
)""".strip(),
    "contacts": """
contacts (
  project_id    BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  case_manager  TEXT,
  supervisor    TEXT,
  attorney      TEXT,
  paralegal     TEXT,
  last_updated  TIMESTAMP DEFAULT NOW()
)""".strip(),
    "demand_info": """
demand_info (
  project_id      BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  demand_approved DATE NULL,
  approved_by     TEXT,
  last_updated    TIMESTAMP DEFAULT NOW()
)""".strip(),
}

def mmddyyyy_to_iso(s):
    if not s or s == "N/A":
        return None
    try:
        m, d, y = s.split("-")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        return None

def fetch_json(endpoint):
    headers = get_dynamic_headers()
    try:
        r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        if r.status_code == 401:
            # retry once on unauthorized
            headers = get_dynamic_headers()
            r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        elif r.status_code == 429:
            # Handle rate limiting
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"⚠️  Rate limited, sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_json(endpoint)  # Retry after delay
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Request failed for {endpoint}: {e}")
        return {}

def format_date(datestr):
    if not datestr:
        return "N/A"
    try:
        dt = datetime.fromisoformat(datestr[:10])
        return dt.strftime("%m-%d-%Y")
    except:
        return datestr
import re
from typing import Any, Dict, List, Optional

def _pick_vital(
    vitals: List[Dict[str, Any]],
    *,
    friendly_names: Optional[List[str]] = None,
    fieldname_prefixes: Optional[List[str]] = None,
    want_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Find a vital item by friendlyName first, else by fieldName prefixes; optionally constrain by fieldType."""
    candidates = []

    # 1) FriendlyName match (case-insensitive, strip punctuation/colon)
    if friendly_names:
        norm_targets = {re.sub(r"[:\s]+$", "", fn).strip().lower() for fn in friendly_names}
        for it in vitals:
            fn = (it.get("friendlyName") or "").strip()
            norm = re.sub(r"[:\s]+$", "", fn).lower()
            if norm in norm_targets:
                candidates.append(it)

    # 2) fieldName prefix match (e.g., 'sumOfamountbilled*')
    if not candidates and fieldname_prefixes:
        for it in vitals:
            name = (it.get("fieldName") or "")
            for pref in fieldname_prefixes:
                base = pref.rstrip("*")
                if name.startswith(base):
                    candidates.append(it)
                    break

    # 3) Narrow by type if requested
    if want_types and candidates:
        typed = [it for it in candidates if (it.get("fieldType") or "").lower() in {t.lower() for t in want_types}]
        if typed:
            candidates = typed

    # Return the "best" candidate (first is fine; they are ordered by position)
    return candidates[0] if candidates else None

def sol_dol_meds_policy_limits(pid: int) -> Dict[str, Any]:
    vitals = fetch_json(f"/core/projects/{pid}/vitals") or []

    def get_val(item: Optional[Dict[str, Any]]) -> Any:
        if not item:
            return "N/A"
        ft = (item.get("fieldType") or "").lower()
        val = item.get("value")
        if val in (None, ""):
            return "N/A"
        if ft == "dateonly":
            return format_date(val)
        if ft in ("currency", "decimal", "number"):
            try:
                return float(str(val))
            except Exception:
                return val
        return val

    out: Dict[str, Any] = {}

    # SOL (date)
    out["SOL"] = get_val(_pick_vital(
        vitals,
        friendly_names=["SOL"],
        fieldname_prefixes=["sol", "solDue", "sol18747Due"],  # broad prefixes are fine
        want_types=["DateOnly"]
    ))

    # DOL / Incident Date (date)
    out["DOL"] = get_val(_pick_vital(
        vitals,
        friendly_names=["DOL", "DOL:", "Incident Date"],
        fieldname_prefixes=["incidentDate"],
        want_types=["DateOnly"]
    ))

    # Personal Injury Type
    out["Personal Injury Type"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Personal Injury Type"],
        fieldname_prefixes=["personalinjurytype"]
    ))

    # Total Meds (currency/number) — THIS FIXES YOUR ISSUE
    out["Total Meds"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Total Meds", "Total Meds:"],
        fieldname_prefixes=["sumOfamountbilled"],  # << no numeric suffix
        want_types=["Currency", "Decimal", "Number"]
    ))

    # Policy Limits
    out["Policy Limits"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Policy Limits", "Policy Limits:"],
        fieldname_prefixes=["policylimits"]
    ))

    # Liability Decision
    out["Liability Decision"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Liability Decision", "Liability Decision:"],
        fieldname_prefixes=["liabilitydecision"]
    ))

    # Last Offer
    out["Last Offer"] = get_val(_pick_vital(
        vitals,
        friendly_names=["Last Offer", "Last Offer:"],
        fieldname_prefixes=["lastoffer"]
    ))

    # Optional: debug which fields were picked
    # print(json.dumps(vitals, indent=2))
    # print("Resolved vitals:", out)

    return out

def get_intake_date(pid):
    project_data = fetch_json(f"/core/projects/{pid}")
    if not project_data:
        return "N/A"
    project_type_code = project_data.get("projectTypeCode", "").strip()
    print(project_type_code)
    endpoint_mapping = {
        "PIMaster": "intake2",
        "LOJE 2.0": "lOJEIntake20Demo",
        "LOJE 2.2": "lOJEIntake20Demo",
        "WC": "wCIntake"
    }
    endpoint = endpoint_mapping.get(project_type_code, "lOJEIntake20Demo")
    print(endpoint)
    data = fetch_json(f"/core/projects/{pid}/Forms/{endpoint}")
    return format_date(data.get("dateOfIntake")) if isinstance(data, dict) else "N/A"

def get_case_summary_sol(pid):
    data = fetch_json(f"/core/projects/{pid}/Forms/caseSummary")
    if not isinstance(data, dict):
        return "N/A"
    sol_section = data.get("sOL", {}) or {}
    date_val   = sol_section.get("dateValue")
    return format_date(date_val) if date_val else "N/A"

def get_nego_info(pid):
    d = fetch_json(f"/core/projects/{pid}/Forms/negotiation") or {}
    if not isinstance(d, dict):
        return {
            "negotiator": "N/A",
            "settlement_date": None,
            "settled": "N/A",
            "settled_amount": None,
            "last_offer": "N/A",
            "last_offer_date": None,
            "date_assigned_to_nego": None
        }
    nego = d.get("negoAssignedTo", {}) or {}
    return {
        "negotiator": nego.get("fullname", "N/A"),
        "settlement_date": format_date(d.get("settlementDate")),
        "settled": d.get("settled", "N/A"),
        "settled_amount": d.get("settledAmount"),
        "last_offer": d.get("lastOffer", "N/A"),
        "last_offer_date": format_date(d.get("lastOfferDate")),
        "date_assigned_to_nego": format_date(d.get("dateAssignedToNego"))
    }

def get_insurance(pid):
    d = fetch_json(f"/core/projects/{pid}/Forms/demandPrep") or {}
    di, ci = d.get("defendantInsurance", {}) or {}, d.get("clientsInsuranceCompany", {}) or {}
    return {"def_name": di.get("fullname", "N/A"), "cli_name": ci.get("fullname", "N/A")}

def get_notes(project_id):
    headers = get_dynamic_headers()
    all_notes, offset, limit = [], 0, 50
    while True:
        url = f"{API_BASE_URL}/core/projects/{project_id}/notes?offset={offset}&limit={limit}"
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 401:
            headers = get_dynamic_headers()
            r = requests.get(url, headers=headers, timeout=30)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            print(f"⚠️  Failed to fetch notes for project {project_id} at offset {offset}: {e}")
            break
        data = r.json()
        items = data.get("items", [])
        if not items:
            break
        all_notes.extend(items)
        if not data.get("links", {}).get("next"):
            break
        offset += limit
    return all_notes

def analyze_notes(notes):
    total_contacts, latest_date = 0, None
    for note in notes:
        if note.get("typeTag") != "note":
            continue
        text = (note.get("subject") or "") + " " + (note.get("body") or "")
        if EMAIL_PATTERN.search(text) or not COMM_KEYWORDS.search(text):
            continue
        ca = note.get("createdAt")
        if not ca:
            continue
        try:
            dt = datetime.fromisoformat(ca.replace("Z", "+00:00"))
        except ValueError:
            continue
        total_contacts += 1
        if latest_date is None or dt > latest_date:
            latest_date = dt
    return total_contacts, latest_date

def get_client_contact_metrics(pid):
    notes = get_notes(pid)
    return analyze_notes(notes)

def get_breakdown(pid):
    d = fetch_json(f"/core/projects/{pid}/Forms/breakdown") or {}
    ln = d.get("lienNegotiatorAssignedTo") or {}

    def fmt(raw):
        if not raw or raw == "N/A":
            return None
        try:
            return datetime.fromisoformat(raw[:10]).strftime("%m-%d-%Y")
        except:
            return None

    return {
        "lien_name": ln.get("fullname", "N/A"),
        "lien_company": ln.get("fromCompany", "N/A"),
        "lien_title": ln.get("jobTitle", "N/A"),
        "lien_dept": ln.get("department", "N/A"),
        "date_assigned": fmt(d.get("dateAssignedToBreakdown")),
        "date_completed": fmt(d.get("dateCompleted"))
    }

def get_lit_review(pid):
    d = fetch_json(f"/core/projects/{pid}/Forms/litCaseReview2") or {}

    def fmt(raw):
        if not raw:
            return "N/A"
        try:
            return datetime.fromisoformat(raw[:10]).strftime("%m-%d-%Y")
        except:
            return raw

    return {
        "trial_date": fmt(d.get("trialDate")),
        "date_complaint_filed": fmt(d.get("dateComplainWasFiled")),
        "date_attorney_assigned": fmt(d.get("dateAttorneyWasAssigned")),
        "settlement_amount": d.get("settlementAmount") or "N/A",
        "settlement_date": fmt(d.get("settlementDate")),
        "dismissal_filed_on": fmt(d.get("dismissalFiledOn"))
    }

def get_demand_info(pid):
    d = fetch_json(f"/core/projects/{pid}/Forms/demand") or {}
    raw = d.get("demandApproved")
    return (format_date(raw) if isinstance(raw, str) else None, d.get("approvedBy"))

def get_project_teams(pid):
    data = fetch_json(f"/core/projects/{pid}/teams") or {}
    if isinstance(data, list):
        return data
    for key in ("teams", "data", "team", "results"):
        if isinstance(data.get(key), list):
            return data[key]
    return []

def get_team_members(team_id):
    data = fetch_json(f"/core/teams/{team_id}") or {}
    for key in ("teamMembers","members","data","results"):
        if isinstance(data.get(key), list):
            return data[key]
    return []

def get_relevant_team_members(pid):
    result, found = [], set()
    for team in get_project_teams(pid):
        tid = team["id"]["native"]
        if team.get("name") == "Default Team" or tid in {0, 240, 242, 2014, 305, 1731, 1584, 1380}:
            continue
        for m in get_team_members(tid):
            full = m.get("fullname","N/A").strip()
            email = m.get("email","N/A").strip()
            roles = {r.get("name","") for r in m.get("teamRoles",[])}
            for role in roles & TARGET_ROLES:
                found.add(role)
                result.append({"full_name": full, "email": email, "role": role})
    for role in TARGET_ROLES - found:
        result.append({"full_name":"N/A","email":"N/A","role":role})
    return result

def get_projects_by_type(code: str, limit: Optional[int] = None) -> List[int]:
    """
    Fetch all project IDs of a given Filevine projectTypeCode.
    If `limit` is None, will page until no more items; otherwise stops at `limit`.
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
                params={"offset": offset, "limit": page_sz, "projectTypeCode": code},
                timeout=30,
            )
            if resp.status_code == 401:
                headers = get_dynamic_headers()
                resp = requests.get(
                    f"{API_BASE_URL}/core/projects",
                    headers=headers,
                    params={"offset": offset, "limit": page_sz, "projectTypeCode": code},
                    timeout=30,
                )
            resp.raise_for_status()

            items = resp.json().get("items", [])
            if not items:
                break

            for pj in items:
                pid = pj["projectId"]["native"]
                projects.append(pid)
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
            backoff = 2 ** attempts
            print(f"⚠️  Error fetching projects (attempt {attempts}), retrying in {backoff}s: {e}")
            time.sleep(backoff)

    if limit is not None and len(projects) < limit:
        print(f"⚠️  Warning: Only found {len(projects)}/{limit} projects of type '{code}'")

    return projects if limit is None else projects[:limit]

# ---------- Print utilities ----------
def _serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def print_table_with_schema(table_name: str, row: Dict):
    print("\n" + "="*88)
    print(f"TABLE: {table_name}")
    print("-"*88)
    print(TABLE_SCHEMAS.get(table_name, "<schema not found>"))
    print("-"*88)
    # Pretty-print values that would be inserted
    print("ROW DATA:")
    print(json.dumps({k: _serialize(v) for k, v in row.items()}, indent=2, default=str))

# ---------- Main project loader (print-only) ----------
def load_project_print_only(pid: int):
    print(f"\n🔍 Processing project {pid}")

    pj = fetch_json(f"/core/projects/{pid}") or {}
    if not pj:
        print(f"⚠️  Could not fetch project {pid}")
        return

    print(f"⏳ Loading {pid} – {pj.get('projectOrClientName','<no name>')} ...")

    vitals = sol_dol_meds_policy_limits(pid) or {}
    nego = get_nego_info(pid) or {}
    ins = get_insurance(pid) or {"def_name": "N/A", "cli_name": "N/A"}
    br = get_breakdown(pid) or {}
    lit = get_lit_review(pid) or {}
    demand_dt, demand_by = get_demand_info(pid)
    contact_count, contact_latest = get_client_contact_metrics(pid)
    project_type_code = pj.get("projectTypeCode")
    team_members = get_relevant_team_members(pid)
    role_map = {m["role"]: m["full_name"] for m in team_members}
    total_meds_val = vitals.get("Total Meds")

    new_data = {
        "projects": {
            "project_id": pj["projectId"]["native"],
            "project_name": pj.get("projectOrClientName", "N/A"),
            "phase_name": pj.get("phaseName"),
            "incident_date": mmddyyyy_to_iso(format_date(pj.get("incidentDate"))) if pj.get("incidentDate") else None,
            "sol_due_date": mmddyyyy_to_iso(get_case_summary_sol(pid)) if get_case_summary_sol(pid) != "N/A" else None,
            # "total_meds": float(vitals.get("Total Meds")) if vitals.get("Total Meds") not in [None, "N/A"] else None,
            "total_meds" : float(total_meds_val) if isinstance(total_meds_val, (int, float, str)) and str(total_meds_val) not in ("", "N/A", "None") else None,
            "policy_limits": vitals.get("Policy Limits", "N/A"),
            "personal_injury_type": vitals.get("Personal Injury Type", "N/A"),
            "liability_decision": vitals.get("Liability Decision", "N/A"),
            "last_offer": vitals.get("Last Offer", "N/A"),
            "date_of_incident": mmddyyyy_to_iso(get_intake_date(pid)) if get_intake_date(pid) != "N/A" else None,
            "client_contact_count": contact_count,
            "latest_client_contact": contact_latest,
            "project_type_code": project_type_code,
            "last_updated": "NOW()"  # informational only
        },
        "negotiation": {
            "project_id": pid,
            "negotiator": nego.get("negotiator", "N/A"),
            "settlement_date": mmddyyyy_to_iso(nego.get("settlement_date")) if nego.get("settlement_date") not in ["N/A", None] else None,
            "settled": nego.get("settled", "N/A"),
            "settled_amount": float(nego.get("settled_amount")) if nego.get("settled_amount") not in [None, "N/A"] else None,
            "last_offer": nego.get("last_offer", "N/A"),
            "last_offer_date": mmddyyyy_to_iso(nego.get("last_offer_date")) if nego.get("last_offer_date") not in ["N/A", None] else None,
            "date_assigned_to_nego": mmddyyyy_to_iso(nego.get("date_assigned_to_nego")) if nego.get("date_assigned_to_nego") not in ["N/A", None] else None,
            "last_updated": "NOW()"
        },
        "insurance_info": {
            "project_id": pid,
            "defendant_insurance_name": ins.get("def_name", "N/A"),
            "client_insurance_name": ins.get("cli_name", "N/A"),
            "last_updated": "NOW()"
        },
        "breakdown_info": {
            "project_id": pid,
            "lien_negotiator_name": br.get("lien_name", "N/A"),
            "lien_negotiator_company": br.get("lien_company", "N/A"),
            "lien_negotiator_title": br.get("lien_title", "N/A"),
            "lien_negotiator_dept": br.get("lien_dept", "N/A"),
            "date_assigned": mmddyyyy_to_iso(br.get("date_assigned")) if br.get("date_assigned") not in ["N/A", None] else None,
            "date_completed": mmddyyyy_to_iso(br.get("date_completed")) if br.get("date_completed") not in ["N/A", None] else None,
            "last_updated": "NOW()"
        },
        "lit_case_review": {
            "project_id": pid,
            "trial_date": mmddyyyy_to_iso(lit.get("trial_date")) if lit.get("trial_date") not in ["N/A", None] else None,
            "date_complaint_filed": mmddyyyy_to_iso(lit.get("date_complaint_filed")) if lit.get("date_complaint_filed") not in ["N/A", None] else None,
            "date_attorney_assigned": mmddyyyy_to_iso(lit.get("date_attorney_assigned")) if lit.get("date_attorney_assigned") not in ["N/A", None] else None,
            "settlement_amount": lit.get("settlement_amount", "N/A"),
            "settlement_date": mmddyyyy_to_iso(lit.get("settlement_date")) if lit.get("settlement_date") not in ["N/A", None] else None,
            "dismissal_filed_on": mmddyyyy_to_iso(lit.get("dismissal_filed_on")) if lit.get("dismissal_filed_on") not in ["N/A", None] else None,
            "last_updated": "NOW()"
        },
        "demand_info": {
            "project_id": pid,
            "demand_approved": mmddyyyy_to_iso(demand_dt) if demand_dt and demand_dt != "N/A" else None,
            "approved_by": demand_by or "N/A",
            "last_updated": "NOW()"
        },
        "contacts": {
            "project_id": pid,
            "case_manager": role_map.get("Case Manager", "N/A"),
            "supervisor": role_map.get("Supervisor", "N/A"),
            "attorney": role_map.get("Attorney", "N/A"),
            "paralegal": role_map.get("Paralegal", "N/A"),
            "last_updated": "NOW()"
        }
    }

    # Print everything
    for table_name in ("projects", "contacts", "negotiation", "insurance_info",
                       "breakdown_info", "lit_case_review", "demand_info"):
        print_table_with_schema(table_name, new_data[table_name])

def main():
    print("🚀 Starting print-only project data loader...")

    # 👇 Hardcode here for testing
    project_ids = [1699079]   # replace with your specific project_id

    print(f"📊 Total projects to process: {len(project_ids)}")

    for pid in project_ids:
        load_project_print_only(pid)

    print("🎉 Done!")

if __name__ == "__main__":
    main()
