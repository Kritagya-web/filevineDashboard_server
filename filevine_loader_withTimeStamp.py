import time
import requests
from datetime import datetime
from auth_refresh import get_dynamic_headers
import re
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Tuple

# --- Configuration ---
API_BASE_URL   = "https://calljacob.api.filevineapp.com"
COMM_KEYWORDS  = re.compile(r"\b(spoke|call|text|message|vm)\b", re.IGNORECASE)
EMAIL_PATTERN  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
TARGET_ROLES   = {"Case Manager", "Supervisor", "Paralegal", "Attorney"}

DB_USER     = "postgres"
DB_PASSWORD = "kritagya"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "postgres"
DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL, echo=True)  # Changed to True for debugging

def mmddyyyy_to_iso(s):
    if not s or s == "N/A":
        return None
    try:
        m, d, y = s.split("-")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        return None

# --- DDL ---
DDL = """
CREATE TABLE IF NOT EXISTS projects (
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
);

CREATE TABLE IF NOT EXISTS negotiation (
  project_id            BIGINT    PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  negotiator            TEXT,
  settlement_date       DATE NULL,
  settled               TEXT,
  settled_amount        NUMERIC(14,2),
  last_offer            TEXT,
  last_offer_date       DATE NULL,
  date_assigned_to_nego DATE NULL,
  last_updated          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS insurance_info (
  project_id                BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  defendant_insurance_name  TEXT,
  client_insurance_name     TEXT,
  last_updated              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS breakdown_info (
  project_id              BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  lien_negotiator_name    TEXT,
  lien_negotiator_company TEXT,
  lien_negotiator_title   TEXT,
  lien_negotiator_dept    TEXT,
  date_assigned           DATE NULL, 
  date_completed          DATE NULL,
  last_updated            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lit_case_review (
  project_id               BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  trial_date               DATE NULL,
  date_complaint_filed     DATE NULL,
  date_attorney_assigned   DATE NULL,
  settlement_amount        TEXT,
  settlement_date          DATE NULL,
  dismissal_filed_on       DATE NULL,
  last_updated             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS contacts (
  project_id    BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  case_manager  TEXT,
  supervisor    TEXT,
  attorney      TEXT,
  paralegal     TEXT,
  last_updated  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS demand_info (
  project_id      BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  demand_approved DATE NULL,
  approved_by     TEXT,
  last_updated    TIMESTAMP DEFAULT NOW()
);
"""

# Initialize database
def initialize_database():
    """Initialize database tables"""
    try:
        with engine.begin() as conn:
            for stmt in DDL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        print("✅ Database tables initialized successfully")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        raise

# Call this at startup
initialize_database()

def fetch_json(endpoint):
    headers = get_dynamic_headers()
    try:
        r = requests.get(API_BASE_URL + endpoint, headers=headers)
        if r.status_code == 401:
            # retry once on unauthorized
            headers = get_dynamic_headers()
            r = requests.get(API_BASE_URL + endpoint, headers=headers)
        elif r.status_code == 429:
            # Handle rate limiting
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"⚠️ Rate limited, sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_json(endpoint)  # Retry after delay
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request failed for {endpoint}: {e}")
        return {}

def format_date(datestr):
    if not datestr:
        return "N/A"
    try:
        dt = datetime.fromisoformat(datestr[:10])
        return dt.strftime("%m-%d-%Y")
    except:
        return datestr

def sol_dol_meds_policy_limits(pid):
    vitals = fetch_json(f"/core/projects/{pid}/vitals") or []
    mapping = {
        "SOL": "sol18747Due",
        "Policy Limits": "policylimits36383",
        "DOL": "incidentDate",
        "Personal Injury Type": "personalinjurytype36397",
        "Total Meds": "sumOfamountbilled36399",
        "Liability Decision": "liabilitydecision36383",
        "Last Offer": "lastoffer36401",
    }
    out = {label: "N/A" for label in mapping}
    for item in vitals:
        fn = item.get("fieldName")
        val = item.get("value")
        ft = item.get("fieldType")
        for label, field in mapping.items():
            if fn == field:
                out[label] = format_date(val) if ft == "DateOnly" else val or "N/A"
    return out

def get_intake_date(pid):
    project_data = fetch_json(f"/core/projects/{pid}")
    if not project_data:
        return "N/A"
    
    project_type_code = project_data.get("projectTypeCode", "").strip()
    
    endpoint_mapping = {
        "PIMaster": "intake2",
        "LOJE 2.0": "lOJEIntake20Demo",
        "LOJE 2.2": "lOJEIntake20Demo",
        "WC": "wCIntake"
    }
    
    endpoint = endpoint_mapping.get(project_type_code, "lOJEIntake20Demo")
    
    try:
        data = fetch_json(f"/core/projects/{pid}/Forms/{endpoint}")
        return format_date(data.get("incidentDate_1")) if isinstance(data, dict) else "N/A"
    except requests.HTTPError as e:
        print(f"⚠️ Intake form error for project {pid} (type: {project_type_code}): {e}")
        return "N/A"

def get_case_summary_sol(pid):
    try:
        data = fetch_json(f"/core/projects/{pid}/Forms/caseSummary")
    except requests.HTTPError as e:
        print(f"⚠️ CaseSummary form error for {pid}: {e}")
        return "N/A"

    if not isinstance(data, dict):
        return "N/A"

    sol_section = data.get("sOL", {})
    date_val   = sol_section.get("dateValue")
    if not date_val:
        return "N/A"

    return format_date(date_val)

def get_nego_info(pid):
    try:
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
        
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"⚠️ Negotiation form not found for {pid}")
        else:
            print(f"⚠️ Negotiation form error for {pid}: {e}")
        return {
            "negotiator": "N/A",
            "settlement_date": None,
            "settled": "N/A",
            "settled_amount": None,
            "last_offer": "N/A",
            "last_offer_date": None,
            "date_assigned_to_nego": None
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
        r = requests.get(url, headers=headers)
        if r.status_code == 401:
            headers = get_dynamic_headers()
            r = requests.get(url, headers=headers)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            print(f"⚠️ Failed to fetch notes for project {project_id} at offset {offset}: {e}")
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
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/breakdown") or {}
    except requests.HTTPError as e:
        print(f"⚠️ Breakdown form error for {pid}: {e}")
        d = {}
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
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/litCaseReview2") or {}
    except requests.HTTPError as e:
        print(f"⚠️ LitCaseReview form error for {pid}: {e}")
        d = {}
    def fmt(raw):
        if not raw: return "N/A"
        try: return datetime.fromisoformat(raw[:10]).strftime("%m-%d-%Y")
        except: return raw
    return {
        "trial_date": fmt(d.get("trialDate")),
        "date_complaint_filed": fmt(d.get("dateComplainWasFiled")),
        "date_attorney_assigned": fmt(d.get("dateAttorneyWasAssigned")),
        "settlement_amount": d.get("settlementAmount") or "N/A",
        "settlement_date": fmt(d.get("settlementDate")),
        "dismissal_filed_on": fmt(d.get("dismissalFiledOn"))
    }

def get_demand_info(pid):
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/demand") or {}
    except requests.HTTPError as e:
        print(f"⚠️ Demand form error for {pid}: {e}")
        return None, None
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

def detect_changes(current_data: Dict, new_data: Dict) -> Tuple[bool, Dict]:
    """
    Compare current and new data, return (has_changes, changes_dict)
    changes_dict format: {field_name: (old_value, new_value)}
    """
    changes = {}
    for field in new_data:  # Changed: iterate over new_data keys
        if field in ['last_updated']:  # Skip timestamp field
            continue
            
        old_val = current_data.get(field)  # Use get() with None default
        new_val = new_data[field]
        
        # Special handling for numeric fields
        if field == 'settled_amount' or field.endswith('_amount'):
            old_val = float(old_val) if old_val not in [None, "N/A", ""] else None
            new_val = float(new_val) if new_val not in [None, "N/A", ""] else None
        
        # Convert None to "N/A" for string comparison consistency
        old_str = str(old_val) if old_val is not None else "None"
        new_str = str(new_val) if new_val is not None else "None"
        
        if old_str != new_str:
            changes[field] = (old_val, new_val)
    
    return (bool(changes), changes)

# --- Fixed UPSERT statements ---
UPSERT_PROJECT = text("""
INSERT INTO projects(
  project_id, project_name, phase_name, incident_date,
  sol_due_date, total_meds, policy_limits, personal_injury_type,
  liability_decision, last_offer, date_of_incident,
  client_contact_count, latest_client_contact, project_type_code,
  last_updated
) VALUES (
  :project_id, :project_name, :phase_name, :incident_date,
  :sol_due_date, :total_meds, :policy_limits, :personal_injury_type,
  :liability_decision, :last_offer, :date_of_incident,
  :client_contact_count, :latest_client_contact, :project_type_code,
  NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  project_name           = EXCLUDED.project_name,
  phase_name             = EXCLUDED.phase_name,
  incident_date          = EXCLUDED.incident_date,
  sol_due_date           = EXCLUDED.sol_due_date,
  total_meds             = EXCLUDED.total_meds,
  policy_limits          = EXCLUDED.policy_limits,
  personal_injury_type   = EXCLUDED.personal_injury_type,
  liability_decision     = EXCLUDED.liability_decision,
  last_offer             = EXCLUDED.last_offer,
  date_of_incident       = EXCLUDED.date_of_incident,
  client_contact_count   = EXCLUDED.client_contact_count,
  latest_client_contact  = EXCLUDED.latest_client_contact,
  project_type_code      = EXCLUDED.project_type_code,
  last_updated           = NOW()
""")

UPSERT_NEGOTIATION = text("""
INSERT INTO negotiation(
  project_id, negotiator, settlement_date, settled,
  settled_amount, last_offer, last_offer_date, date_assigned_to_nego,
  last_updated
) VALUES (
  :project_id, :negotiator, :settlement_date, :settled,
  :settled_amount, :last_offer, :last_offer_date, :date_assigned_to_nego,
  NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  negotiator            = EXCLUDED.negotiator,
  settlement_date       = EXCLUDED.settlement_date,
  settled               = EXCLUDED.settled,
  settled_amount        = EXCLUDED.settled_amount,
  last_offer            = EXCLUDED.last_offer,
  last_offer_date       = EXCLUDED.last_offer_date,
  date_assigned_to_nego = EXCLUDED.date_assigned_to_nego,
  last_updated          = NOW()
""")

UPSERT_INSURANCE = text("""
INSERT INTO insurance_info(
  project_id, defendant_insurance_name, client_insurance_name, last_updated
) VALUES (
  :project_id, :defendant_insurance_name, :client_insurance_name, NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  defendant_insurance_name = EXCLUDED.defendant_insurance_name,
  client_insurance_name    = EXCLUDED.client_insurance_name,
  last_updated            = NOW()
""")

UPSERT_BREAKDOWN = text("""
INSERT INTO breakdown_info(
  project_id, lien_negotiator_name, lien_negotiator_company,
  lien_negotiator_title, lien_negotiator_dept, date_assigned, date_completed,
  last_updated
) VALUES (
  :project_id, :lien_negotiator_name, :lien_negotiator_company,
  :lien_negotiator_title, :lien_negotiator_dept, :date_assigned, :date_completed,
  NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  lien_negotiator_name    = EXCLUDED.lien_negotiator_name,
  lien_negotiator_company = EXCLUDED.lien_negotiator_company,
  lien_negotiator_title   = EXCLUDED.lien_negotiator_title,
  lien_negotiator_dept    = EXCLUDED.lien_negotiator_dept,
  date_assigned           = EXCLUDED.date_assigned,
  date_completed          = EXCLUDED.date_completed,
  last_updated           = NOW()
""")

UPSERT_LIT = text("""
INSERT INTO lit_case_review(
  project_id, trial_date, date_complaint_filed,
  date_attorney_assigned, settlement_amount, settlement_date,
  dismissal_filed_on, last_updated
) VALUES (
  :project_id, :trial_date, :date_complaint_filed,
  :date_attorney_assigned, :settlement_amount, :settlement_date,
  :dismissal_filed_on, NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  trial_date             = EXCLUDED.trial_date,
  date_complaint_filed   = EXCLUDED.date_complaint_filed,
  date_attorney_assigned = EXCLUDED.date_attorney_assigned,
  settlement_amount      = EXCLUDED.settlement_amount,
  settlement_date        = EXCLUDED.settlement_date,
  dismissal_filed_on     = EXCLUDED.dismissal_filed_on,
  last_updated          = NOW()
""")

UPSERT_DEMAND = text("""
INSERT INTO demand_info(
  project_id, demand_approved, approved_by, last_updated
) VALUES (
  :project_id, :demand_approved, :approved_by, NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  demand_approved = EXCLUDED.demand_approved,
  approved_by     = EXCLUDED.approved_by,
  last_updated   = NOW()
""")

UPSERT_CONTACTS = text("""
INSERT INTO contacts(
  project_id, case_manager, supervisor,
  attorney, paralegal, last_updated
) VALUES (
  :project_id, :case_manager, :supervisor,
  :attorney, :paralegal, NOW()
)
ON CONFLICT (project_id) DO UPDATE SET
  case_manager = EXCLUDED.case_manager,
  supervisor   = EXCLUDED.supervisor,
  attorney     = EXCLUDED.attorney,
  paralegal    = EXCLUDED.paralegal,
  last_updated = NOW()
""")

def get_projects_by_type(code: str, limit: Optional[int] = None) -> List[int]:
    """
    Fetch all project IDs of a given FileVine projectTypeCode.
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
                params={
                    "offset": offset,
                    "limit": page_sz,
                    "projectTypeCode": code,
                },
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
                print(f"⚠️ Giving up after {attempts} failed attempts: {e}")
                break
            backoff = 2 ** attempts
            print(f"⚠️ Error fetching projects (attempt {attempts}), retrying in {backoff}s: {e}")
            time.sleep(backoff)

    if limit is not None and len(projects) < limit:
        print(f"⚠️ Warning: Only found {len(projects)}/{limit} projects of type '{code}'")

    return projects if limit is None else projects[:limit]

def load_project(pid):
    try:
        print(f"\n🔍 Processing project {pid}")
        
        # Fetch current data from DB
        with engine.connect() as conn:
            tables = [
                "projects", "contacts", "negotiation", 
                "insurance_info", "breakdown_info", 
                "lit_case_review", "demand_info"
            ]
            
            current_data = {}
            for table in tables:
                result = conn.execute(
                    text(f"SELECT * FROM {table} WHERE project_id = :pid"),
                    {"pid": pid}
                ).fetchone()
                current_data[table] = dict(result._asdict()) if result else {}

        # Fetch basic project info
        pj = fetch_json(f"/core/projects/{pid}") or {}
        if not pj:
            print(f"⚠️ Could not fetch project {pid}")
            return

        print(f"⏳ Loading {pid} – {pj.get('projectOrClientName','<no name>')} ...")

        # Get all data with error handling
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

        # Prepare new data for all tables with proper field names
        new_data = {
            "projects": {
                "project_id": pj["projectId"]["native"],
                "project_name": pj.get("projectOrClientName", "N/A"),
                "phase_name": pj.get("phaseName"),
                "incident_date": mmddyyyy_to_iso(format_date(pj.get("incidentDate"))) if pj.get("incidentDate") else None,
                "sol_due_date": mmddyyyy_to_iso(get_case_summary_sol(pid)) if get_case_summary_sol(pid) != "N/A" else None,
                "total_meds": float(vitals.get("Total Meds")) if vitals.get("Total Meds") not in [None, "N/A"] else None,
                "policy_limits": vitals.get("Policy Limits", "N/A"),
                "personal_injury_type": vitals.get("Personal Injury Type", "N/A"),
                "liability_decision": vitals.get("Liability Decision", "N/A"),
                "last_offer": vitals.get("Last Offer", "N/A"),
                "date_of_incident": mmddyyyy_to_iso(get_intake_date(pid)) if get_intake_date(pid) != "N/A" else None,
                "client_contact_count": contact_count,
                "latest_client_contact": contact_latest,
                "project_type_code": project_type_code
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
            },
            "insurance_info": {
                "project_id": pid,
                "defendant_insurance_name": ins.get("def_name", "N/A"),
                "client_insurance_name": ins.get("cli_name", "N/A")
            },
            "breakdown_info": {
                "project_id": pid,
                "lien_negotiator_name": br.get("lien_name", "N/A"),
                "lien_negotiator_company": br.get("lien_company", "N/A"),
                "lien_negotiator_title": br.get("lien_title", "N/A"),
                "lien_negotiator_dept": br.get("lien_dept", "N/A"),
                "date_assigned": mmddyyyy_to_iso(br.get("date_assigned")) if br.get("date_assigned") not in ["N/A", None] else None,
                "date_completed": mmddyyyy_to_iso(br.get("date_completed")) if br.get("date_completed") not in ["N/A", None] else None
            },
            "lit_case_review": {
                "project_id": pid,
                "trial_date": mmddyyyy_to_iso(lit.get("trial_date")) if lit.get("trial_date") not in ["N/A", None] else None,
                "date_complaint_filed": mmddyyyy_to_iso(lit.get("date_complaint_filed")) if lit.get("date_complaint_filed") not in ["N/A", None] else None,
                "date_attorney_assigned": mmddyyyy_to_iso(lit.get("date_attorney_assigned")) if lit.get("date_attorney_assigned") not in ["N/A", None] else None,
                "settlement_amount": lit.get("settlement_amount", "N/A"),
                "settlement_date": mmddyyyy_to_iso(lit.get("settlement_date")) if lit.get("settlement_date") not in ["N/A", None] else None,
                "dismissal_filed_on": mmddyyyy_to_iso(lit.get("dismissal_filed_on")) if lit.get("dismissal_filed_on") not in ["N/A", None] else None
            },
            "demand_info": {
                "project_id": pid,
                "demand_approved": mmddyyyy_to_iso(demand_dt) if demand_dt and demand_dt != "N/A" else None,
                "approved_by": demand_by or "N/A"
            },
            "contacts": {
                "project_id": pid,
                "case_manager": role_map.get("Case Manager", "N/A"),
                "supervisor": role_map.get("Supervisor", "N/A"),
                "attorney": role_map.get("Attorney", "N/A"),
                "paralegal": role_map.get("Paralegal", "N/A")
            }
        }

        # Compare data across all tables
        all_changes = {}
        has_any_changes = False
        is_new_project = not current_data.get("projects")  # Check if project exists
        
        for table in tables:
            current = current_data.get(table, {})
            new = new_data.get(table, {})
            has_changes, changes = detect_changes(current, new)
            
            if has_changes or is_new_project:
                all_changes[table] = changes
                has_any_changes = True

        if not has_any_changes and not is_new_project:
            print(f"✅ Project {pid}: No changes detected")
            return

        # Print changes or new project status
        if is_new_project:
            print(f"🆕 Project {pid}: New project - inserting all data")
        else:
            print(f"🔄 Project {pid}: Changes detected in {len(all_changes)} table(s)")
            for table, changes in all_changes.items():
                if changes:  # Only print if there are actual changes
                    print(f"  {table.replace('_', ' ').title()} changes:")
                    for field, (old_val, new_val) in changes.items():
                        print(f"    - {field}: {old_val} → {new_val}")

        # Execute database operations
        with engine.begin() as conn:
            try:
                # Always insert/update projects first (parent table)
                if "projects" in all_changes or is_new_project:
                    conn.execute(UPSERT_PROJECT, new_data["projects"])
                    print(f"  ✅ Updated projects table")
                
                # Then update child tables
                if "negotiation" in all_changes or is_new_project:
                    conn.execute(UPSERT_NEGOTIATION, new_data["negotiation"])
                    print(f"  ✅ Updated negotiation table")
                
                if "insurance_info" in all_changes or is_new_project:
                    conn.execute(UPSERT_INSURANCE, new_data["insurance_info"])
                    print(f"  ✅ Updated insurance_info table")
                
                if "breakdown_info" in all_changes or is_new_project:
                    conn.execute(UPSERT_BREAKDOWN, new_data["breakdown_info"])
                    print(f"  ✅ Updated breakdown_info table")
                
                if "lit_case_review" in all_changes or is_new_project:
                    conn.execute(UPSERT_LIT, new_data["lit_case_review"])
                    print(f"  ✅ Updated lit_case_review table")
                
                if "demand_info" in all_changes or is_new_project:
                    conn.execute(UPSERT_DEMAND, new_data["demand_info"])
                    print(f"  ✅ Updated demand_info table")
                
                if "contacts" in all_changes or is_new_project:
                    conn.execute(UPSERT_CONTACTS, new_data["contacts"])
                    print(f"  ✅ Updated contacts table")

                print(f"💾 Project {pid}: Successfully processed all tables")

            except Exception as db_error:
                print(f"❌ Database error for project {pid}: {db_error}")
                raise

    except Exception as e:
        print(f"❌ Failed to process project {pid}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def test_database_connection():
    """Test database connection and basic operations"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print(f"✅ Database connection successful: {result.fetchone()}")
            
            # Test table existence
            tables_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('projects', 'negotiation', 'insurance_info', 'breakdown_info', 'lit_case_review', 'contacts', 'demand_info')
                ORDER BY table_name
            """)).fetchall()
            
            print(f"✅ Found {len(tables_check)} tables: {[t[0] for t in tables_check]}")
            
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting project data loader...")
    
    # Test database connection first
    if not test_database_connection():
        print("❌ Cannot proceed without database connection")
        exit(1)
    
    # Initialize variables
    project_ids = get_projects_by_type("LOJE 2.0", limit=None)  # Get ALL projects
    print(f"📊 Total projects to process: {len(project_ids)}")
    
    if not project_ids:
        print("⚠️ No projects found!")
        exit(1)
    
    processed_count = 0
    failed_projects = []
    batch_size = 20  # Reasonable batch size for production
    retry_delay = 3  # seconds between batches to be respectful to API
    
    # Process projects in batches
    for i in range(0, len(project_ids), batch_size):
        batch = project_ids[i:i + batch_size]
        print(f"\n📦 Processing batch {i//batch_size + 1} (projects {i+1}-{min(i+batch_size, len(project_ids))})...")
        
        for pid in batch:
            try:
                load_project(pid)
                processed_count += 1
            except Exception as e:
                print(f"❌ Failed to process project {pid}: {str(e)}")
                failed_projects.append(pid)
                continue
        
        # Add delay between batches to be nice to the API
        if i + batch_size < len(project_ids):
            print(f"⏳ Waiting {retry_delay} seconds before next batch...")
            time.sleep(retry_delay)
    
    # Final report
    print("\n" + "="*60)
    print("📈 FINAL SUMMARY:")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"❌ Failed to process: {len(failed_projects)}")
    print(f"📊 Total projects: {len(project_ids)}")
    
    if failed_projects:
        print(f"\n❌ Failed project IDs: {failed_projects}")
    
    print("🎉 Process completed!")