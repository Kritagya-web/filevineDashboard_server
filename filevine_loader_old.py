import time
import requests
from datetime import datetime
from auth_refresh import get_dynamic_headers
import re
from sqlalchemy import create_engine, text
from typing import Optional, List

# --- Configuration ---
API_BASE_URL   = "https://calljacob.api.filevineapp.com"
COMM_KEYWORDS  = re.compile(r"\b(spoke|call|text|message|vm)\b", re.IGNORECASE)
EMAIL_PATTERN  = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
TARGET_ROLES   = {"Case Manager", "Supervisor", "Paralegal", "Attorney"}

DB_USER     = "postgres"
DB_PASSWORD = "kritagya"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "calljacob"
DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL, echo=False)


def mmddyyyy_to_iso(s: Optional[str]) -> Optional[str]:
    if not s or s == "N/A":
        return None
    m, d, y = s.split("-")
    return f"{y}-{m.zfill(2)}-{d.zfill(2)}"


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
  project_type_code      TEXT
);

CREATE TABLE IF NOT EXISTS negotiation (
  project_id            BIGINT    PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  negotiator            TEXT,
  settlement_date       DATE NULL,
  settled               TEXT,
  settled_amount        NUMERIC(14,2),
  last_offer            TEXT,
  last_offer_date       DATE NULL,
  date_assigned_to_nego DATE NULL
);

CREATE TABLE IF NOT EXISTS insurance_info (
  project_id                BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  defendant_insurance_name  TEXT,
  client_insurance_name     TEXT
);

CREATE TABLE IF NOT EXISTS breakdown_info (
  project_id              BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  lien_negotiator_name    TEXT,
  lien_negotiator_company TEXT,
  lien_negotiator_title   TEXT,
  lien_negotiator_dept    TEXT,
  date_assigned           DATE NULL,
  date_completed          DATE NULL
);

CREATE TABLE IF NOT EXISTS lit_case_review (
  project_id               BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  trial_date               DATE NULL,
  date_complaint_filed     DATE NULL,
  date_attorney_assigned   DATE NULL,
  settlement_amount        TEXT,
  settlement_date          DATE NULL,
  dismissal_filed_on       DATE NULL
);

CREATE TABLE IF NOT EXISTS contacts (
  project_id    BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  case_manager  TEXT,
  supervisor    TEXT,
  attorney      TEXT,
  paralegal     TEXT
);

CREATE TABLE IF NOT EXISTS demand_info (
  project_id      BIGINT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  demand_approved DATE NULL,
  approved_by     TEXT
);
"""

with engine.begin() as conn:
    for stmt in DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(text(stmt))


def fetch_json(endpoint: str):
    headers = get_dynamic_headers()
    try:
        r = requests.get(API_BASE_URL + endpoint, headers=headers)
        if r.status_code == 401:
            # retry once on unauthorized
            headers = get_dynamic_headers()
            r = requests.get(API_BASE_URL + endpoint, headers=headers)
        elif r.status_code == 429:
            # Handle rate limiting
            retry_after = int(r.headers.get("Retry-After", 5))
            print(f"⚠️ Rate limited, sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_json(endpoint)  # Retry after delay
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request failed for {endpoint}: {e}")
        return {}


def format_date(datestr: Optional[str]) -> str:
    if not datestr:
        return "N/A"
    try:
        dt = datetime.fromisoformat(datestr[:10])
        return dt.strftime("%m-%d-%Y")
    except Exception:
        return datestr


def sol_dol_meds_policy_limits(pid: int) -> dict:
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
                out[label] = format_date(val) if ft == "DateOnly" else (val or "N/A")
    return out


def get_intake_date(pid: int) -> str:
    try:
        data = fetch_json(f"/core/projects/{pid}/Forms/lOJEIntake20Demo")
        return format_date(data.get("incidentDate_1")) if isinstance(data, dict) else "N/A"
    except requests.HTTPError as e:
        print(f"⚠️ Intake form error for {pid}: {e}")
        return "N/A"


def get_case_summary_sol(pid: int) -> str:
    try:
        data = fetch_json(f"/core/projects/{pid}/Forms/caseSummary")
    except requests.HTTPError as e:
        print(f"⚠️ CaseSummary form error for {pid}: {e}")
        return "N/A"

    if not isinstance(data, dict):
        return "N/A"

    sol_section = data.get("sOL", {})
    date_val = sol_section.get("dateValue")
    if not date_val:
        return "N/A"
    return format_date(date_val)


def get_nego_info(pid: int) -> dict:
    """
    Returns a dict with keys:
    negotiator, settlement_date, settled, settled_amount, last_offer,
    last_offer_date, date_assigned_to_nego
    """
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/negotiation") or {}
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
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
            "date_assigned_to_nego": None,
        }

    negotiator = (d.get("negotiatorAssignedTo") or {}).get("fullname", "N/A")
    return {
        "negotiator": negotiator or "N/A",
        "settlement_date": d.get("settlementDate"),
        "settled": d.get("settled", "N/A"),
        "settled_amount": d.get("settledAmount"),
        "last_offer": d.get("lastOffer", "N/A"),
        "last_offer_date": d.get("lastOfferDate"),
        "date_assigned_to_nego": d.get("dateAssignedToNegotiations"),
    }


def get_insurance(pid: int) -> dict:
    d = fetch_json(f"/core/projects/{pid}/Forms/demandPrep") or {}
    di = d.get("defendantInsurance", {}) or {}
    ci = d.get("clientsInsuranceCompany", {}) or {}
    return {
        "def_name": di.get("fullname", "N/A"),
        "cli_name": ci.get("fullname", "N/A"),
    }


def get_notes(project_id: int) -> list:
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


def analyze_notes(notes: list) -> tuple[int, Optional[datetime]]:
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


def get_client_contact_metrics(pid: int) -> tuple[int, Optional[datetime]]:
    notes = get_notes(pid)
    return analyze_notes(notes)


def get_breakdown(pid: int) -> dict:
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
        except Exception:
            return None

    return {
        "lien_name": ln.get("fullname", "N/A"),
        "lien_company": ln.get("fromCompany", "N/A"),
        "lien_title": ln.get("jobTitle", "N/A"),
        "lien_dept": ln.get("department", "N/A"),
        "date_assigned": fmt(d.get("dateAssignedToBreakdown")),
        "date_completed": fmt(d.get("dateCompleted")),
    }


def get_lit_review(pid: int) -> dict:
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/litCaseReview2") or {}
    except requests.HTTPError as e:
        print(f"⚠️ LitCaseReview form error for {pid}: {e}")
        d = {}

    def fmt(raw):
        if not raw:
            return "N/A"
        try:
            return datetime.fromisoformat(raw[:10]).strftime("%m-%d-%Y")
        except Exception:
            return raw

    return {
        "trial_date": fmt(d.get("trialDate")),
        "date_complaint_filed": fmt(d.get("dateComplainWasFiled")),
        "date_attorney_assigned": fmt(d.get("dateAttorneyWasAssigned")),
        "settlement_amount": d.get("settlementAmount") or "N/A",
        "settlement_date": fmt(d.get("settlementDate")),
        "dismissal_filed_on": fmt(d.get("dismissalFiledOn")),
    }


def get_demand_info(pid: int) -> tuple[Optional[str], Optional[str]]:
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/demand") or {}
    except requests.HTTPError as e:
        print(f"⚠️ Demand form error for {pid}: {e}")
        return None, None
    raw = d.get("demandApproved")
    return (format_date(raw) if isinstance(raw, str) else None, d.get("approvedBy"))


def get_project_teams(pid: int) -> list:
    data = fetch_json(f"/core/projects/{pid}/teams") or {}

    if isinstance(data, list):
        return data

    for key in ("teams", "data", "team", "results"):
        if isinstance(data.get(key), list):
            return data[key]
    return []


def get_team_members(team_id: int) -> list:
    data = fetch_json(f"/core/teams/{team_id}") or {}
    for key in ("teamMembers", "members", "data", "results"):
        if isinstance(data.get(key), list):
            return data[key]
    return []


def get_relevant_team_members(pid: int) -> list[dict]:
    result, found = [], set()
    for team in get_project_teams(pid):
        tid = team["id"]["native"]
        if team.get("name") == "Default Team" or tid in {240, 242, 2014}:
            continue
        for m in get_team_members(tid):
            full = (m.get("fullname", "N/A") or "N/A").strip()
            email = (m.get("email", "N/A") or "N/A").strip()
            roles = {r.get("name", "") for r in m.get("teamRoles", [])}
            for role in roles & TARGET_ROLES:
                found.add(role)
                result.append({"full_name": full, "email": email, "role": role})
    for role in TARGET_ROLES - found:
        result.append({"full_name": "N/A", "email": "N/A", "role": role})
    return result


# --- UPSERT statements ---

UPSERT_PROJECT = text("""
INSERT INTO projects(
  project_id, project_name, phase_name, incident_date,
  sol_due_date, total_meds, policy_limits, personal_injury_type,
  liability_decision, last_offer, date_of_incident,
  client_contact_count, latest_client_contact, project_type_code
) VALUES (
  :project_id, :project_name, :phase_name, :incident_date,
  :sol_due_date, :total_meds, :policy_limits, :personal_injury_type,
  :liability_decision, :last_offer, :date_of_incident,
  :client_contact_count, :latest_client_contact, :project_type_code
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
  project_type_code      = EXCLUDED.project_type_code;
""")

UPSERT_NEGOTIATION = text("""
INSERT INTO negotiation(
  project_id, negotiator, settlement_date, settled,
  settled_amount, last_offer, last_offer_date, date_assigned_to_nego
) VALUES (
  :project_id, :negotiator, :settlement_date, :settled,
  :settled_amount, :last_offer, :last_offer_date, :date_assigned_to_nego
)
ON CONFLICT (project_id) DO UPDATE SET
  negotiator            = EXCLUDED.negotiator,
  settlement_date       = EXCLUDED.settlement_date,
  settled               = EXCLUDED.settled,
  settled_amount        = EXCLUDED.settled_amount,
  last_offer            = EXCLUDED.last_offer,
  last_offer_date       = EXCLUDED.last_offer_date,
  date_assigned_to_nego = EXCLUDED.date_assigned_to_nego;
""")

UPSERT_INSURANCE = text("""
INSERT INTO insurance_info(
  project_id, defendant_insurance_name, client_insurance_name
) VALUES (
  :project_id, :def_name, :cli_name
)
ON CONFLICT (project_id) DO UPDATE SET
  defendant_insurance_name = EXCLUDED.defendant_insurance_name,
  client_insurance_name    = EXCLUDED.client_insurance_name;
""")

UPSERT_BREAKDOWN = text("""
INSERT INTO breakdown_info(
  project_id, lien_negotiator_name, lien_negotiator_company,
  lien_negotiator_title, lien_negotiator_dept, date_assigned, date_completed
) VALUES (
  :project_id, :lien_name, :lien_company,
  :lien_title, :lien_dept, :date_assigned, :date_completed
)
ON CONFLICT (project_id) DO UPDATE SET
  lien_negotiator_name    = EXCLUDED.lien_negotiator_name,
  lien_negotiator_company = EXCLUDED.lien_negotiator_company,
  lien_negotiator_title   = EXCLUDED.lien_negotiator_title,
  lien_negotiator_dept    = EXCLUDED.lien_negotiator_dept,
  date_assigned           = EXCLUDED.date_assigned,
  date_completed          = EXCLUDED.date_completed;
""")

UPSERT_LIT = text("""
INSERT INTO lit_case_review(
  project_id, trial_date, date_complaint_filed,
  date_attorney_assigned, settlement_amount, settlement_date,
  dismissal_filed_on
) VALUES (
  :project_id, :trial_date, :date_complaint_filed,
  :date_attorney_assigned, :settlement_amount, :settlement_date,
  :dismissal_filed_on
)
ON CONFLICT (project_id) DO UPDATE SET
  trial_date             = EXCLUDED.trial_date,
  date_complaint_filed   = EXCLUDED.date_complaint_filed,
  date_attorney_assigned = EXCLUDED.date_attorney_assigned,
  settlement_amount      = EXCLUDED.settlement_amount,
  settlement_date        = EXCLUDED.settlement_date,
  dismissal_filed_on     = EXCLUDED.dismissal_filed_on;
""")

UPSERT_DEMAND = text("""
INSERT INTO demand_info(
  project_id, demand_approved, approved_by
) VALUES (
  :project_id, :demand_approved, :approved_by
)
ON CONFLICT (project_id) DO UPDATE SET
  demand_approved = EXCLUDED.demand_approved,
  approved_by     = EXCLUDED.approved_by;
""")

UPSERT_CONTACTS = text("""
INSERT INTO contacts(
  project_id, case_manager, supervisor,
  attorney, paralegal
) VALUES (
  :project_id, :case_manager, :supervisor,
  :attorney, :paralegal
)
ON CONFLICT (project_id) DO UPDATE SET
  case_manager = EXCLUDED.case_manager,
  supervisor   = EXCLUDED.supervisor,
  attorney     = EXCLUDED.attorney,
  paralegal    = EXCLUDED.paralegal;
""")


def get_projects_by_type(code: str, limit: Optional[int] = None) -> List[int]:
    """
    Fetch all project IDs of a given FileVine projectTypeCode.
    If limit is None, will page until no more items; otherwise stops at limit.
    """
    projects: List[int] = []
    offset = 0
    page_sz = 100  # FileVine max page size
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


def load_project(pid: int):
    try:
        pj = fetch_json(f"/core/projects/{pid}") or {}
        if not pj:
            print(f"⚠️ Could not fetch project {pid}")
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

        cs_sol = get_case_summary_sol(pid)
        intake_dt = get_intake_date(pid)

        rec = {
            "project_id":           pj["projectId"]["native"],
            "project_name":         pj.get("projectOrClientName", "N/A"),
            "phase_name":           pj.get("phaseName"),
            "incident_date":        mmddyyyy_to_iso(format_date(pj.get("incidentDate")))
                                   if pj.get("incidentDate") else None,
            "sol_due_date":         mmddyyyy_to_iso(cs_sol) if cs_sol != "N/A" else None,
            "total_meds":           float(vitals.get("Total Meds"))
                                   if vitals.get("Total Meds") not in [None, "N/A"] else None,
            "policy_limits":        vitals.get("Policy Limits", "N/A"),
            "personal_injury_type": vitals.get("Personal Injury Type", "N/A"),
            "liability_decision":   vitals.get("Liability Decision", "N/A"),
            "last_offer":           vitals.get("Last Offer", "N/A"),
            "date_of_incident":     mmddyyyy_to_iso(intake_dt) if intake_dt != "N/A" else None,
            "client_contact_count": contact_count,
            "latest_client_contact": contact_latest,
            "project_type_code":    project_type_code,
        }

        with engine.begin() as conn:
            conn.execute(UPSERT_PROJECT, rec)

            if nego:
                conn.execute(UPSERT_NEGOTIATION, {
                    "project_id":            pid,
                    "negotiator":            nego.get("negotiator", "N/A"),
                    "settlement_date":       mmddyyyy_to_iso(format_date(nego.get("settlement_date")))
                                            if nego.get("settlement_date") else None,
                    "settled":               nego.get("settled", "N/A"),
                    "settled_amount":        float(nego.get("settled_amount"))
                                            if nego.get("settled_amount") not in [None, "N/A"] else None,
                    "last_offer":            nego.get("last_offer", "N/A"),
                    "last_offer_date":       mmddyyyy_to_iso(format_date(nego.get("last_offer_date")))
                                            if nego.get("last_offer_date") else None,
                    "date_assigned_to_nego": mmddyyyy_to_iso(format_date(nego.get("date_assigned_to_nego")))
                                            if nego.get("date_assigned_to_nego") else None,
                })

            conn.execute(UPSERT_INSURANCE, {
                "project_id": pid,
                "def_name":   ins.get("def_name", "N/A"),
                "cli_name":   ins.get("cli_name", "N/A"),
            })

            if br:
                conn.execute(UPSERT_BREAKDOWN, {
                    "project_id":    pid,
                    "lien_name":     br.get("lien_name", "N/A"),
                    "lien_company":  br.get("lien_company", "N/A"),
                    "lien_title":    br.get("lien_title", "N/A"),
                    "lien_dept":     br.get("lien_dept", "N/A"),
                    "date_assigned": mmddyyyy_to_iso(br.get("date_assigned"))
                                     if br.get("date_assigned") and br.get("date_assigned") != "N/A" else None,
                    "date_completed": mmddyyyy_to_iso(br.get("date_completed"))
                                      if br.get("date_completed") and br.get("date_completed") != "N/A" else None,
                })

            if lit:
                conn.execute(UPSERT_LIT, {
                    "project_id":             pid,
                    "trial_date":             mmddyyyy_to_iso(lit.get("trial_date"))
                                              if lit.get("trial_date") != "N/A" else None,
                    "date_complaint_filed":   mmddyyyy_to_iso(lit.get("date_complaint_filed"))
                                              if lit.get("date_complaint_filed") != "N/A" else None,
                    "date_attorney_assigned": mmddyyyy_to_iso(lit.get("date_attorney_assigned"))
                                              if lit.get("date_attorney_assigned") != "N/A" else None,
                    "settlement_amount":      lit.get("settlement_amount", "N/A"),
                    "settlement_date":        mmddyyyy_to_iso(lit.get("settlement_date"))
                                              if lit.get("settlement_date") != "N/A" else None,
                    "dismissal_filed_on":     mmddyyyy_to_iso(lit.get("dismissal_filed_on"))
                                              if lit.get("dismissal_filed_on") != "N/A" else None,
                })

            if demand_dt or demand_by:
                conn.execute(UPSERT_DEMAND, {
                    "project_id":      pid,
                    "demand_approved": mmddyyyy_to_iso(demand_dt)
                                       if demand_dt and demand_dt != "N/A" else None,
                    "approved_by":     demand_by or "N/A",
                })

            role_map = {m["role"]: m["full_name"] for m in get_relevant_team_members(pid)}
            conn.execute(UPSERT_CONTACTS, {
                "project_id":  pid,
                "case_manager": role_map.get("Case Manager", "N/A"),
                "supervisor":   role_map.get("Supervisor", "N/A"),
                "attorney":     role_map.get("Attorney", "N/A"),
                "paralegal":    role_map.get("Paralegal", "N/A"),
            })

    except Exception as e:
        print(f"❌ Failed to load {pid}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    project_ids = get_projects_by_type("LOJE 2.0", limit=None)
    print(f"Total projects fetched: {len(project_ids)}")

    failed_projects: List[int] = []
    successful_count = 0
    batch_size = 20
    retry_delay = 5  # seconds

    for i in range(0, len(project_ids), batch_size):
        batch = project_ids[i:i + batch_size]
        print(f"\nProcessing batch {i // batch_size + 1} (projects {i + 1}-{min(i + batch_size, len(project_ids))})...")

        for pid in batch:
            try:
                load_project(pid)
                successful_count += 1
            except Exception as e:
                print(f"❌ Failed to load {pid}: {str(e)}")
                failed_projects.append(pid)
                continue

        if i + batch_size < len(project_ids):
            print(f"⏳ Waiting {retry_delay} seconds before next batch...")
            time.sleep(retry_delay)

    if failed_projects:
        print(f"\nRetrying {len(failed_projects)} failed projects...")
        retry_failed: List[int] = []

        for pid in failed_projects:
            try:
                print(f"\nRetrying project {pid}")
                load_project(pid)
                successful_count += 1
            except Exception as e:
                print(f"❌ Failed again to load {pid}: {str(e)}")
                retry_failed.append(pid)
                continue

        print("\n" + "=" * 50)
        print("Processing complete!")
        print(f"Successfully loaded: {successful_count}/{len(project_ids)}")
        print(f"Failed after retry: {len(retry_failed)}")
        if retry_failed:
            print("Failed project IDs:", retry_failed)
    else:
        print("\n" + "=" * 50)
        print("Processing complete!")
        print(f"Successfully loaded: {successful_count}/{len(project_ids)}")
        print("Failed after retry: 0")
