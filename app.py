import logging
import io
import csv

from fastapi import FastAPI, Request, HTTPException
from starlette.responses import StreamingResponse

from tasks import enqueue_project
from filevine_loader import engine

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("filevine-webhook")

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        logger.error("Failed to parse JSON body")
        raise HTTPException(400, "Invalid JSON")

    logger.info("RAW WEBHOOK PAYLOAD: %r", data)
    pid = (
        data.get("projectId")
        or data.get("projectID")
        or data.get("ProjectId")
        or data.get("objectId", {}).get("native")
        or data.get("data", {}).get("objectId", {}).get("native")
    )
    evt = data.get("eventType") or data.get("Event") or "unknown_event"

    if not pid:
        logger.warning("No projectId found in payload, ignoring.")
        return {"status": "ignored"}

    logger.info("🔔 Enqueueing project %s for event %s", pid, evt)
    job_id = enqueue_project(pid)
    logger.info("🏷 Job %s queued for project %s", job_id, pid)
    return {"status": "queued", "projectId": pid, "jobId": job_id}


def iter_full_export_csv():
    """
    Streams exactly the joined columns & friendly headers you specified,
    straight out of Postgres as CSV.
    """
    # 1) Pull the full SELECT as a plain string:
    query = """
    SELECT
      p.project_type_code   AS "Filevine Template Name",
      c.case_manager        AS "Team Case Manager Full Name",
      c.supervisor          AS "Team Supervisor Full Name",
      c.attorney            AS "Team Attorney Full Name",
      c.paralegal           AS "Team Paralegal Full Name",
      n.negotiator          AS "Nego Assigned: Full Name",
      p.project_name        AS "Name",
      p.phase_name          AS "Phase",
      p.date_of_incident    AS "Date of Intake",
      p.incident_date       AS "Incident Date",
      p.sol_due_date        AS "SOL Due",
      p.total_meds          AS "Medical Recs/Bills: Total Amount Billed",
      n.settled             AS "NEGOTIATION: Settled",
      n.settlement_date     AS "NEGOTIATION: Settlement Date",
      p.policy_limits       AS "Policy Limits",
      p.client_contact_count      AS "Count of Client Contact Items",
      p.latest_client_contact     AS "Client Contact: Latest Created",
      i.defendant_insurance_name  AS "Defendant Insurance",
      i.client_insurance_name     AS "Client Insurance",
      n.date_assigned_to_nego     AS "NEGOTIATION: Date Assigned to Nego",
      n.last_offer                AS "NEGOTIATION: Last Offer",
      n.last_offer_date           AS "NEGOTIATION: Last Offer Date",
      b.lien_negotiator_name      AS "BREAKDOWN: Lien Negotiator Assigned To",
      b.date_assigned             AS "BREAKDOWN: Date Assigned To Breakdown",
      n.settlement_date           AS "SETTLEMENT: Latest Date of Settlement",
      l.trial_date                AS "Lit Case Summary: Trial Date",
      l.date_complaint_filed      AS "Lit Case Summary: Date Complaint was Filed",
      l.date_attorney_assigned    AS "Lit Case Summary: Date Attorney was Assigned",
      l.settlement_amount         AS "Lit Case Summary: Settlement Amount",
      l.settlement_date           AS "Lit Case Summary: Settlement Date",
      l.dismissal_filed_on        AS "Lit Case Summary: Dismissal Filed On",
      d.demand_approved           AS "Demand: Demand Approved",
      d.approved_by               AS "Demand: Approved By"
    FROM projects p
      LEFT JOIN contacts        c USING (project_id)
      LEFT JOIN negotiation     n USING (project_id)
      LEFT JOIN insurance_info  i USING (project_id)
      LEFT JOIN breakdown_info  b USING (project_id)
      LEFT JOIN lit_case_review l USING (project_id)
      LEFT JOIN demand_info     d USING (project_id)
    ORDER BY p.project_id;
    """

    # 2) Open a raw DB connection and cursor
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # 3) Execute the plain SQL string
    cursor.execute(query)

    # 4) Stream out the header
    header = [col[0] for col in cursor.description]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)

    # 5) Stream each row
    for row in cursor:
        writer.writerow(row)
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

    cursor.close()
    conn.close()


@app.get("/export/full.csv")
def export_full():
    """
    Download the fully joined dataset with all your friendly column names.
    """
    return StreamingResponse(
        iter_full_export_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="filevine_full_export.csv"'}
    )
