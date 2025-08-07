import logging
from fastapi import FastAPI, Request, HTTPException
from tasks import enqueue_project

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
    logger.debug("Payload keys: %s", list(data.keys()))

    pid = (
        data.get("projectId")
        or data.get("projectID")
        or data.get("ProjectId")
        or data.get("objectId", {}).get("native")
        or data.get("data", {}).get("objectId", {}).get("native")
    )
    evt = (
        data.get("eventType")
        or data.get("Event")
        or "unknown_event"
    )

    if not pid:
        logger.warning("No projectId found in payload, ignoring.")
        return {"status": "ignored"}

    logger.info("🔔 Enqueueing project %s for event %s", pid, evt)
    job_id = enqueue_project(pid)
    logger.info("🏷 Job %s queued for project %s", job_id, pid)
    return {"status":"queued", "projectId": pid, "jobId": job_id}
