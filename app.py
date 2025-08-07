# app.py
import logging
from fastapi import FastAPI, Request, HTTPException
from tasks import enqueue_project

# ——— Configure logging ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("filevine-webhook")

app = FastAPI()

@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        logger.error("Received invalid JSON")
        raise HTTPException(400, "Invalid JSON")

    pid = data.get("projectId")
    evt = data.get("eventType", "unknown_event")
    if not pid:
        logger.warning("Missing projectId in payload: %r", data)
        raise HTTPException(400, "Missing projectId")

    logger.info("Enqueueing project %s for event %s", pid, evt)
    job_id = enqueue_project(pid)
    logger.info("Enqueued job %s for project %s", job_id, pid)
    return {"status":"queued", "projectId": pid, "jobId": job_id}
