# tasks.py

from redis import Redis
from rq import Queue
from worker_tasks import process_project

# Connect to local Redis
redis_conn = Redis(host="localhost", port=6379, db=0)
q = Queue("filevine", connection=redis_conn)

def enqueue_project(project_id: int):
    """
    Schedule process_project(pid) to run in the background.
    Returns the RQ job ID.
    """
    job = q.enqueue(process_project, project_id, retry=3)
    print(f"🗳 Enqueued project {project_id}, job id {job.id}")
    return job.id
