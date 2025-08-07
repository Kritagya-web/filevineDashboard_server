# tasks.py

from redis import Redis
from rq import Queue, Retry
from worker_tasks import process_project

# Connect to local Redis
redis_conn = Redis(host="localhost", port=6379, db=0)
q = Queue("filevine", connection=redis_conn)

def enqueue_project(project_id):
    redis_conn = Redis()          # or your REDIS_URL
    q = Queue("filevine", connection=redis_conn)
    # retry up to 3 times with default backoff
    job = q.enqueue(
        process_project,
        project_id,
        retry=Retry(max=3)
    )
    return job.id

