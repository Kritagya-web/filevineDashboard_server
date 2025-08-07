# worker_tasks.py

from filevine_loader import load_project

def process_project(project_id: int):
    """
    RQ worker entrypoint. Calls your loader and lets failures be retried.
    """
    print(f"🚀 Worker: processing project {project_id}")
    try:
        load_project(project_id)
        print(f"✅ Worker: done project {project_id}")
    except Exception as e:
        print(f"❌ Worker: error on project {project_id}: {e}")
        raise  # ensures RQ will retry up to 3 times
