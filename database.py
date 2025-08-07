# database.py

import sqlite3

DB_PATH = "projects.db"

def init_db():
    """
    Ensure the `projects` table exists.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS projects (
        project_id    INTEGER PRIMARY KEY,
        last_modified TEXT NOT NULL
      )
    """)
    conn.commit()
    conn.close()

def get_last_modified(project_id: int) -> str | None:
    """
    Return the last_modified timestamp for a given project_id,
    or None if the project_id is not found.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute(
        "SELECT last_modified FROM projects WHERE project_id = ?",
        (project_id,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None
