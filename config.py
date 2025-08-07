# config.py
import os
from dotenv import load_dotenv

# if you're running outside of VS Code's debugger, explicitly load the .env:
load_dotenv()  

# Filevine API
API_BASE_URL = os.getenv("API_BASE_URL", "https://calljacob.api.filevineapp.com")

API_KEY     = os.getenv("API_KEY")
API_SECRET  = os.getenv("API_SECRET")
USER_ID     = os.getenv("USER_ID")
ORG_ID      = os.getenv("ORG_ID")
SESSION_URL = os.getenv("SESSION_URL")

# Postgres connection
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_URL      = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
