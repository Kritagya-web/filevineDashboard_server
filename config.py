# config.py

# Filevine API
API_BASE_URL = "https://calljacob.api.filevineapp.com"

# Postgres connection
DB_USER     = "postgres"
DB_PASSWORD = "kritagya"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "calljacob"
DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
