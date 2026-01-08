from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from src.pipelines.logbook_sheet import run_logbook_sheet_pipeline
from src.pipelines.logbook_entry import run_logbook_entry_pipeline
from src.db.connections import db_manager
import logging

from src.config.settings import settings

app = FastAPI(title="ITXDA Pipeline API")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.post("/afl")
def execute_pipeline():
    try:
        logger.info("Starting pipeline execution...")
        run_logbook_sheet_pipeline()
        run_logbook_entry_pipeline()
        logger.info("Pipeline execution completed successfully.")
        return {"status": "success", "message": "All pipelines executed successfully."}
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    health_status = {"status": "ok", "testing_mode": settings.IS_TESTING}

    # Postgres Check
    try:
        with db_manager.get_postgres_engine().connect() as connection:
            connection.execute(text("SELECT 1"))
        health_status["postgres"] = "connected"
    except Exception as e:
        health_status["postgres"] = f"error: {str(e)}"
        health_status["status"] = "degraded"

    # MySQL Check
    try:
        with db_manager.mysql_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        health_status["mysql"] = "connected"
    except Exception as e:
        health_status["mysql"] = f"error: {str(e)}"
        health_status["status"] = "degraded"

    return health_status
