from fastapi import FastAPI, HTTPException, Header, status
from sqlalchemy import text
from src.pipelines.logbook_sheet import run_logbook_sheet_pipeline
from src.pipelines.logbook_entry import run_logbook_entry_pipeline
from src.db.connections import db_manager
import logging

from src.config.settings import settings

app = FastAPI(title="ITXDA Pipeline API")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def caesar_cipher(text: str, shift: int) -> str:
    result = ""
    for char in text:
        if char.isalpha():
            start = ord("A") if char.isupper() else ord("a")
            result += chr((ord(char) - start + shift) % 26 + start)
        else:
            result += char
    return result


@app.get("/afl")
def execute_pipeline(x_secret_key: str = Header(..., alias="X-Key")):
    expected_key = (
        caesar_cipher(settings.SECRET_KEY, settings.CAESAR_SHIFT)
        if settings.SECRET_KEY
        else None
    )
    if expected_key is None or x_secret_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Secret Key",
        )
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
