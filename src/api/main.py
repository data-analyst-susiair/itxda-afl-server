from fastapi import FastAPI, HTTPException
from src.pipelines.logbook_sheet import run_logbook_sheet_pipeline
from src.pipelines.logbook_entry import run_logbook_entry_pipeline
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
    return {"status": settings.IS_TESTING}
