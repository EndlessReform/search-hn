from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from pydantic import BaseModel
from prometheus_client import (
    Gauge,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from datasets import Dataset
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from enum import Enum
from typing import Optional
from threading import Lock

app = FastAPI()

# Load environment variables
load_dotenv()
postgres_conn = os.getenv("DATABASE_URL")
hf_token = os.getenv("HF_TOKEN")

# Create SQLAlchemy engine
engine = create_engine(postgres_conn)

# Prometheus setup
REGISTRY = CollectorRegistry()
job_status = Gauge(
    "hn_mirror_job_status", "Status of the HN mirror job", ["status"], registry=REGISTRY
)
job_status.labels("finished").set(0)
job_status.labels("failed").set(0)


# Global state
class JobStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class AppState:
    def __init__(self):
        self.job_status = JobStatus.IDLE
        self.error_message: Optional[str] = None
        self.lock = Lock()

    def set_status(self, status: JobStatus, error_message: Optional[str] = None):
        with self.lock:
            self.job_status = status
            self.error_message = error_message

    def get_status(self):
        with self.lock:
            return self.job_status, self.error_message


app.state.mirror_job = AppState()


class JobCreate(BaseModel):
    dry_run: bool = False


def run_mirror_job(dry_run: bool = False):
    try:
        app.state.mirror_job.set_status(JobStatus.RUNNING)

        # Mirror 'kids' table
        crosswalk_ds = Dataset.from_sql("SELECT * FROM kids;", engine, cache_dir=None)
        if not dry_run:
            crosswalk_ds.push_to_hub(
                repo_id="jkeisling/hn-crosswalk", max_shard_size="5GB", token=hf_token
            )

        # Mirror 'items' table
        items_ds = Dataset.from_sql("SELECT * FROM items;", engine, cache_dir=None)
        if not dry_run:
            items_ds.push_to_hub(
                repo_id="jkeisling/hn-items", max_shard_size="5GB", token=hf_token
            )

        app.state.mirror_job.set_status(JobStatus.FINISHED)
        job_status.labels("finished").set(1)
        job_status.labels("failed").set(0)
    except Exception as e:
        app.state.mirror_job.set_status(JobStatus.FAILED, str(e))
        job_status.labels("finished").set(0)
        job_status.labels("failed").set(1)


@app.post("/jobs")
async def create_job(job: JobCreate, background_tasks: BackgroundTasks):
    current_status, _ = app.state.mirror_job.get_status()
    if current_status == JobStatus.RUNNING:
        raise HTTPException(status_code=409, detail="A job is already running")

    app.state.mirror_job.set_status(JobStatus.IDLE)  # Reset state
    background_tasks.add_task(run_mirror_job, job.dry_run)
    return {
        "message": f"Job started to mirror 'kids' and 'items' tables (Dry run: {job.dry_run})"
    }


# Endpoints
@app.post("/jobs")
async def create_job(background_tasks: BackgroundTasks):
    current_status, _ = app.state.mirror_job.get_status()
    if current_status == JobStatus.RUNNING:
        raise HTTPException(status_code=409, detail="A job is already running")

    app.state.mirror_job.set_status(JobStatus.IDLE)  # Reset state
    background_tasks.add_task(run_mirror_job)
    return {"message": "Job started to mirror 'kids' and 'items' tables"}


@app.get("/jobs/status")
async def get_job_status():
    status, error_message = app.state.mirror_job.get_status()
    return {"status": status.value, "error": error_message}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
