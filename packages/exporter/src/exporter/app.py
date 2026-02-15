from __future__ import annotations

from enum import Enum
from threading import Lock
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Gauge,
    generate_latest,
)
from pydantic import BaseModel, ConfigDict

from exporter.mirror import run_mirror_job


class JobStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class AppState:
    def __init__(self) -> None:
        self.job_status = JobStatus.IDLE
        self.error_message: Optional[str] = None
        self.lock = Lock()

    def set_status(self, status: JobStatus, error_message: Optional[str] = None) -> None:
        with self.lock:
            self.job_status = status
            self.error_message = error_message

    def get_status(self) -> tuple[JobStatus, Optional[str]]:
        with self.lock:
            return self.job_status, self.error_message


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = False


def create_app() -> FastAPI:
    app = FastAPI(title="HN Mirror Exporter")

    registry = CollectorRegistry()
    job_status_metric = Gauge(
        "hn_mirror_job_status",
        "Status of the HN mirror job",
        ["status"],
        registry=registry,
    )
    job_status_metric.labels("finished").set(0)
    job_status_metric.labels("failed").set(0)

    app.state.mirror_job = AppState()

    def _run_mirror_job_with_status(dry_run: bool = False) -> None:
        try:
            app.state.mirror_job.set_status(JobStatus.RUNNING)
            run_mirror_job(dry_run=dry_run)
            app.state.mirror_job.set_status(JobStatus.FINISHED)
            job_status_metric.labels("finished").set(1)
            job_status_metric.labels("failed").set(0)
        except Exception as exc:  # pragma: no cover - exercised in runtime jobs
            app.state.mirror_job.set_status(JobStatus.FAILED, str(exc))
            job_status_metric.labels("finished").set(0)
            job_status_metric.labels("failed").set(1)

    @app.post("/jobs")
    async def create_job(job: JobCreate, background_tasks: BackgroundTasks) -> dict[str, str]:
        current_status, _ = app.state.mirror_job.get_status()
        if current_status == JobStatus.RUNNING:
            raise HTTPException(status_code=409, detail="A job is already running")

        app.state.mirror_job.set_status(JobStatus.IDLE)
        background_tasks.add_task(_run_mirror_job_with_status, job.dry_run)
        return {
            "message": (
                "Job started to mirror 'kids' and 'items' tables "
                f"(dry_run: {job.dry_run})"
            )
        }

    @app.get("/jobs/status")
    async def get_job_status() -> dict[str, str | None]:
        status, error_message = app.state.mirror_job.get_status()
        return {"status": status.value, "error": error_message}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
