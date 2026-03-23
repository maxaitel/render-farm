from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Callable

from .models import JobPhase, JobRecord, utc_now


Mutator = Callable[[JobRecord], None]

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    phase TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_phase_idx ON jobs(phase);
"""


class JobStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.jobs_root = database_path.parent / "jobs"
        self._jobs: dict[str, JobRecord] = {}
        self._subscribers: dict[str, set[asyncio.Queue[dict]]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._db_lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None

    async def load(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.database_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)
        self._conn.execute(
            "INSERT OR IGNORE INTO users (id, username, created_at) VALUES (1, ?, ?)",
            ("local", utc_now().isoformat()),
        )
        self._conn.commit()

        self._jobs = {}
        await self._import_legacy_jobs()
        rows = self._conn.execute("SELECT payload FROM jobs ORDER BY created_at ASC").fetchall()

        for row in rows:
            job = JobRecord.model_validate_json(row["payload"])
            if job.phase == JobPhase.running:
                job.phase = JobPhase.failed
                job.error = "Server restarted while the render was in progress."
                job.status_message = "Interrupted by server restart."
            self._jobs[job.id] = job
            await self._persist(job)

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def list_jobs(self) -> list[JobRecord]:
        async with self._lock:
            jobs = [job.model_copy(deep=True) for job in self._jobs.values()]
        return sorted(jobs, key=lambda item: item.created_at, reverse=True)

    async def get(self, job_id: str) -> JobRecord | None:
        async with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    async def create(self, job: JobRecord) -> JobRecord:
        return (await self.create_many([job]))[0]

    async def create_many(self, jobs: list[JobRecord]) -> list[JobRecord]:
        snapshots = [job.model_copy(deep=True) for job in jobs]
        await self._persist_many(snapshots)
        async with self._lock:
            for job, snapshot in zip(jobs, snapshots, strict=True):
                self._jobs[job.id] = job
        for snapshot in snapshots:
            await self._broadcast(snapshot)
        return snapshots

    async def mutate(self, job_id: str, mutator: Mutator) -> JobRecord:
        async with self._lock:
            job = self._jobs[job_id]
            mutator(job)
            snapshot = job.model_copy(deep=True)
        await self._persist(snapshot)
        await self._broadcast(snapshot)
        return snapshot

    async def append_log(self, job_id: str, line: str) -> JobRecord:
        def update(job: JobRecord) -> None:
            cleaned = line.strip()
            if not cleaned:
                return
            job.logs_tail.append(cleaned)
            if len(job.logs_tail) > 40:
                job.logs_tail = job.logs_tail[-40:]

        return await self.mutate(job_id, update)

    async def subscribe(self, job_id: str) -> asyncio.Queue[dict]:
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=50)
        async with self._lock:
            self._subscribers[job_id].add(queue)
            job = self._jobs.get(job_id)
            snapshot = job.model_copy(deep=True) if job else None
        if snapshot:
            await queue.put(snapshot.model_dump(mode="json"))
        return queue

    async def unsubscribe(self, job_id: str, queue: asyncio.Queue[dict]) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(job_id)
            if not subscribers:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(job_id, None)

    async def queued_job_ids(self) -> list[str]:
        async with self._lock:
            return [
                job.id
                for job in sorted(self._jobs.values(), key=lambda item: item.created_at)
                if job.phase == JobPhase.queued
            ]

    async def _broadcast(self, snapshot: JobRecord) -> None:
        payload = snapshot.model_dump(mode="json")
        async with self._lock:
            subscribers = list(self._subscribers.get(snapshot.id, set()))
        for queue in subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                continue

    async def _persist(self, snapshot: JobRecord) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            await asyncio.to_thread(self._persist_sync, snapshot)

    async def _persist_many(self, snapshots: list[JobRecord]) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            await asyncio.to_thread(self._persist_many_sync, snapshots)

    def _persist_sync(self, snapshot: JobRecord) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        with self._conn:
            self._write_job_sync(snapshot)

    def _persist_many_sync(self, snapshots: list[JobRecord]) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        with self._conn:
            for snapshot in snapshots:
                self._write_job_sync(snapshot)

    def _write_job_sync(self, snapshot: JobRecord) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        payload = snapshot.model_dump(mode="json")
        self._conn.execute(
            """
            INSERT INTO jobs (id, user_id, created_at, phase, source_filename, payload)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                created_at = excluded.created_at,
                phase = excluded.phase,
                source_filename = excluded.source_filename,
                payload = excluded.payload
            """,
            (
                snapshot.id,
                1,
                payload["created_at"],
                payload["phase"],
                payload["source_filename"],
                json.dumps(payload, indent=2),
            ),
        )

    async def _import_legacy_jobs(self) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        for meta_path in sorted(self.jobs_root.glob("*/job.json")):
            try:
                payload = await asyncio.to_thread(meta_path.read_text, "utf-8")
                job = JobRecord.model_validate_json(payload)
            except Exception as exc:
                print(f"Skipping unreadable legacy job file {meta_path}: {exc}")
                continue

            already_imported = self._conn.execute(
                "SELECT 1 FROM jobs WHERE id = ?",
                (job.id,),
            ).fetchone()
            if already_imported:
                continue
            await self._persist(job)
