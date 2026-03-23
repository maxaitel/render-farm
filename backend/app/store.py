from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from pathlib import Path
from typing import Callable

from .models import JobPhase, JobRecord


Mutator = Callable[[JobRecord], None]


class JobStore:
    def __init__(self, jobs_root: Path) -> None:
        self.jobs_root = jobs_root
        self._jobs: dict[str, JobRecord] = {}
        self._subscribers: dict[str, set[asyncio.Queue[dict]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def load(self) -> None:
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        for meta_path in sorted(self.jobs_root.glob("*/job.json")):
            payload = await asyncio.to_thread(meta_path.read_text, "utf-8")
            job = JobRecord.model_validate_json(payload)
            if job.phase == JobPhase.running:
                job.phase = JobPhase.failed
                job.error = "Server restarted while the render was in progress."
                job.status_message = "Interrupted by server restart."
            self._jobs[job.id] = job
            await self._persist(job)

    async def list_jobs(self) -> list[JobRecord]:
        async with self._lock:
            jobs = [job.model_copy(deep=True) for job in self._jobs.values()]
        return sorted(jobs, key=lambda item: item.created_at, reverse=True)

    async def get(self, job_id: str) -> JobRecord | None:
        async with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    async def create(self, job: JobRecord) -> JobRecord:
        async with self._lock:
            self._jobs[job.id] = job
            snapshot = job.model_copy(deep=True)
        await self._persist(snapshot)
        await self._broadcast(snapshot)
        return snapshot

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
        job_dir = self.jobs_root / snapshot.id
        job_dir.mkdir(parents=True, exist_ok=True)
        meta_path = job_dir / "job.json"
        payload = json.dumps(snapshot.model_dump(mode="json"), indent=2)
        await asyncio.to_thread(meta_path.write_text, payload, "utf-8")

