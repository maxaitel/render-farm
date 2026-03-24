from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from .models import (
    ActivityRecord,
    AdminOverview,
    JobPhase,
    JobRecord,
    UserFileRecord,
    UserRecord,
    UserRole,
    UserSessionRecord,
    UserStatus,
    utc_now,
)
from .security import hash_password, verify_password


Mutator = Callable[[JobRecord], None]

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'user',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    approved_at TEXT,
    approved_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS user_sessions (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    ip_address TEXT,
    user_agent TEXT
);

CREATE TABLE IF NOT EXISTS user_files (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_root TEXT NOT NULL,
    original_size_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id TEXT NOT NULL REFERENCES user_files(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    phase TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    actor_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    subject_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    file_id TEXT REFERENCES user_files(id) ON DELETE SET NULL,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    description TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}'
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_phase_idx ON jobs(phase);
CREATE INDEX IF NOT EXISTS jobs_user_id_idx ON jobs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_file_id_idx ON jobs(file_id, created_at DESC);
CREATE INDEX IF NOT EXISTS user_files_user_id_idx ON user_files(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS user_sessions_user_id_idx ON user_sessions(user_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS activity_log_created_at_idx ON activity_log(created_at DESC);
"""


class JobStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.jobs_root = database_path.parent / "jobs"
        self.files_root = database_path.parent / "files"
        self._jobs: dict[str, JobRecord] = {}
        self._subscribers: dict[str, set[asyncio.Queue[dict]]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._db_lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None

    async def load(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self.files_root.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.database_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)
        self._conn.executescript(INDEX_SQL)

        self._jobs = {}
        rows = self._conn.execute(
            "SELECT id, user_id, file_id, created_at, phase, source_filename, payload FROM jobs ORDER BY created_at ASC"
        ).fetchall()

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

    async def ensure_bootstrap_admin(self, username: str | None, password: str | None) -> None:
        if self._conn is None or not username or not password:
            return

        now = utc_now().isoformat()
        try:
            password_hash = hash_password(password)
        except ValueError as exc:
            print(f"Skipping bootstrap admin update for {username!r}: {exc}")
            return
        async with self._db_lock:
            await asyncio.to_thread(self._ensure_bootstrap_admin_sync, username, password_hash, now)

    def _ensure_bootstrap_admin_sync(self, username: str, password_hash: str, now: str) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        existing = self._conn.execute(
            "SELECT id FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        with self._conn:
            if existing:
                self._conn.execute(
                    """
                    UPDATE users
                    SET password_hash = ?, role = ?, status = ?, approved_at = COALESCE(approved_at, ?)
                    WHERE id = ?
                    """,
                    (password_hash, UserRole.admin.value, UserStatus.approved.value, now, existing["id"]),
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO users (username, password_hash, role, status, created_at, approved_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        username,
                        password_hash,
                        UserRole.admin.value,
                        UserStatus.approved.value,
                        now,
                        now,
                    ),
                )

    async def create_user(
        self,
        *,
        username: str,
        password: str,
        role: UserRole = UserRole.user,
        status: UserStatus = UserStatus.pending,
        approved_by_user_id: int | None = None,
    ) -> UserRecord:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        now = utc_now().isoformat()
        password_hash = hash_password(password)
        approved_at = now if status == UserStatus.approved else None

        async with self._db_lock:
            return await asyncio.to_thread(
                self._create_user_sync,
                username,
                password_hash,
                role.value,
                status.value,
                now,
                approved_at,
                approved_by_user_id,
            )

    def _create_user_sync(
        self,
        username: str,
        password_hash: str,
        role: str,
        status: str,
        now: str,
        approved_at: str | None,
        approved_by_user_id: int | None,
    ) -> UserRecord:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO users (
                    username,
                    password_hash,
                    role,
                    status,
                    created_at,
                    approved_at,
                    approved_by_user_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, password_hash, role, status, now, approved_at, approved_by_user_id),
            )

        row = self._conn.execute(
            "SELECT * FROM users WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
        assert row is not None
        return self._user_from_row(row)

    async def get_user_by_username(self, username: str) -> UserRecord | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            row = await asyncio.to_thread(
                lambda: self._conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            )
        return self._user_from_row(row) if row else None

    async def get_user_by_id(self, user_id: int) -> UserRecord | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            row = await asyncio.to_thread(
                lambda: self._conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            )
        return self._user_from_row(row) if row else None

    async def authenticate_user(self, username: str, password: str) -> UserRecord | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            row = await asyncio.to_thread(
                lambda: self._conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            )
        if row is None:
            return None
        password_hash = row["password_hash"] or ""
        if not password_hash or not verify_password(password, password_hash):
            return None
        return self._user_from_row(row)

    async def create_session(
        self,
        *,
        user_id: int,
        token_hash: str,
        expires_in_hours: int,
        ip_address: str | None,
        user_agent: str | None,
    ) -> UserSessionRecord:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        created_at = utc_now()
        expires_at = created_at + timedelta(hours=expires_in_hours)
        session_id = uuid.uuid4().hex

        async with self._db_lock:
            return await asyncio.to_thread(
                self._create_session_sync,
                session_id,
                user_id,
                token_hash,
                created_at.isoformat(),
                expires_at.isoformat(),
                ip_address,
                user_agent,
            )

    def _create_session_sync(
        self,
        session_id: str,
        user_id: int,
        token_hash: str,
        created_at: str,
        expires_at: str,
        ip_address: str | None,
        user_agent: str | None,
    ) -> UserSessionRecord:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO user_sessions (
                    id,
                    user_id,
                    token_hash,
                    created_at,
                    expires_at,
                    last_seen_at,
                    ip_address,
                    user_agent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    user_id,
                    token_hash,
                    created_at,
                    expires_at,
                    created_at,
                    ip_address,
                    user_agent,
                ),
            )
            self._conn.execute(
                "UPDATE users SET last_login_at = ? WHERE id = ?",
                (created_at, user_id),
            )

        row = self._conn.execute("SELECT * FROM user_sessions WHERE id = ?", (session_id,)).fetchone()
        assert row is not None
        return self._session_from_row(row)

    async def get_session_with_user(self, token_hash: str) -> tuple[UserSessionRecord, UserRecord] | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            row = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT
                        s.id AS session_id,
                        s.user_id AS session_user_id,
                        s.created_at AS session_created_at,
                        s.expires_at AS session_expires_at,
                        s.last_seen_at AS session_last_seen_at,
                        s.ip_address AS session_ip_address,
                        s.user_agent AS session_user_agent,
                        u.*
                    FROM user_sessions AS s
                    JOIN users AS u ON u.id = s.user_id
                    WHERE s.token_hash = ?
                    """,
                    (token_hash,),
                ).fetchone()
            )
        if row is None:
            return None

        session = UserSessionRecord.model_validate(
            {
                "id": row["session_id"],
                "user_id": row["session_user_id"],
                "created_at": row["session_created_at"],
                "expires_at": row["session_expires_at"],
                "last_seen_at": row["session_last_seen_at"],
                "ip_address": row["session_ip_address"],
                "user_agent": row["session_user_agent"],
            }
        )
        user = self._user_from_row(row)
        return session, user

    async def touch_session(self, session_id: str) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        last_seen_at = utc_now().isoformat()
        async with self._db_lock:
            await asyncio.to_thread(
                lambda: self._conn.execute(
                    "UPDATE user_sessions SET last_seen_at = ? WHERE id = ?",
                    (last_seen_at, session_id),
                )
            )
            self._conn.commit()

    async def revoke_session(self, token_hash: str) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            await asyncio.to_thread(
                lambda: self._conn.execute(
                    "DELETE FROM user_sessions WHERE token_hash = ?",
                    (token_hash,),
                )
            )
            self._conn.commit()

    async def prune_expired_sessions(self) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        now = utc_now().isoformat()
        async with self._db_lock:
            await asyncio.to_thread(
                lambda: self._conn.execute(
                    "DELETE FROM user_sessions WHERE expires_at <= ?",
                    (now,),
                )
            )
            self._conn.commit()

    async def list_users(self) -> list[UserRecord]:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            rows = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT
                        u.*,
                        COUNT(DISTINCT f.id) AS render_file_count,
                        COUNT(DISTINCT j.id) AS run_count
                    FROM users AS u
                    LEFT JOIN user_files AS f ON f.user_id = u.id
                    LEFT JOIN jobs AS j ON j.user_id = u.id
                    GROUP BY u.id
                    ORDER BY
                        CASE u.status
                            WHEN 'pending' THEN 0
                            WHEN 'approved' THEN 1
                            ELSE 2
                        END,
                        u.created_at DESC
                    """
                ).fetchall()
            )
        return [self._user_from_row(row) for row in rows]

    async def set_user_status(
        self,
        *,
        user_id: int,
        status: UserStatus,
        actor_user_id: int,
    ) -> UserRecord | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        now = utc_now().isoformat()
        async with self._db_lock:
            return await asyncio.to_thread(
                self._set_user_status_sync,
                user_id,
                status.value,
                now,
                actor_user_id,
            )

    def _set_user_status_sync(
        self,
        user_id: int,
        status: str,
        now: str,
        actor_user_id: int,
    ) -> UserRecord | None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        with self._conn:
            self._conn.execute(
                """
                UPDATE users
                SET
                    status = ?,
                    approved_at = CASE WHEN ? = ? THEN COALESCE(approved_at, ?) ELSE approved_at END,
                    approved_by_user_id = CASE WHEN ? = ? THEN COALESCE(approved_by_user_id, ?) ELSE approved_by_user_id END
                WHERE id = ?
                """,
                (
                    status,
                    status,
                    UserStatus.approved.value,
                    now,
                    status,
                    UserStatus.approved.value,
                    actor_user_id,
                    user_id,
                ),
            )

        row = self._conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return self._user_from_row(row) if row else None

    async def create_user_file(self, record: UserFileRecord) -> UserFileRecord:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            await asyncio.to_thread(self._create_user_file_sync, record)
        return record.model_copy(deep=True)

    def _create_user_file_sync(self, record: UserFileRecord) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        payload = record.model_dump(mode="json")
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO user_files (
                    id,
                    user_id,
                    created_at,
                    updated_at,
                    source_filename,
                    source_path,
                    source_root,
                    original_size_bytes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["id"],
                    payload["user_id"],
                    payload["created_at"],
                    payload["updated_at"],
                    payload["source_filename"],
                    payload["source_path"],
                    payload["source_root"],
                    payload["original_size_bytes"],
                ),
            )

    async def get_user_file(self, user_id: int, file_id: str) -> UserFileRecord | None:
        files = await self.list_user_files(user_id)
        for item in files:
            if item.id == file_id:
                return item
        return None

    async def list_user_files(self, user_id: int) -> list[UserFileRecord]:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")

        async with self._db_lock:
            files_rows = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT *
                    FROM user_files
                    WHERE user_id = ?
                    ORDER BY updated_at DESC, created_at DESC
                    """,
                    (user_id,),
                ).fetchall()
            )
            job_rows = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT payload
                    FROM jobs
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                    """,
                    (user_id,),
                ).fetchall()
            )

        jobs_by_file: dict[str, list[JobRecord]] = defaultdict(list)
        for row in job_rows:
            job = JobRecord.model_validate_json(row["payload"])
            jobs_by_file[job.file_id].append(job)

        files: list[UserFileRecord] = []
        for row in files_rows:
            record = self._file_from_row(row)
            record.jobs = jobs_by_file.get(record.id, [])
            record.latest_job = record.jobs[0] if record.jobs else None
            files.append(record)
        return files

    async def list_jobs(self, user_id: int | None = None) -> list[JobRecord]:
        async with self._lock:
            jobs = [job.model_copy(deep=True) for job in self._jobs.values()]
        if user_id is not None:
            jobs = [job for job in jobs if job.user_id == user_id]
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

    async def create_activity(
        self,
        *,
        event_type: str,
        description: str,
        actor_user_id: int | None = None,
        subject_user_id: int | None = None,
        file_id: str | None = None,
        job_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        created_at = utc_now().isoformat()
        serialized = json.dumps(metadata or {}, indent=2, sort_keys=True)
        async with self._db_lock:
            await asyncio.to_thread(
                self._create_activity_sync,
                created_at,
                actor_user_id,
                subject_user_id,
                file_id,
                job_id,
                event_type,
                description,
                serialized,
            )

    def _create_activity_sync(
        self,
        created_at: str,
        actor_user_id: int | None,
        subject_user_id: int | None,
        file_id: str | None,
        job_id: str | None,
        event_type: str,
        description: str,
        metadata: str,
    ) -> None:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO activity_log (
                    created_at,
                    actor_user_id,
                    subject_user_id,
                    file_id,
                    job_id,
                    event_type,
                    description,
                    metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    actor_user_id,
                    subject_user_id,
                    file_id,
                    job_id,
                    event_type,
                    description,
                    metadata,
                ),
            )

    async def list_activity(self, limit: int = 200) -> list[ActivityRecord]:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            rows = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT
                        a.*,
                        actor.username AS actor_username,
                        subject.username AS subject_username
                    FROM activity_log AS a
                    LEFT JOIN users AS actor ON actor.id = a.actor_user_id
                    LEFT JOIN users AS subject ON subject.id = a.subject_user_id
                    ORDER BY a.created_at DESC, a.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            )
        return [self._activity_from_row(row) for row in rows]

    async def admin_overview(self) -> AdminOverview:
        if self._conn is None:
            raise RuntimeError("JobStore database is not initialized.")
        async with self._db_lock:
            row = await asyncio.to_thread(
                lambda: self._conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_users,
                        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_users,
                        SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_users,
                        (SELECT COUNT(*) FROM user_files) AS total_files,
                        (SELECT COUNT(*) FROM jobs) AS total_runs,
                        (SELECT COUNT(*) FROM jobs WHERE phase IN ('queued', 'running')) AS active_runs
                    FROM users
                    """
                ).fetchone()
            )
        return AdminOverview.model_validate(
            {
                "pending_users": row["pending_users"] or 0,
                "approved_users": row["approved_users"] or 0,
                "suspended_users": row["suspended_users"] or 0,
                "total_files": row["total_files"] or 0,
                "total_runs": row["total_runs"] or 0,
                "active_runs": row["active_runs"] or 0,
            }
        )

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
            INSERT INTO jobs (id, user_id, file_id, created_at, phase, source_filename, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                file_id = excluded.file_id,
                created_at = excluded.created_at,
                phase = excluded.phase,
                source_filename = excluded.source_filename,
                payload = excluded.payload
            """,
            (
                snapshot.id,
                snapshot.user_id,
                snapshot.file_id,
                payload["created_at"],
                payload["phase"],
                payload["source_filename"],
                json.dumps(payload, indent=2),
            ),
        )
        self._conn.execute(
            "UPDATE user_files SET updated_at = ? WHERE id = ?",
            (utc_now().isoformat(), snapshot.file_id),
        )

    def _user_from_row(self, row: sqlite3.Row) -> UserRecord:
        return UserRecord.model_validate(
            {
                "id": row["id"],
                "username": row["username"],
                "role": row["role"],
                "status": row["status"],
                "created_at": row["created_at"],
                "approved_at": row["approved_at"],
                "approved_by_user_id": row["approved_by_user_id"],
                "last_login_at": row["last_login_at"],
                "render_file_count": row["render_file_count"] if "render_file_count" in row.keys() else 0,
                "run_count": row["run_count"] if "run_count" in row.keys() else 0,
            }
        )

    def _session_from_row(self, row: sqlite3.Row) -> UserSessionRecord:
        return UserSessionRecord.model_validate(dict(row))

    def _file_from_row(self, row: sqlite3.Row) -> UserFileRecord:
        return UserFileRecord.model_validate(dict(row))

    def _activity_from_row(self, row: sqlite3.Row) -> ActivityRecord:
        metadata = {}
        if row["metadata"]:
            try:
                metadata = json.loads(row["metadata"])
            except json.JSONDecodeError:
                metadata = {}
        return ActivityRecord.model_validate(
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "event_type": row["event_type"],
                "description": row["description"],
                "actor_user_id": row["actor_user_id"],
                "actor_username": row["actor_username"],
                "subject_user_id": row["subject_user_id"],
                "subject_username": row["subject_username"],
                "file_id": row["file_id"],
                "job_id": row["job_id"],
                "metadata": metadata,
            }
        )
