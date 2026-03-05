

import asyncpg
import structlog

log = structlog.get_logger()

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            self.dsn,
            min_size=5,
            max_size=20,
            command_timeout=10,
        )
        await self._create_schema()
        log.info("database.connected")

    async def disconnect(self):
        if self._pool:
            await self._pool.close()

    async def _create_schema(self):
        async with self._pool.acquire() as conn:

    async def upsert_call(self, data: dict):
        async with self._pool.acquire() as conn:

                data.get("id"), data.get("caller_id"), data.get("destination"),
                data.get("direction"), data.get("state"), data.get("queue_id"),
                data.get("agent_id"), data.get("hangup_cause"), data.get("start_time"),
                data.get("answer_time"), data.get("end_time"), data.get("bill_seconds", 0),
            )

    async def get_realtime_stats(self) -> dict:
        async with self._pool.acquire() as conn:

            return dict(row)

    async def get_hourly_volume(self, hours: int = 24) -> list[dict]:
        async with self._pool.acquire() as conn:

            return [dict(r) for r in rows]

    async def get_agent_stats(self) -> list[dict]:
        async with self._pool.acquire() as conn:

            return [dict(r) for r in rows]
