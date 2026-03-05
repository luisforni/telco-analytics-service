

import asyncio
import json

import structlog
from aiokafka import AIOKafkaConsumer

from app.services.db import Database

log = structlog.get_logger()

class KafkaConsumerService:
    def __init__(self, brokers: str, topics: list[str], group_id: str, db: Database):
        self.brokers = brokers
        self.topics = topics
        self.group_id = group_id
        self.db = db
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._running = True
        log.info("kafka.consumer.started", topics=self.topics)

        async for msg in self._consumer:
            if not self._running:
                break
            try:
                await self._process(msg.topic, msg.value)
            except Exception as e:
                log.error("kafka.consumer.error", topic=msg.topic, error=str(e))

    async def stop(self):
        self._running = False
        if self._consumer:
            await self._consumer.stop()

    async def _process(self, topic: str, payload: dict):
        event_type = payload.get("event_type", "")

        if topic in ("call-events", "cdr-events"):
            await self.db.upsert_call({
                "id":           payload.get("call_id"),
                "caller_id":    payload.get("caller_id"),
                "destination":  payload.get("destination"),
                "direction":    payload.get("direction"),
                "state":        payload.get("state", "UNKNOWN"),
                "queue_id":     payload.get("queue_id"),
                "agent_id":     payload.get("agent_id"),
                "hangup_cause": payload.get("hangup_cause"),
                "start_time":   payload.get("start_time"),
                "answer_time":  payload.get("answer_time"),
                "end_time":     payload.get("end_time"),
                "bill_seconds": payload.get("bill_seconds", 0),
            })

        elif topic == "orchestrator-events":
            await self.db.upsert_call({
                "id":    payload.get("call_id"),
                "state": payload.get("to_state"),
            })

        elif event_type == "TRANSCRIPTION_COMPLETE":
            log.debug("transcription.received", call_id=payload.get("call_id"))
