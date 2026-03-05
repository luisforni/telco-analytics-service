

import logging
import os
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.api.routes import router
from app.services.kafka_consumer import KafkaConsumerService
from app.services.db import Database

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("analytics_service.starting")
    db = Database(dsn=os.getenv("DATABASE_URL", "postgresql://telco:telco@postgres:5432/telco_analytics"))
    await db.connect()
    app.state.db = db

    consumer = KafkaConsumerService(
        brokers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
        topics=[
            os.getenv("TOPIC_CALL_EVENTS", "call-events"),
            os.getenv("TOPIC_CDR", "cdr-events"),
            os.getenv("TOPIC_ORCH_EVENTS", "orchestrator-events"),
            os.getenv("TOPIC_TRANSCRIPTIONS", "transcription-events"),
        ],
        group_id="analytics-service",
        db=db,
    )
    import asyncio
    consumer_task = asyncio.create_task(consumer.start())
    app.state.consumer = consumer
    app.state.consumer_task = consumer_task
    yield
    log.info("analytics_service.stopping")
    await consumer.stop()
    await db.disconnect()

app = FastAPI(
    title="Telco Analytics Service",
    description="Real-time call center metrics, KPIs and historical reporting",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/metrics", make_asgi_app())
app.include_router(router, prefix="/api/v1")

@app.get("/healthz")
async def health():
    return {"status": "ok", "service": "analytics-service"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8008")), workers=1)
