

import structlog
from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse

log = structlog.get_logger()
router = APIRouter(tags=["Analytics"])

def _db(request: Request):
    return request.app.state.db

@router.get("/stats/realtime")
async def realtime_stats(request: Request):

    stats = await _db(request).get_realtime_stats()
    return stats

@router.get("/stats/hourly")
async def hourly_volume(request: Request, hours: int = Query(default=24, ge=1, le=168)):

    data = await _db(request).get_hourly_volume(hours)
    return data

@router.get("/stats/agents")
async def agent_stats(request: Request):

    data = await _db(request).get_agent_stats()
    return data

@router.get("/kpis")
async def kpis(request: Request):

    db = _db(request)
    stats = await db.get_realtime_stats()
    aht = round(stats.get("avg_handle_time") or 0, 1)
    return {
        "aht_seconds": aht,
        "calls_last_hour": stats.get("calls_last_hour", 0),
        "active_calls": stats.get("active_calls", 0),
        "completed_today": stats.get("completed_today", 0),
        "note": "CSAT and FCR require sentiment data integration",
    }

@router.get("/queries/{query_name}")
async def run_saved_query(query_name: str, request: Request):

    import os, aiofiles
    query_path = os.path.join(os.path.dirname(__file__), "..", "..", "queries", f"{query_name}.sql")
    if not os.path.exists(query_path):
        return JSONResponse(status_code=404, content={"error": f"Query '{query_name}' not found"})
    async with aiofiles.open(query_path) as f:
        sql = await f.read()
    db = _db(request)
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [dict(r) for r in rows]
