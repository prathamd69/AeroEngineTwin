import uuid
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request, Query
import io
import polars as pl
import time

from src.services import SimulationIngestor, SimulationIngestionError
from src.services import SimulationDownsampler
from src.utils import serialize_telemetry
from src.schemas import SimulationResponse, SENSOR_COLUMNS

from config import (SIM_BUCKET,
                    MIN_BUCKETS, MAX_BUCKETS, DEFAULT_BUCKETS,
                    DEFAULT_START, DEFAULT_END)

router = APIRouter()


def _parse_run_id(run_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(400, f"'{run_id}' is not a valid run_id (expected a UUID).")


@router.post("/ingest")
async def ingest_run(request: Request, file: UploadFile = File(...), session_name: str = Form(...),):
    run_id = uuid.uuid4()

    raw_bytes = await file.read()  # read once, reused for both sinks below

    ingestor = SimulationIngestor()

    try:
        row_count = await ingestor.ingest_to_db(
            run_id, session_name, io.BytesIO(raw_bytes), request.app.state.db_pool
        )
    except SimulationIngestionError as e:
        raise HTTPException(status_code=422, detail=str(e))

    archive_key = f"{run_id}.parquet"
    try:
        request.app.state.s3_client.put_object(
            Bucket=SIM_BUCKET,
            Key=archive_key,
            Body=raw_bytes,
        )
    except Exception as e:
        ingestor.logger.error(f"Raw archive to SeaweedFS failed for run_id={run_id}: {e}")
        return {
            "run_id": str(run_id),
            "session_name": session_name,
            "rows_ingested": row_count,
            "archived": False,
            "archive_error": str(e),
        }

    # Only flip archived/archive_key once the S3 write actually succeeded —
    # this is why it isn't part of the ingest_to_db transaction above.
    async with request.app.state.db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE simulation_runs SET archived = TRUE, archive_key = $1 WHERE run_id = $2;",
            archive_key, run_id,
        )

    return {"run_id": str(run_id), "session_name": session_name, "rows_ingested": row_count, "archived": True}


@router.get("/runs")
async def list_runs(request: Request):
    """Lists every ingested run's metadata — reads only simulation_runs,
    never touches the simulations hypertable."""
    async with request.app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT run_id, session_name, row_count, t_start, t_end, archived, archive_key
            FROM simulation_runs
            ORDER BY ingested_at DESC;
            """
        )

    return [
        {
            "run_id": str(r["run_id"]),
            "session_name": r["session_name"],
            "row_count": r["row_count"],
            "t_start": r["t_start"],
            "t_end": r["t_end"],
            "archived": r["archived"],
            "archive_key": r["archive_key"],
        }
        for r in rows
    ]


@router.get("/runs/{run_id}/telemetry", response_model=SimulationResponse)
async def get_telemetry(
    request: Request,
    run_id: str,
    start: float = Query(DEFAULT_START),
    end: float = Query(DEFAULT_END),
    target_buckets: int = Query(DEFAULT_BUCKETS),
    channels: str | None = Query(None, description="Comma-separated channel names; omit for all"),
):
    if end <= start:
        raise HTTPException(400, "end must be greater than start")
    parsed_run_id = _parse_run_id(run_id)
    target_buckets = max(MIN_BUCKETS, min(target_buckets, MAX_BUCKETS))

    io_start = time.perf_counter()
    async with request.app.state.db_pool.acquire() as conn:
        requested_channels = [c.strip() for c in channels.split(",")] if channels else SENSOR_COLUMNS
        invalid = set(requested_channels) - set(SENSOR_COLUMNS)
        if invalid:
            raise HTTPException(400, f"Unknown channels: {invalid}")
        col_list = ", ".join(["t"] + requested_channels)
        rows = await conn.fetch(
            f"SELECT {col_list} FROM simulations WHERE run_id = $1 AND t BETWEEN $2 AND $3 ORDER BY t;",
            parsed_run_id, start, end,
        )
    io_duration = (time.perf_counter() - io_start) * 1000

    if not rows:
        raise HTTPException(404, f"No data for run_id={run_id} in range [{start}, {end}]")

    df = pl.DataFrame([dict(r) for r in rows])

    cols_to_drop = [col for col in ["run_id", "ingested_at"] if col in df.columns]
    if cols_to_drop:
        df = df.drop(cols_to_drop)

    downsampler = SimulationDownsampler()
    result_df, mode = downsampler.query_dataframe(df, target_buckets)

    downsampler.logger.info(f"DB fetch for [{run_id}] {start}-{end}s: {io_duration:.2f}ms")

    return serialize_telemetry(result_df, run_id, mode, target_buckets)


@router.delete("/runs/{run_id}")
async def delete_run(request: Request, run_id: str):
    """
    Deletes a simulation's rows from TimescaleDB, its summary row from
    simulation_runs, and its archived Parquet file from SeaweedFS.
    Exposed for future use — use src/scripts/delete_simulation.py from
    the terminal for now.
    """
    parsed_run_id = _parse_run_id(run_id)

    async with request.app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            deleted_run = await conn.fetchrow(
                "DELETE FROM simulation_runs WHERE run_id = $1 RETURNING row_count;",
                parsed_run_id,
            )
            await conn.execute("DELETE FROM simulations WHERE run_id = $1;", parsed_run_id)

    if deleted_run is None:
        raise HTTPException(404, f"No data found for run_id={run_id}")

    archive_deleted = True
    archive_error = None
    try:
        request.app.state.s3_client.delete_object(Bucket=SIM_BUCKET, Key=f"{run_id}.parquet")
    except Exception as e:
        archive_deleted = False
        archive_error = str(e)

    response = {
        "run_id": run_id,
        "rows_deleted": deleted_run["row_count"],
        "archive_deleted": archive_deleted,
    }
    if archive_error:
        response["archive_error"] = archive_error
    return response