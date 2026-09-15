import asyncio
import sys
import uuid
import asyncpg
import boto3
from botocore.client import Config

from src.utils import configLogger
from config import (DATABASE_URL, SIM_BUCKET,
                    AWS_SECRET_KEY, AWS_ACCESS_KEY,
                    SEAWEED_ENDPOINT, REGION_NAME)

logger = configLogger(__file__)


async def delete_simulation(run_id: str) -> None:
    try:
        parsed_run_id = uuid.UUID(run_id)
    except ValueError:
        logger.error(f"'{run_id}' is not a valid UUID.")
        return

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        async with conn.transaction():
            deleted_run = await conn.fetchrow(
                "DELETE FROM simulation_runs WHERE run_id = $1 RETURNING row_count;",
                parsed_run_id,
            )
            await conn.execute("DELETE FROM simulation_runs WHERE run_id = $1;", parsed_run_id)
    finally:
        await conn.close()

    if deleted_run is None:
        logger.warning(f"No DB rows found for run_id={run_id}. Nothing to archive-delete.")
        return

    logger.info(f"Deleted run_id={run_id} ({deleted_run['row_count']} telemetry rows).")

    s3 = boto3.client(
        's3',
        endpoint_url=SEAWEED_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION_NAME
    )
    try:
        s3.delete_object(Bucket=SIM_BUCKET, Key=f"{run_id}.parquet")
        logger.info(f"Deleted archived object '{run_id}.parquet' from bucket '{SIM_BUCKET}'.")
    except Exception as e:
        logger.error(f"Failed to delete archive for run_id={run_id}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m src.scripts.delete_simulation <run_id>")
        sys.exit(1)

    asyncio.run(delete_simulation(sys.argv[1]))