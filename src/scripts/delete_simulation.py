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
    """Deletes a run's DB rows first, then its archived CSV. Same order
    and logic as the /runs/{run_id} DELETE endpoint, just callable
    directly from the terminal without going through the API."""
    try:
        parsed_run_id = uuid.UUID(run_id)
    except ValueError:
        logger.error(f"'{run_id}' is not a valid UUID.")
        return

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        result = await conn.execute("DELETE FROM simulations WHERE run_id = $1;", parsed_run_id)
        deleted_rows = int(result.split(" ")[-1])
    finally:
        await conn.close()

    if deleted_rows == 0:
        logger.warning(f"No DB rows found for run_id={run_id}. Nothing to archive-delete.")
        return

    logger.info(f"Deleted {deleted_rows} rows from TimescaleDB for run_id={run_id}")

    s3 = boto3.client(
        's3',
        endpoint_url=SEAWEED_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION_NAME
    )
    try:
        s3.delete_object(Bucket=SIM_BUCKET, Key=f"{run_id}.csv")
        logger.info(f"Deleted archived object '{run_id}.csv' from bucket '{SIM_BUCKET}'.")
    except Exception as e:
        logger.error(f"Failed to delete archive for run_id={run_id}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m src.scripts.delete_simulation <run_id>")
        sys.exit(1)

    asyncio.run(delete_simulation(sys.argv[1]))