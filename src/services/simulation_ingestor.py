from pathlib import Path
from typing import BinaryIO, Union
import polars as pl
import asyncpg
import io
import uuid
from src.utils import configLogger
from src.schemas import SIMULATION_SCHEMA


class SimulationIngestionError(Exception):
    """Custom exception for ingestion failures."""
    pass


SENSOR_COLUMNS = [f for f in SIMULATION_SCHEMA.keys() if f != "t"]


class SimulationIngestor:
    """
    Handles the ingestion, validation, cleaning, and artifact generation
    of raw aero engine telemetry data.
    Supports TimescaleDB write (FastAPI ingestion endpoint, new).
    """

    def __init__(self):
        self.logger = configLogger(self.__class__.__name__)

    def _read_and_validate(self, source: Union[Path, BinaryIO]) -> pl.DataFrame:
        self.logger.debug("Reading Parquet and enforcing SIMULATION_SCHEMA...")

        if isinstance(source, Path):
            raw_bytes = source.read_bytes()
        else:
            source.seek(0)  # in case anything upstream already read the stream
            raw_bytes = source.read()

        try:
            df = pl.read_parquet(io.BytesIO(raw_bytes))
        except Exception as e:
            self.logger.error(f"Failed to parse Parquet file: {e}")
            raise SimulationIngestionError(f"File is not valid Parquet data: {e}")

        # Parquet is self-typed but not guaranteed to match SIMULATION_SCHEMA
        # exactly — cast explicitly, same enforcement read_csv's
        # schema_overrides used to give us. Raises SchemaError/
        # ColumnNotFoundError, both already handled by _validate_and_clean.
        df = df.cast(SIMULATION_SCHEMA)

        if df.height == 0:
            self.logger.error("Ingestion failed: Dataframe is empty.")
            raise SimulationIngestionError("The uploaded simulation file is empty.")
        return df

    def _clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies physics-safe data cleaning rules. Unchanged from original."""
        self.logger.debug(f"Applying data cleaning to {df.height} rows...")
        initial_rows = df.height
        null_counts = df.null_count()
        total_nulls = sum(null_counts.row(0))

        if total_nulls > 0:
            self.logger.warning(
                f"Schema Audit: Found {total_nulls} corrupted/null fields "
                f"across {initial_rows} rows. Initiating forward-fill coercion."
            )

        df = df.fill_null(strategy="forward")
        df = df.fill_null(0.0)
        return df

    def _validate_and_clean(self, source: Union[Path, BinaryIO]) -> pl.DataFrame:
        """Full read -> validate -> clean -> sort pipeline, sink-agnostic."""
        try:
            df = self._read_and_validate(source)
            clean_df = self._clean_data(df)
            return clean_df.sort("t")
        except pl.exceptions.ColumnNotFoundError as e:
            self.logger.error(f"Missing required sensor columns: {str(e)}")
            raise SimulationIngestionError(f"Missing required sensor columns: {str(e)}")
        except pl.exceptions.SchemaError as e:
            self.logger.error(f"Data type mismatch: {str(e)}")
            raise SimulationIngestionError(f"Data type mismatch in simulation file: {str(e)}")
        except SimulationIngestionError:
            raise
        except Exception as e:
            self.logger.exception("Unexpected error occurred during simulation ingestion.")
            raise SimulationIngestionError(f"Failed to parse telemetry data: {str(e)}")

    async def ingest_to_db(
        self, run_id: uuid.UUID, session_name: str, source: Union[Path, BinaryIO], pool: asyncpg.Pool
    ) -> int:
        """
        Writes both the simulation_runs summary row and the bulk telemetry
        insert in one transaction. No FK between the tables, so order
        between the two statements doesn't matter for correctness — kept
        as summary-row-first purely for readability.
        """
        self.logger.info(f"Starting DB ingestion for run_id={run_id}")
        clean_df = self._validate_and_clean(source)

        row_count = clean_df.height
        # Already sorted by "t" above, so first/last give min/max directly
        # without a separate min()/max() pass over the column.
        t_start = clean_df.select(pl.col("t").first()).item()
        t_end = clean_df.select(pl.col("t").last()).item()

        records = [
            (run_id, row["t"], *[row.get(c) for c in SENSOR_COLUMNS])
            for row in clean_df.iter_rows(named=True)
        ]
        columns = ["run_id", "t"] + SENSOR_COLUMNS

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO simulation_runs (run_id, session_name, row_count, t_start, t_end)
                        VALUES ($1, $2, $3, $4, $5);
                        """,
                        run_id, session_name, row_count, t_start, t_end
                    )
                    await conn.copy_records_to_table(
                        "simulations", records=records, columns=columns
                    )
        except asyncpg.PostgresError as e:
            self.logger.exception("Database write failed during ingestion.")
            raise SimulationIngestionError(f"Failed to write telemetry to database: {str(e)}")

        self.logger.info(f"Successfully ingested {row_count} rows for run_id={run_id}")
        return row_count