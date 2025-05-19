import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
from pathlib import Path
from duckdb import DuckDBPyConnection
import logging
from typing import List


class DataLoader:
    """Validates and loads Parquet files into a DuckDB table using pyarrow for validation."""

    REQUIRED_COLUMNS = {
        "clip_name": pa.string(),
        "frame_id": pa.int64(),
        "vehicle_type": pa.string(),
        "detection": pa.bool_(),
        "distance": pa.int64(),
    }

    def __init__(self, data_path: str, db_path: str):
        self.data_path: Path = Path(data_path)
        self.db_path: str = db_path
        self.conn: DuckDBPyConnection = duckdb.connect(database=db_path)
        logging.debug(f"Connected to DuckDB database at '{db_path}'")

    def __enter__(self) -> "DataLoader":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logging.debug("DuckDB connection closed.")

    def _validate_file(self, file_path: Path) -> bool:
        """Validate if a parquet file matches the required schema and distance range."""
        file_str = str(file_path)
        try:
            parquet_file = pq.ParquetFile(file_str)
            schema = parquet_file.schema_arrow

            for col, expected_type in self.REQUIRED_COLUMNS.items():
                if schema.get_field_index(col) < 0:
                    raise ValueError(f"Missing required column '{col}'")
                actual_type = schema.field(col).type
                if actual_type != expected_type:
                    raise TypeError(f"Column '{col}' has wrong type. Expected {expected_type}, got {actual_type}")

            table = pq.read_table(file_str, columns=["distance"])
            distance_series = table.column("distance").to_pandas()
            if not distance_series.between(1, 100).all():
                raise ValueError("'distance' column has values outside the allowed range 1 to 100")

            logging.debug(f"File '{file_path.name}' passed validation.")
            return True

        except Exception as e:
            logging.warning(f"Validation failed for '{file_path.name}': {e}")
            return False

    def load_data(self, table_name: str = "interview_table") -> None:
        """
        Loads validated parquet files into DuckDB as a single table.

        Raises:
            FileNotFoundError: If no parquet files found.
            RuntimeError: If no valid parquet files after validation.
            RuntimeError: If DuckDB fails to read parquet files.
        """
        files: List[Path] = list(self.data_path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No Parquet files found in directory '{self.data_path.resolve()}'")

        valid_files: List[str] = [str(f) for f in files if self._validate_file(f)]
        if not valid_files:
            raise RuntimeError("No valid Parquet files found after validation.")

        try:
            self.conn.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet($1);
                """,
                (valid_files,),
            )
            logging.info(f"Loaded {len(valid_files)} file(s) into DuckDB table '{table_name}'.")
        except duckdb.Error as e:
            raise RuntimeError(f"Failed to load Parquet files into DuckDB: {e}")

    def get_connection(self) -> DuckDBPyConnection:
        """Returns the active DuckDB connection."""
        return self.conn
