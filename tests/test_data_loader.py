import pytest
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory

from src.data_loader import DataLoader


@pytest.fixture
def parquet_interview_data():
    """Create temp directory with valid Parquet files matching DataLoader schema."""
    with TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)

        data = [
            {
                "clip_name": "clip_001",
                "frame_id": 1,
                "vehicle_type": "car",
                "detection": True,
                "distance": 30,
            },
            {
                "clip_name": "clip_001",
                "frame_id": 2,
                "vehicle_type": "truck",
                "detection": False,
                "distance": 55,
            },
            {
                "clip_name": "clip_002",
                "frame_id": 3,
                "vehicle_type": "bike",
                "detection": True,
                "distance": 10,
            },
        ]

        df = pd.DataFrame(data)

        # Save multiple parquet files to simulate multiple parts
        for i in range(2):
            df.to_parquet(dir_path / f"interview_data_part_{i}.parquet", index=False)

        yield dir_path


def test_load_interview_parquet_data(parquet_interview_data):
    with DataLoader(str(parquet_interview_data), db_path=":memory:") as loader:
        loader.load_data("interview_table")

        df = loader.get_connection().execute("SELECT * FROM interview_table").fetchdf()

    # 3 rows per file, 2 files = 6 rows total
    assert len(df) == 6
    # Columns should exactly match required schema keys
    assert set(df.columns) == set(DataLoader.REQUIRED_COLUMNS.keys())
    # Validate some data values
    assert "clip_001" in df["clip_name"].values
    assert df["distance"].between(1, 100).all()
    assert df["vehicle_type"].isin(["car", "truck", "bike"]).all()


def test_load_data_no_parquet_files():
    with TemporaryDirectory() as tmpdir:
        loader = DataLoader(tmpdir, db_path=":memory:")
        with pytest.raises(FileNotFoundError):
            loader.load_data("interview_table")


def test_load_data_invalid_parquet_file(tmp_path):
    # Write a bogus file named as parquet
    bad_file = tmp_path / "bad.parquet"
    bad_file.write_text("this is not a parquet file")

    loader = DataLoader(str(tmp_path), db_path=":memory:")

    with pytest.raises(RuntimeError, match="No valid Parquet files found"):
        loader.load_data("interview_table")


def test_load_data_validation_failure(tmp_path):
    # Create a parquet missing required columns
    df = pd.DataFrame(
        {
            "clip_name": ["clip_001"],
            "frame_id": [1],
            # missing vehicle_type, detection, distance
        }
    )
    bad_file = tmp_path / "incomplete.parquet"
    df.to_parquet(bad_file)

    loader = DataLoader(str(tmp_path), db_path=":memory:")

    with pytest.raises(RuntimeError, match="No valid Parquet files found"):
        loader.load_data("interview_table")


def test_validate_file_accepts_good_file(parquet_interview_data):
    loader = DataLoader(str(parquet_interview_data), db_path=":memory:")
    file_path = next(Path(parquet_interview_data).glob("*.parquet"))
    assert loader._validate_file(file_path) is True


def test_validate_file_rejects_bad_file(tmp_path):
    # Create parquet with invalid distance values
    df = pd.DataFrame(
        {
            "clip_name": ["clip_001"],
            "frame_id": [1],
            "vehicle_type": ["car"],
            "detection": [True],
            "distance": [500],
        }
    )
    bad_file = tmp_path / "bad_distance.parquet"
    df.to_parquet(bad_file)

    loader = DataLoader(str(tmp_path), db_path=":memory:")
    assert loader._validate_file(bad_file) is False
