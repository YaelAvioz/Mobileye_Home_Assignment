import pytest
import duckdb


@pytest.fixture(scope="module")
def duckdb_conn():
    # Create in-memory DuckDB connection for testing
    conn = duckdb.connect(database=":memory:")

    # Create test table schema (matching your interview_table schema)
    conn.execute("""
    CREATE TABLE interview_table (
        clip_name VARCHAR,
        frame_id INTEGER,
        vehicle_type VARCHAR,
        detection BOOLEAN,
        distance INTEGER
    )
    """)

    # Insert sample test data
    test_data = [
        ("clip1", 1, "car", True, 5),
        ("clip1", 2, "car", False, 15),
        ("clip1", 3, "truck", True, 25),
        ("clip2", 1, "car", True, 35),
        ("clip2", 2, "truck", False, 45),
        ("clip2", 3, "truck", True, 55),
        ("clip3", 1, "car", True, 65),
        ("clip3", 2, "car", True, 75),
        ("clip3", 3, "truck", False, 85),
    ]

    conn.executemany("INSERT INTO interview_table VALUES (?, ?, ?, ?, ?)", test_data)

    yield conn

    conn.close()
