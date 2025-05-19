from src.client import Client

from tests.test_utils import duckdb_conn

def test_query_detection_stats_basic(duckdb_conn):
    client = Client(duckdb_conn)
    df = client.query_detection_stats()

    assert not df.empty
    assert "success_rate" in df.columns

    # Check some expected groups
    assert set(df["vehicle_type"]).issubset({"car", "truck"})
    assert set(df["clip_name"]).issubset({"clip1", "clip2", "clip3"})


def test_query_detection_stats_filters(duckdb_conn):
    client = Client(duckdb_conn)

    # Filter by vehicle_type
    df = client.query_detection_stats(vehicle_types=["car"])
    assert all(df["vehicle_type"] == "car")

    # Filter by clip_name
    df = client.query_detection_stats(clip_names=["clip2"])
    assert all(df["clip_name"] == "clip2")

    # Filter by frame_id range
    df = client.query_detection_stats(min_frame_id=2, max_frame_id=3)
    assert all((df["clip_name"].isin(["clip1", "clip2", "clip3"])))


def test_query_detection_stats_distance_bin_and_min_frames(duckdb_conn):
    client = Client(duckdb_conn)

    # Use custom distance bin size and min_frames to filter results
    df = client.query_detection_stats(distance_bin_size=20, min_frames=2)

    # All counts must be >= min_frames
    assert (df["total_frames"] >= 2).all()

    # distance_bin multiples of 20
    assert all(df["distance_bin"] % 20 == 0)


def test_query_detection_stats_no_data(duckdb_conn):
    client = Client(duckdb_conn)

    # Filter that yields no results
    df = client.query_detection_stats(vehicle_types=["nonexistent_vehicle"])
    assert df.empty
    assert list(df.columns) == [
        "vehicle_type",
        "clip_name",
        "distance_bin",
        "total_frames",
        "detected_frames",
        "success_rate",
    ]
