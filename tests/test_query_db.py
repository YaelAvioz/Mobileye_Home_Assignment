from tests.test_utils import duckdb_conn

def test_basic_query_limit_10(duckdb_conn):
    """Step: Basic query to select first 10 rows from interview_table."""
    df = duckdb_conn.execute("SELECT * FROM interview_table LIMIT 10").df()
    assert not df.empty
    assert len(df) <= 10
    # Check columns exist
    expected_cols = ["clip_name", "frame_id", "vehicle_type", "detection", "distance"]
    assert list(df.columns) == expected_cols


def test_detection_success_pivot_table(duckdb_conn):
    """
    Test a query that pivots detection success rates by vehicle_type across distance bins.
    The output columns represent distance bins (e.g., '1-10', '11-20', ... '91-100'),
    and values represent %True detection success in each bin.
    """
    query = """
    WITH binned AS (
        SELECT
            vehicle_type,
            CASE
                WHEN distance BETWEEN 1 AND 10 THEN '1-10'
                WHEN distance BETWEEN 11 AND 20 THEN '11-20'
                WHEN distance BETWEEN 21 AND 30 THEN '21-30'
                WHEN distance BETWEEN 31 AND 40 THEN '31-40'
                WHEN distance BETWEEN 41 AND 50 THEN '41-50'
                WHEN distance BETWEEN 51 AND 60 THEN '51-60'
                WHEN distance BETWEEN 61 AND 70 THEN '61-70'
                WHEN distance BETWEEN 71 AND 80 THEN '71-80'
                WHEN distance BETWEEN 81 AND 90 THEN '81-90'
                WHEN distance BETWEEN 91 AND 100 THEN '91-100'
                ELSE 'other'
            END AS distance_bin,
            detection
        FROM interview_table
    )
    SELECT
        vehicle_type,
        100.0 * SUM(CASE WHEN distance_bin = '1-10' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '1-10' THEN 1 ELSE 0 END), 0) AS "1-10",
        100.0 * SUM(CASE WHEN distance_bin = '11-20' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '11-20' THEN 1 ELSE 0 END), 0) AS "11-20",
        100.0 * SUM(CASE WHEN distance_bin = '21-30' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '21-30' THEN 1 ELSE 0 END), 0) AS "21-30",
        100.0 * SUM(CASE WHEN distance_bin = '31-40' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '31-40' THEN 1 ELSE 0 END), 0) AS "31-40",
        100.0 * SUM(CASE WHEN distance_bin = '41-50' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '41-50' THEN 1 ELSE 0 END), 0) AS "41-50",
        100.0 * SUM(CASE WHEN distance_bin = '51-60' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '51-60' THEN 1 ELSE 0 END), 0) AS "51-60",
        100.0 * SUM(CASE WHEN distance_bin = '61-70' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '61-70' THEN 1 ELSE 0 END), 0) AS "61-70",
        100.0 * SUM(CASE WHEN distance_bin = '71-80' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '71-80' THEN 1 ELSE 0 END), 0) AS "71-80",
        100.0 * SUM(CASE WHEN distance_bin = '81-90' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '81-90' THEN 1 ELSE 0 END), 0) AS "81-90",
        100.0 * SUM(CASE WHEN distance_bin = '91-100' AND detection THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN distance_bin = '91-100' THEN 1 ELSE 0 END), 0) AS "91-100"
    FROM binned
    GROUP BY vehicle_type
    ORDER BY vehicle_type;
    """

    df = duckdb_conn.execute(query).df()

    expected_bins = [f"{i}-{i + 9}" for i in range(1, 100, 10)]
    expected_columns = ["vehicle_type"] + expected_bins

    # Check columns match expected
    assert list(df.columns) == expected_columns

    # Check vehicle_type values are subset of known types in test data
    known_types = {"car", "truck"}
    assert set(df["vehicle_type"]).issubset(known_types)

    # Values should be floats or None (NULL)
    for col in expected_bins:
        # Values can be NULL if no data for that bin; dropna for testing range
        vals = df[col].dropna()
        assert vals.between(0, 100).all()
