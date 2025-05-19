from typing import List, Optional
import pandas as pd
import duckdb


class Client:
    """
    Client class for querying detection statistics from a DuckDB database table.
    """

    GROUP_FIELDS = ["vehicle_type", "clip_name"]

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize the Client with a DuckDB connection.
        """
        self.conn = conn
        self.table_name = "interview_table"

    def query_detection_stats(
        self,
        vehicle_types: Optional[List[str]] = None,
        clip_names: Optional[List[str]] = None,
        min_frame_id: Optional[int] = None,
        max_frame_id: Optional[int] = None,
        min_distance: int = 1,
        max_distance: int = 100,
        distance_bin_size: int = 10,
        min_frames: int = 1,
    ) -> pd.DataFrame:
        """
        Query detection statistics grouped by vehicle type, clip name, and distance bins.
        """
        group_fields = self.GROUP_FIELDS
        group_select = ", ".join(group_fields)
        group_by = ", ".join(group_fields + ["distance_bin"])

        base_query = f"""
        SELECT
            {group_select},
            FLOOR(distance / ?) * ? AS distance_bin,
            COUNT(*) AS total_frames,
            SUM(CASE WHEN detection THEN 1 ELSE 0 END) AS detected_frames
        FROM {self.table_name}
        WHERE distance BETWEEN ? AND ?
        """

        filters = []
        params = [distance_bin_size, distance_bin_size, min_distance, max_distance]

        def add_filter(condition: str, values):
            filters.append(condition)
            if isinstance(values, list):
                params.extend(values)
            else:
                params.append(values)

        if vehicle_types:
            placeholders = ", ".join(["?"] * len(vehicle_types))
            add_filter(f"vehicle_type IN ({placeholders})", vehicle_types)

        if clip_names:
            placeholders = ", ".join(["?"] * len(clip_names))
            add_filter(f"clip_name IN ({placeholders})", clip_names)

        if min_frame_id is not None:
            add_filter("frame_id >= ?", min_frame_id)

        if max_frame_id is not None:
            add_filter("frame_id <= ?", max_frame_id)

        if filters:
            base_query += " AND " + " AND ".join(filters)

        base_query += f"""
        GROUP BY {group_by}
        HAVING COUNT(*) >= ?
        ORDER BY {group_by}
        """
        params.append(min_frames)

        result = self.conn.execute(base_query, params).df()

        if not result.empty:
            result["success_rate"] = result["detected_frames"] / result["total_frames"]
        else:
            columns = group_fields + [
                "distance_bin",
                "total_frames",
                "detected_frames",
                "success_rate",
            ]

            result = pd.DataFrame(columns=columns)

        return result
