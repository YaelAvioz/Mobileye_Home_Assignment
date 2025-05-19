import argparse
import logging
import os
import duckdb
from client import Client
from utils import validate_args, configure_logging

DB_PATH = os.environ.get("DB_PATH", "duckdb/interview_table.duckdb")


def main():
    parser = argparse.ArgumentParser(description="Detection Success Analyzer")

    parser.add_argument("--vehicles", type=str, nargs="*", default=None, help="Vehicle types to include, e.g. --vehicles car truck")
    parser.add_argument("--clip-names", type=str, nargs="*", default=None, help="Clip names to filter, e.g. --clip-names clip1 clip2")
    parser.add_argument("--min-frame-id", type=int, default=None, help="Minimum frame index to include")
    parser.add_argument("--max-frame-id", type=int, default=None, help="Maximum frame index to include")
    parser.add_argument("--min-distance", type=int, default=1, help="Minimum distance to consider (default: 1)")
    parser.add_argument("--max-distance", type=int, default=100, help="Maximum distance to consider (default: 100)")
    parser.add_argument("--distance-bin-size", type=int, default=10, help="Distance bin size (default: 10)")
    parser.add_argument("--min-frames", type=int, default=1, help="Minimum number of frames per bin to consider.")
    parser.add_argument("-v", "--verbose", action="store_const", dest="loglevel", const=logging.INFO, default=logging.WARNING, help="Enable INFO level logging")

    args = parser.parse_args()
    validate_args(args, parser)

    configure_logging(args.loglevel)
        
    with duckdb.connect(database=DB_PATH) as conn:
        logging.info("DuckDB connection established.")

        client = Client(conn)
        result = client.query_detection_stats(
            vehicle_types=args.vehicles,
            clip_names=args.clip_names,
            min_frame_id=args.min_frame_id,
            max_frame_id=args.max_frame_id,
            min_distance=args.min_distance,
            max_distance=args.max_distance,
            distance_bin_size=args.distance_bin_size,
            min_frames=args.min_frames,
        )

        logging.info("Query executed successfully. Showing results:")
        print(result.to_string())


if __name__ == "__main__":
    main()
