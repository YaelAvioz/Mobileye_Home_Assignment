import argparse
import logging
from typing import Union


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """
    Validate CLI arguments after parsing.

    Raises:
        SystemExit via parser.error if any validation fails.
    """
    if args.max_distance < args.min_distance:
        parser.error(f"--max-distance ({args.max_distance}) must be >= --min-distance ({args.min_distance})")

    if args.min_frame_id is not None and args.max_frame_id is not None:
        if args.max_frame_id < args.min_frame_id:
            parser.error(f"--max-frame-id ({args.max_frame_id}) must be >= --min-frame-id ({args.min_frame_id})")

    if args.distance_bin_size <= 0:
        parser.error("--distance-bin-size must be a positive integer.")

    if args.min_frames < 1:
        parser.error("--min-frames must be at least 1.")


def configure_logging(level: Union[int, str]) -> None:
    """
    Configure logging format and level.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
