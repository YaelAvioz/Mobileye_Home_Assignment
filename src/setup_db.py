import argparse
import os
from data_loader import DataLoader

DB_PATH = os.environ.get("DB_PATH", "duckdb/interview_table.duckdb")


def main():
    parser = argparse.ArgumentParser(description="Detection Success Analyzer DB setup")

    parser.add_argument("--data-path", type=str, required=True, help="Path to directory containing parquet files.")

    args = parser.parse_args()

    loader = DataLoader(data_path=args.data_path, db_path=DB_PATH)
    loader.load_data()


if __name__ == "__main__":
    main()
