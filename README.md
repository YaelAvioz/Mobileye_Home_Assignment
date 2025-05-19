
# Detection Success Analyzer

A Python-based tool that calculates and exposes detection success rates for different vehicle types and distance bins, leveraging DuckDB for fast and efficient SQL-based data analysis.


## Features

-  Validates and loads Parquet datasets into DuckDB
-  Queries detection success by vehicle types, clips, and distance bins
-  Supports flexible CLI filters (frame ranges, clip names, vehicle types, etc.)
-  Includes unit tests for data loading and query logic


## 🐳 Setup with Docker

### 1. Build the Docker image

```bash
docker build -t detection-success-analyzer .
````

### 2. Run the Docker container

```bash
docker run -it --rm -v "$PWD":/app -w /app detection-success-analyzer
```


## 🛠️ Initialize the DuckDB Database

To load your Parquet data into a `.duckdb` file:

```bash
python src/setup_db.py --data-path data
```

By default, this uses the path `duckdb/interview_table.duckdb`. You can override it with an environment variable `DB_PATH`.


## 🧑‍💻 Usage

Run the analyzer on the loaded dataset using:

```bash
python src/main.py [OPTIONS]
```

### CLI Options

| Option                | Description                                 |
| --------------------- | ------------------------------------------- |
| `--vehicles`          | Vehicle types to include (e.g. `car truck`) |
| `--clip-names`        | Filter by clip names (e.g. `clip1 clip2`)   |
| `--min-frame-id`      | Minimum frame index to include              |
| `--max-frame-id`      | Maximum frame index to include              |
| `--min-distance`      | Minimum distance to consider (default: 1)   |
| `--max-distance`      | Maximum distance to consider (default: 100) |
| `--distance-bin-size` | Distance bin size (default: 10)             |
| `--min-frames`        | Minimum number of frames per bin to include |
| `-v`, `--verbose`     | Enable verbose logging                      |
| `-h`, `--help`        | Show help message and exit                  |

### Example

```bash
python src/main.py --vehicles car truck --min-distance 1 --max-distance 50 --distance-bin-size 10 --min-frames 5 -v
```


## ✅ Testing

Run all automated tests with:

```bash
pytest tests/
```

Tests cover:

* Data validation and DuckDB loading
* Query logic with filtering and binning


## 📁 Project Structure

```
Mobileye_Home_Assignment/
├── Dockerfile
├── requirements.txt
├── src/
│   ├── main.py
│   ├── setup_db.py
│   ├── utils.py
│   └── data_loader.py
├── data/
│   └── file1.parquet
│   └── file2.parquet
│   └── file3.parquet
├── duckdb/
│   └── interview_table.duckdb
├── tests/
│   ├── test_data_loader.py
│   ├── test_utils.py
│   ├── test_client.py
│   └── test_query_db.py
```
