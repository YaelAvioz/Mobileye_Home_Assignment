# Use an official Python image
FROM python:3.11-slim

# Copy the requirements file 
COPY requirements.txt requirements.txt

# Install DuckDB and dependencies
RUN pip install -r requirements.txt

# Default command
CMD ["/bin/bash"]
