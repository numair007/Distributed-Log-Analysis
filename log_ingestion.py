# log_ingestion.py
from pymongo import MongoClient
import re

# MongoDB connection
client = MongoClient('mongodb://127.0.0.1:27017/')
db = client.logsdb
collection = db.access_logs

# Regex pattern for NASA HTTP logs
log_pattern = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+ \S+ \S+)" (\d{3}) (\d+|-)$'

# Function to parse a log line
def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return {
            "host": match.group(1),
            "identity": match.group(2),
            "user": match.group(3),
            "datetime": match.group(4),
            "request": match.group(5),
            "status": int(match.group(6)),
            "bytes": int(match.group(7)) if match.group(7) != '-' else 0
        }
    return None

# Ingest log data into MongoDB
def ingest_logs(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            log_entry = parse_log_line(line)
            if log_entry:
                collection.insert_one(log_entry)

if __name__ == "__main__":
    log_file_path = 'path_to_nasa_http_log_file'  # Replace with actual path
    ingest_logs(log_file_path)
    print("Log data successfully ingested into MongoDB.")
