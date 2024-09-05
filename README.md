# Distributed-Log-Analysis
This project demonstrates a distributed system for analyzing large-scale log data using PySpark and MongoDB. The system performs log ingestion, analytics (log aggregation, traffic analysis, error tracking), and optimizes performance through caching and partitioning.

## Dataset

The dataset used in this project is the **NASA HTTP Logs**, which can be downloaded from [here](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html). These logs contain server request data from NASA's web servers, making them ideal for log aggregation, traffic analysis, and error tracking.

## Requirements

### Software

- Python 3.x
- MongoDB
- Apache Spark with PySpark
- MongoDB Spark Connector

### Python Libraries

- `pyspark`
- `pymongo`
- `re` (for regex parsing)

To install required packages, run:
```bash
pip install pymongo pyspark

