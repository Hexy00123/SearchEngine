# big-data-assignment2-2025

# Report with code description
https://github.com/Hexy00123/SearchEngine/blob/main/report/BigData_Assignment2_report.pdf

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
- Ensure that a.parquet file is in the app/ directory
## Step 2: Run the command
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.
