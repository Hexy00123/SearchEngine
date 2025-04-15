#!/bin/bash
set -e

# Start ssh server
service ssh restart > /dev/null 2>&1

# Starting the services
echo "APP: starting services..."
bash start-services.sh > /dev/null 2>&1

# Creating a virtual environment
echo "APP: Creating venv..."
python3 -m venv .venv > /dev/null 2>&1
source .venv/bin/activate > /dev/null 2>&1

# Install packages
echo "APP: Installing dependencies..."
pip install -r requirements.txt  # > /dev/null 2>&1

# Package the virtual env
echo "APP: Packing venv..."
if venv-pack -o /app/.venv.tar.gz > /dev/null 2>&1; then
    echo "APP: Virtual environment packed successfully."
else
    echo "APP: Env already packed"
fi

# Collect data
echo "APP: Preparing data..."
bash prepare_data.sh # > /dev/null 2>&1

# Run the indexer
echo "APP: Indexing data..."
bash index/index.sh /index/data > /dev/null 2>&1

# Run the ranker
echo "APP: Running the queries..."
bash search.sh "exciting adventure on the airplane"
bash search.sh "criminal film about police"
bash search.sh "ancient egypt town"
