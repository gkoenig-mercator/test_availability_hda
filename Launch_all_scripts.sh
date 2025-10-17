#!/bin/bash

set -e

echo "=== Starting Python script ==="

python metadata_check.py
sleep 30
python main.py
python Adds_data_in_database.py
python deploy_error_table_to_markdown.py

echo "All scripts completed successfully!"
