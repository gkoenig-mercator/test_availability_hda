import pandas as pd
from sqlalchemy import create_engine, insert
import os
import csv
import uuid
from dotenv import load_dotenv
from database_management.database_creation import testing_metadata, datasets_tested

load_dotenv()

api_key = os.getenv("API_KEY")
username = os.environ.get("DATABASE_USERNAME", "project-test-availability-edito")
password = os.environ.get("DATABASE_PASSWORD","hi49nwyqwpv8psk4o7r3")
database_url = os.environ.get("DATABASE_URL","postgresql-850370.project-test-availability-edito")
database_name = os.environ.get("DATABASE_NAME","defaultdb")
database_port = os.environ.get("DATABASE_PORT"," 5432")

table_name = "Data_accessibility_tests"
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{database_url}:{database_port}/{database_name}"
)

def append_test_metadata_in_db(start_time, end_time, linux_version, hda_version,
                               script_version, run_duration, number_of_datasets):

    with engine.begin() as conn:
        test_run = {
            "start_time": start_time,
            "end_time": end_time,
            "run_duration_seconds": run_duration,
            "numbers_of_datasets": number_of_datasets,
            "linux_version": linux_version,
            "hda_version": hda_version,
            "script_version": script_version,
        }
        result = conn.execute(insert(testing_metadata).values(test_run))
        test_id = result.inserted_primary_key[0]  # UUID of the new test run

    return test_id

def append_dataset_downloadable_status_in_db(data_dir, test_id):
    """
    Pandas-optimized version - perfect for teams comfortable with pandas
    and datasets up to a few thousand rows
    """
    
    file_path = os.path.join(data_dir, "Datasets_availability.csv")
    dataset_rows = []

    # Read CSV and prepare rows
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dataset_rows.append({
                "id": row["id"],          # unique ID for this dataset_test row
                "test_id": test_id,               # link to the test_run
                "Dataset_id": row["Dataset_id"],
                "Error": row["Error"],
                "Min_Lon":row["Min Lon"],
                "Max_Lon":row["Max Lon"],
                "Min_Lat":row["Min Lat"],
                "Max_Lat":row["Max Lat"],
                "Start":row["Start"],
                "End":row["End"],
                "Volume":row["Volume (GB)"],
                "Query":row["Query"]
             })

    # Insert into database
    if dataset_rows:
        with engine.begin() as conn:
            conn.execute(insert(datasets_tested), dataset_rows)

if __name__ == "__main__":

    test_id = append_test_metadata_in_db(start_time, end_time,
                                         linux_version, hda_version,
                                         script_version, run_duration, number_of_datasets)
    append_dataset_downloadable_status_in_db('data', test_id)