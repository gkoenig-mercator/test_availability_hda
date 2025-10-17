import os
from datetime import datetime

def get_duration_in_seconds_from_two_utc(start_time, end_time):
    duration = end_time - start_time

    duration_seconds = duration.total_seconds()

    return int(duration_seconds)

def get_number_of_datasets_downloaded(data_dir, filename="Datasets_availability.csv"):
    file_path = os.path.join(data_dir, filename)
    with open(file_path, "r", encoding="utf-8") as f:
         num_rows = sum(1 for _ in f) - 1 

    return num_rows

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)