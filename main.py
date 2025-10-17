# main.py
import pandas as pd
import logging
from hda_utils.config import get_client
from hda_utils.exceptions import apply_exceptions
from hda_utils.metadata import get_geographic_boundaries, get_start_and_end_dates
from hda_utils.query_builder import build_query_from_metadata
from hda_utils.helpers import get_volume_in_Gb, search_with_timeout
from hda_utils.get_versions import get_versions
from hda_utils.general import get_duration_in_seconds_from_two_utc, get_number_of_datasets_downloaded, default_serializer
import uuid
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)

def main():
    
    start_time = datetime.utcnow()
    c = get_client()
    datasets_availability = []

    for dataset in c.datasets()[:10]:
        dataset_id = dataset['dataset_id']
        query = {}
        try:
            metadata_dataset = c.metadata(dataset_id=dataset_id)
            query = build_query_from_metadata(metadata_dataset)
            query = apply_exceptions(dataset_id, query)

            min_lon, max_lon, min_lat, max_lat = get_geographic_boundaries(metadata_dataset)
            start_date, end_date = get_start_and_end_dates(metadata_dataset)

            matches = search_with_timeout(query, 120)
            volume = get_volume_in_Gb(matches)

            datasets_availability.append([
                str(uuid.uuid4()), dataset_id, True, None, 
                min_lon, max_lon, min_lat, max_lat,
                start_date, end_date, volume, query
            ])
            print(f"{dataset_id}: {volume} GB")

        except Exception as e:
            logging.exception(f"Error processing dataset {dataset_id}")
            datasets_availability.append([
                str(uuid.uuid4()), dataset_id,
                False, str(e), -999, -999, -999,
                -999, "3000-06-06T00:00:00Z", "3000-06-06T00:00:00Z",
                0, query
            ])

    end_time = datetime.utcnow()
    
    df = pd.DataFrame(
        datasets_availability,
        columns=['id','Dataset_id', 'Available', 'Error', 'Min Lon', 'Max Lon',
                 'Min Lat', 'Max Lat', 'Start', 'End', 'Volume (GB)', 'Query']
    )
    df.to_csv('data/Datasets_availability.csv', index=False)
    print("✅ Saved results to data/Datasets_availability.csv")

    df_with_error = df.copy()
    df_with_error = df_with_error[df_with_error["Available"]==False]
    df_with_error.to_csv('data/Datasets_with_errors.csv', index=False)

    versions = get_versions()
    run_duration = get_duration_in_seconds_from_two_utc(start_time, end_time)
    number_of_datasets = get_number_of_datasets_downloaded('data')
    
    test_info = {
        "start_time": start_time,
        "end_time": end_time,
        "run_duration_seconds": run_duration,
        "number_of_datasets": number_of_datasets,
        "versions": {
            "linux_version": versions['linux_version'],
            "hda_version": versions['hda_version'],
            "script_version": versions['script_version']
        }
    }


    # Save to JSON
    with open("data/test_info.json", "w") as f:
        json.dump(test_info, f, indent=4, default=default_serializer)

if __name__ == "__main__":
    main()
