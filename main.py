# main.py
import pandas as pd
import logging
from hda_utils.config import get_client
from hda_utils.exceptions import apply_exceptions
from hda_utils.metadata import get_geographic_boundaries, get_start_and_end_dates
from hda_utils.query_builder import build_query_from_metadata
from hda_utils.helpers import get_volume_in_Gb, run_with_timeout

logging.basicConfig(level=logging.INFO)

def main():
    c = get_client()
    datasets_availability = []

    for dataset in c.datasets():
        dataset_id = dataset['dataset_id']
        query = {}
        try:
            metadata_dataset = c.metadata(dataset_id=dataset_id)
            query = build_query_from_metadata(metadata_dataset)
            query = apply_exceptions(dataset_id, query)

            min_lon, max_lon, min_lat, max_lat = get_geographic_boundaries(metadata_dataset)
            start_date, end_date = get_start_and_end_dates(metadata_dataset)

            matches = run_with_timeout(c.search, 30, query)
            volume = get_volume_in_Gb(matches)

            datasets_availability.append([
                dataset_id, True, None, min_lon, max_lon, min_lat, max_lat,
                start_date, end_date, volume, query
            ])
            print(f"{dataset_id}: {volume} GB")

        except Exception as e:
            logging.exception(f"Error processing dataset {dataset_id}")
            datasets_availability.append([
                dataset_id, False, str(e), None, None, None, None, None, None, None, query
            ])

    df = pd.DataFrame(
        datasets_availability,
        columns=['Dataset_id', 'Available', 'Error', 'Min Lon', 'Max Lon',
                 'Min Lat', 'Max Lat', 'Start', 'End', 'Volume (GB)', 'Query']
    )
    df.to_csv('data/Datasets_availability.csv', index=False)
    print("✅ Saved results to data/Datasets_availability.csv")

if __name__ == "__main__":
    main()
