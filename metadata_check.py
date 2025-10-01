from hda import Client, Configuration
import pandas as pd
import logging

logging.getLogger("hda").setLevel("DEBUG")

config = Configuration(path='../.hdarc')
c = Client(config=config, retry_max=500, sleep_max=2)

datasets_availability = []

for dataset in c.datasets()[::-1]:
    dataset_id = dataset['dataset_id']
    try:
        # Just try fetching metadata
        metadata_dataset = c.metadata(dataset_id=dataset_id)

        # If we succeed, mark dataset as accessible
        datasets_availability.append(
            [dataset_id, True, None, metadata_dataset]
        )
        print(f"✅ Metadata accessible for {dataset_id}")

    except Exception as e:
        logging.exception(f"❌ Error accessing metadata for dataset {dataset_id}")
        datasets_availability.append(
            [dataset_id, False, str(e), None]
        )

# Save results
data_download = pd.DataFrame(
    datasets_availability,
    columns=['Dataset_id', 'Metadata Accessible', 'Error', 'Metadata']
)

data_download.to_csv('Datasets_metadata_check.csv', index=False)
