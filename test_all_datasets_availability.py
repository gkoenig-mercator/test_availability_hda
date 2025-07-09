from hda import Client, Configuration
import pandas as pd
import logging
logging.getLogger("hda").setLevel("DEBUG")

config = Configuration(path='../.hdarc')
c = Client(config=config)
datasets_id = [dataset['dataset_id'] for dataset in c.datasets()]
datasets_availability = []
def get_geographic_boundaries(dataset_id, Client):
    try :
        dataset_geographical_bounds = Client.dataset(dataset_id)['metadata']['_source']['location']['coordinates']
        min_lon = dataset_geographical_bounds[0][0]
        min_lat = dataset_geographical_bounds[1][1]
        max_lon = dataset_geographical_bounds[1][0]
        max_lat = dataset_geographical_bounds[0][1]

        return min_lon, max_lon, min_lat, max_lat
    except :
        return None, None, None, None

def get_start_and_end_dates(dataset_id, Client):
    try: 
        metadata = Client.metadata(dataset_id)
        return metadata['properties']['startdate']['default'], metadata['properties']['enddate']['default']
    except:
        return None, None

def get_volume_in_Gb(matches):
    return int(matches.volume/(10e9))

for dataset_id in datasets_id:
    query = {
        "dataset_id": dataset_id
     }
    try:
        min_lon, max_lon, min_lat, max_lat = get_geographic_boundaries(dataset_id, c)
        start_date, end_date = get_start_and_end_dates(dataset_id, c)
        matches = c.search(query)
        volume = get_volume_in_Gb(matches)
        datasets_availability.append([dataset_id, True, None, min_lon, max_lon, min_lat, max_lat, start_date, end_date, volume])
        print(dataset_id, f"Download volume of {volume}Gb")
    except Exception as e:
        datasets_availability.append([dataset_id, False, e, None, None, None, None, None, None, None])
        print(e)

data_download = pd.DataFrame(datasets_availability, columns = ['Dataset_id','Available','Error','Minimum Longitude','Maximum Longitude','Minimum Latitude','Maximum Latitude', 'Start date','End date','Downloadable volume (Gb)'])

data_download.to_csv('Datasets_availability.csv')