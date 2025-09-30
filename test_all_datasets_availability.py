from hda import Client, Configuration
import pandas as pd
import logging
logging.getLogger("hda").setLevel("DEBUG")

config = Configuration(path='../.hdarc')
c = Client(config=config, retry_max=500, sleep_max=2)
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
    try: 
        return int(matches.volume/(10e9))
    except:
        return None

def retrieve_required_informations(metadata):
    dic_info = {}
    list_info = metadata['required']

    if 'variables' in list_info:
        list_info.remove('variables')

    for info in list_info:
        if info=='latitude' or info=='longitude':
            dic_info[info]=45
        elif info=='altitude':
            dic_info[info]=0
        elif info=='startdate':
            dic_info[info]=metadata['properties'][info]['minimum']
        elif info=='enddate':
            dic_info[info]=metadata['properties'][info]['maximum']
        else:
            try:
                dic_info[info] = metadata['properties'][info]['items']['oneOf'][0]['const']
            except:
                dic_info[info] = metadata['properties'][info]['oneOf'][0]['const']
    return dic_info

def create_query(dic_info):
    query = dic_info
    if 'latitude' in query.keys():
        query['bbox']=[40, 41, 40, 41]
        del query['latitude']
        del query['longitude']

    return query
        

for dataset_id in datasets_id[::-1]:

    try:
        metadata_dataset = c.metadata(dataset_id=dataset_id)
        dic_info = retrieve_required_informations(metadata_dataset)
        query = create_query(dic_info)
    except Exception as e:
        print(e)
        datasets_availability.append([dataset_id, False, e, None, None, None, None, None, None, None])
        continue
        

    print(query)
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