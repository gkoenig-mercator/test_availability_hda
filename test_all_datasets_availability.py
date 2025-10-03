from hda import Client, Configuration
import pandas as pd
import logging

EXCEPTIONS = {
    "EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V1_HOURLY_NETCDF": {
        "notes": "Requires a complete query; fails with minimal query. Needs bbox, dates, and all fixed parameters.",
        "force_fields": {
            "productType": "LST",
            "productionStatus": "ARCHIVED",
            "acquisitionType": "NOMINAL",
            "platform": "GOES",
            "processingCenter": "IPMA",
            "resolution": "5000"
        }
    },
    "EO:MO:DAT:NWSHELF": {
        "notes": "Fails when bbox is included. Queries should omit bbox.",
        "remove_fields": ["bbox"]
    },
    "EO:MO:DAT:NWSHELF_MULTIYEAR_BGC_004_011": {
        "notes": "Same as NWSHELF: omit bbox, otherwise query hangs.",
        "remove_fields": ["bbox"]
    },
    "EO:EUM:DAT:MULT:HIRSL1": {
        "notes": "Fails unless orbit is provided. publication can be omitted, but orbit must be present.",
        "required_fields": ["orbit"]
    },
    "EO:EUM:DAT:0684": {
        "notes": "Needs repeatCycleIdentifier (not marked required in metadata).",
        "required_fields": ["repeatCycleIdentifier"]
    },
    "EO:ESA:DAT:SENTINEL-3": {
        "notes": "Fails if startdate/enddate are empty. Must supply non-empty values.",
        "require_non_empty": ["startdate", "enddate"]
    },
    "EO:CLMS:DAT:CLMS_GLOBAL_DMP_300M_V1_10DAILY_NETCDF": {
        "notes": "ProductionStatus must be ARCHIVED, not CANCELLED.",
        "force_fields": {"productionStatus": "ARCHIVED"}
    }
}


#logging.getLogger("hda").setLevel("DEBUG")

config = Configuration(path='../.hdarc')
c = Client(config=config, retry_max=3, sleep_max=1)

datasets_availability = []


def get_geographic_boundaries(metadata):
    """Extract dataset bounding box coordinates if available."""
    try:
        coords = metadata['metadata']['_source']['location']['coordinates']
        min_lon, max_lon = coords[0][0], coords[1][0]
        min_lat, max_lat = coords[1][1], coords[0][1]
        return min_lon, max_lon, min_lat, max_lat
    except Exception:
        return None, None, None, None


def get_start_and_end_dates(metadata):
    """Extract dataset start/end dates if available."""
    try:
        props = metadata['properties']
        return props['startdate']['default'], props['enddate']['default']
    except Exception:
        return None, None


def get_volume_in_Gb(matches):
    """Convert volume from bytes to gigabytes."""
    try:
        return int(matches.volume / 1e9)
    except Exception:
        return None


def retrieve_required_informations(metadata):
    """Build query dictionary based on required metadata fields."""
    dic_info = {}
    required = list(metadata.get('required', []))  # copy to avoid mutating original

    if 'variables' in required:
        required.remove('variables')

    for info in required:
        try:
            if info in ('latitude', 'longitude'):
                dic_info[info] = 45
            elif info == 'altitude':
                dic_info[info] = 0
            elif info == 'startdate':
                dic_info[info] = metadata['properties'][info]['minimum']
            elif info == 'enddate':
                dic_info[info] = metadata['properties'][info]['maximum']
            else:
                # Try different schema structures
                dic_info[info] = (
                    metadata['properties'][info]['items']['oneOf'][0]['const']
                    if 'items' in metadata['properties'][info]
                    else metadata['properties'][info]['oneOf'][0]['const']
                )
        except Exception:
            dic_info[info] = None
    return dic_info


def create_query(dic_info):
    """Adapt query format for API consumption."""
    query = dict(dic_info)  # shallow copy
    if 'latitude' in query and 'longitude' in query:
        query['bbox'] = [40, 41, 40, 41]
        query.pop('latitude')
        query.pop('longitude')
    return query

def build_query_from_metadata(metadata, startdate=None, enddate=None, items_per_page=200, start_index=0):
    """
    Build a query dictionary using metadata information.
    - Fills in deterministic fields (dataset_id, productType, platform, etc.).
    - Leaves bbox, startdate, enddate flexible (uses arguments if provided).
    - Adds pagination fields.
    """

    query = {}

    properties = metadata.get("properties", {})

    for key, prop in properties.items():
        # Skip date and bbox (handled separately)
        if key in ["startdate", "enddate", "bbox"]:
            continue

        try:
            # If property has fixed possible values, take the first one
            if "oneOf" in prop:
                query[key] = prop["oneOf"][0]["const"]
            elif "items" in prop and "oneOf" in prop["items"]:
                query[key] = prop["items"]["oneOf"][0]["const"]
            elif "default" in prop and prop["default"] != "":
                query[key] = prop["default"]
        except Exception:
            # fallback: ignore if we can't resolve it
            pass
    
    # Handle temporal range
    if startdate:
        query["startdate"] = startdate
    if enddate:
        query["enddate"] = enddate

    # Pagination
    query["itemsPerPage"] = items_per_page
    query["startIndex"] = start_index

    return query

def apply_exceptions(dataset_id, query):
    for key, rules in EXCEPTIONS.items():
        if dataset_id.startswith(key):  # prefix match for dataset families
            print(f"⚠️ Applying exception rules for {dataset_id}: {rules['notes']}")

            if "force_fields" in rules:
                query.update(rules["force_fields"])

            if "remove_fields" in rules:
                for field in rules["remove_fields"]:
                    query.pop(field, None)

            if "required_fields" in rules:
                for field in rules["required_fields"]:
                    if field not in query or not query[field]:
                        query[field] = "MISSING"  # or some default you define

            if "require_non_empty" in rules:
                for field in rules["require_non_empty"]:
                    if not query.get(field):
                        raise ValueError(f"{dataset_id} requires non-empty {field}")

    return query

for dataset in c.datasets()[694:712]:
    dataset_id = dataset['dataset_id']
    try:
        # Only one metadata request per dataset
        metadata_dataset = c.metadata(dataset_id=dataset_id)

        query = build_query_from_metadata(metadata_dataset)
        print(query)
        print(dataset_id)

        query = apply_exceptions("dataset_id", query)
        print(query)

        min_lon, max_lon, min_lat, max_lat = get_geographic_boundaries(metadata_dataset)
        start_date, end_date = get_start_and_end_dates(metadata_dataset)

        matches = c.search(query)
        volume = get_volume_in_Gb(matches)

        datasets_availability.append(
            [dataset_id, True, None, min_lon, max_lon, min_lat, max_lat,
             start_date, end_date, volume, query]
        )
        print(dataset_id, f"Download volume of {volume} Gb")

    except Exception as e:
        logging.exception(f"Error processing dataset {dataset_id}")
        datasets_availability.append(
            [dataset_id, False, str(e), None, None, None, None, None, None, None, query]
        )

# Save results
data_download = pd.DataFrame(
    datasets_availability,
    columns=[
        'Dataset_id', 'Available', 'Error',
        'Minimum Longitude', 'Maximum Longitude',
        'Minimum Latitude', 'Maximum Latitude',
        'Start date', 'End date',
        'Downloadable volume (Gb)','Query'
    ]
)

data_download.to_csv('Datasets_availability.csv', index=False)