from hda import Client, Configuration
import pandas as pd
import logging
import re
import threading
import time

# --- Exceptions expressed as regex patterns (strings) ---
# Use regex so exact matches and families/prefixes are handled the same way.
EXCEPTIONS_RAW = {
    # exact name (anchor with ^ and $)
    r"^EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V1_HOURLY_NETCDF$": {
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
    r"^EO:MO:DAT:NWSHELF$": {
        "notes": "Fails when bbox is included. Queries should omit bbox.",
        "remove_fields": ["bbox"]
    },
    r"^EO:MO:DAT:NWSHELF_MULTIYEAR_BGC_004_011$": {
        "notes": "Same as NWSHELF: omit bbox, otherwise query hangs.",
        "remove_fields": ["bbox"]
    },
    # family/prefix: matches EO:EUM:DAT:06** (any string starting with EO:EUM:DAT:06)
    r"^EO:EUM:DAT:06.*": {
        "notes": "All datasets in this family need repeatCycleIdentifier (not marked required in metadata).",
        "required_fields": ["repeatCycleIdentifier"]
    },
    r"^EO:ESA:DAT:SENTINEL-3$": {
        "notes": "Fails if startdate/enddate are empty. Must supply non-empty values.",
        "require_non_empty": ["startdate", "enddate"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_DMP_300M_V1_10DAILY_NETCDF$": {
        "notes": "ProductionStatus must be ARCHIVED, not CANCELLED.",
        "force_fields": {"productionStatus": "ARCHIVED"}
    },
    r"^EO:ECMWF:DAT:CAMS_GLOBAL_ATMOSPHERIC_COMPOSITION_FORECASTS$": {
        "notes":"The pressure levels and model levels are in the required fields but actually block the query. There is als a need for startdate and enddate",
        "remove_fields":["pressure_level","model_level"],
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",},
    },
    r"^EO:ECMWF:DAT:CAMS_GLOBAL_FIRE_EMISSIONS_GFAS$": {
        "notes": "The startdate and enddate fields are required",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",},
    },
    r"^EO:ECMWF:DAT:CAMS_GLOBAL_GREENHOUSE_GAS_FORECASTS$": {
        "notes": "The startdate and enddate fields are required",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",}
    },
    r"^EO:ECMWF:DAT:CAMS_GLOBAL_REANALYSIS_EAC4$": {
        "notes": "The startdate and enddate fields are required",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",}
    },
    r"^EO:ECMWF:DAT:CAMS_SOLAR_RADIATION_TIMESERIES$": {
        "notes":"The startdate, enddate, longitude, latitude and altitude fields are required",
        "force_fields": {
        "startdate": "2025-10-06T00:00:00.000Z",
        "enddate": "2025-10-06T23:59:59.999Z",
        "latitude": "40",
        "longitude": "40",
        "altitude": "1000",
        },
    },
    r"^EO:ECMWF:DAT:REANALYSIS_ERA5_LAND_TIMESERIES$": {
        "notes":"The startdate, enddate, longitude and latitude fields are required",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",
              "latitude": "40",
              "longitude": "50",
              },
        },
    r"^EO:ECMWF:DAT:REANALYSIS_ERA5_SINGLE_LEVELS_TIMESERIES$": {
        "notes": "The startdate, enddate, longitude and latitude fields are required",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",
              "latitude": "40",
              "longitude": "50",
              },
    },
    r"^EO:ECMWF:DAT:CAMS_GLOBAL_GHG_REANALYSIS_EGG4$": {
        "notes":"The dates were missing",
        "force_fields": {
              "startdate": "2025-10-03T00:00:00.000Z",
              "enddate": "2025-10-03T23:59:59.999Z",
        },
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_BA_300M_V3_DAILY_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_GDMP_300M_V1_10DAILY_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V1_10DAILY-DAILY-CYCLE_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V1_10DAILY-TCI_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V2_10DAILY-DAILY-CYCLE_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CLMS:DAT:CLMS_GLOBAL_LST_5KM_V2_10DAILY-TCI_NETCDF$": {
        "notes": "The Production status is not necessary.",
        "remove_fields": ["productionStatus"]
    },
    r"^EO:CNES:DAT:SWH:SPOT5$": {
        "notes": "The bbox is necessary.",
        "force_fields": {
                "bbox": [
                         15.7421745918424,
                         30.67706860391895,
                         33.090693529791174,
                         43.67197322929656
                         ],
          },
    },
    r"^EO:CRYO:DAT:HRSI:SWS$": {
        "notes": "The Production status is not necessary.",
        "force_fields": {
                "bbox": [
                         15.7421745918424,
                         30.67706860391895,
                         33.090693529791174,
                         43.67197322929656
                         ],
          },
    },
    r"^EO:ECMWF:DAT:CAMS_EUROPE_AIR_QUALITY_FORECASTS$": {
        "notes": "Requires a complete query; fails with minimal query. Needs bbox, dates, and all fixed parameters.",
        "force_fields": {
            "startdate": "2025-10-09T00:00:00.000Z",
            "enddate": "2025-10-09T23:59:59.999Z",
            "model": "ensemble",
            "level": "0",
            "type":"forecast",
            "time": "00:00",
            "leadtime_hour": "0",
        }
    },
}

# Compile patterns once into a list of tuples: (compiled_regex, rules)
def compile_exception_patterns(exceptions_raw):
    compiled = []
    for pat, rules in exceptions_raw.items():
        compiled.append((re.compile(pat), rules))
    return compiled

COMPILED_EXCEPTIONS = compile_exception_patterns(EXCEPTIONS_RAW)


def register_exception(pattern: str, rules: dict, at_front: bool = True):
    """
    Register a new exception at runtime.
    - pattern: a regex string (e.g. r"^EO:EUM:DAT:06.*" or r"^EO:MY:DATA:EXACT$").
    - rules: dict with possible keys: notes, force_fields, remove_fields, required_fields, require_non_empty
    - at_front: if True, new rule is checked before existing rules (gives it higher precedence).
    """
    compiled = (re.compile(pattern), rules)
    if at_front:
        COMPILED_EXCEPTIONS.insert(0, compiled)
    else:
        COMPILED_EXCEPTIONS.append(compiled)


def _apply_rules(query: dict, rules: dict, dataset_id: str):
    """Helper to apply the rules to the query dict. Returns modified query."""
    # Force/override fields
    if "force_fields" in rules:
        query.update(rules["force_fields"])

    # Remove fields
    if "remove_fields" in rules:
        for field in rules["remove_fields"]:
            query.pop(field, None)

    # Required fields: ensure presence (set placeholder if missing)
    if "required_fields" in rules:
        for field in rules["required_fields"]:
            if field not in query or query.get(field) in (None, "", []):
                if field=="repeatCycleIdentifier":
                    query[field]="2"
                else:
                    # You might prefer None, a sentinel, or a default. Change here if needed.
                    query[field] = "MISSING"

    # Require non-empty: raise if missing or empty (keeps original behavior)
    if "require_non_empty" in rules:
        for field in rules["require_non_empty"]:
            if not query.get(field):
                raise ValueError(f"{dataset_id} requires non-empty {field}")

    return query


def apply_exceptions(dataset_id: str, query: dict):
    """
    Apply all matching exception rules to `query`.
    Patterns are regexes and tested with .search() against dataset_id.
    Multiple patterns can match; rules are applied in the order in COMPILED_EXCEPTIONS.
    """
    for pattern, rules in COMPILED_EXCEPTIONS:
        if pattern.search(dataset_id):
            print(f"⚠️ Applying exception rules for {dataset_id}: {rules.get('notes', '')}")
            query = _apply_rules(query, rules, dataset_id)
    return query


# ---------------------------
# rest of your original script with one small fix: make sure to call apply_exceptions(dataset_id, query)
# ---------------------------

# logging.getLogger("hda").setLevel("DEBUG")

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


def run_with_timeout(method, timeout, *args, **kwargs):
    result = {}

    def wrapper():
        result["value"] = method(*args, **kwargs)

    thread = threading.Thread(target=wrapper)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        raise TimeoutError(f"Dataset check exceeded {timeout} seconds")

    # ✅ Return the captured result
    return result.get("value")

# ---- main loop (same as before, but corrected apply_exceptions call) ----
for dataset in c.datasets()[1200:1300]:
    dataset_id = dataset['dataset_id']
    query = {}  # ensure query exists even if exception is raised early
    try:
        # Only one metadata request per dataset
        metadata_dataset = c.metadata(dataset_id=dataset_id)

        query = build_query_from_metadata(metadata_dataset)
        print(query)
        print(dataset_id)

        # <-- FIXED: pass the actual dataset_id variable, not the literal string
        query = apply_exceptions(dataset_id, query)
        print(query)

        min_lon, max_lon, min_lat, max_lat = get_geographic_boundaries(metadata_dataset)
        start_date, end_date = get_start_and_end_dates(metadata_dataset)

        matches = run_with_timeout(c.search, 30, query)
#        matches = c.search(query)
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
