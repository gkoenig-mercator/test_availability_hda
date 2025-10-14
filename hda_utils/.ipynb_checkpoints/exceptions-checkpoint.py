# hda_utils/exceptions.py
import re

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

def compile_exception_patterns(exceptions_raw):
    return [(re.compile(pat), rules) for pat, rules in exceptions_raw.items()]

COMPILED_EXCEPTIONS = compile_exception_patterns(EXCEPTIONS_RAW)

def register_exception(pattern: str, rules: dict, at_front: bool = True):
    compiled = (re.compile(pattern), rules)
    if at_front:
        COMPILED_EXCEPTIONS.insert(0, compiled)
    else:
        COMPILED_EXCEPTIONS.append(compiled)

def _apply_rules(query: dict, rules: dict, dataset_id: str):
    if "force_fields" in rules:
        query.update(rules["force_fields"])
    if "remove_fields" in rules:
        for field in rules["remove_fields"]:
            query.pop(field, None)
    if "required_fields" in rules:
        for field in rules["required_fields"]:
            if not query.get(field):
                query[field] = "MISSING"
    if "require_non_empty" in rules:
        for field in rules["require_non_empty"]:
            if not query.get(field):
                raise ValueError(f"{dataset_id} requires non-empty {field}")
    return query

def apply_exceptions(dataset_id: str, query: dict):
    for pattern, rules in COMPILED_EXCEPTIONS:
        if pattern.search(dataset_id):
            print(f"⚠️ Applying exception rules for {dataset_id}: {rules.get('notes', '')}")
            query = _apply_rules(query, rules, dataset_id)
    return query
