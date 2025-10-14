# hda_utils/metadata.py
def get_geographic_boundaries(metadata):
    try:
        coords = metadata['metadata']['_source']['location']['coordinates']
        min_lon, max_lon = coords[0][0], coords[1][0]
        min_lat, max_lat = coords[1][1], coords[0][1]
        return min_lon, max_lon, min_lat, max_lat
    except Exception:
        return None, None, None, None

def get_start_and_end_dates(metadata):
    try:
        props = metadata['properties']
        return props['startdate']['default'], props['enddate']['default']
    except Exception:
        return None, None
