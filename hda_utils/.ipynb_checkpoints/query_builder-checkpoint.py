# hda_utils/query_builder.py
def build_query_from_metadata(metadata, startdate=None, enddate=None, items_per_page=200, start_index=0):
    query = {}
    properties = metadata.get("properties", {})
    for key, prop in properties.items():
        if key in ["startdate", "enddate", "bbox"]:
            continue
        try:
            if "oneOf" in prop:
                query[key] = prop["oneOf"][0]["const"]
            elif "items" in prop and "oneOf" in prop["items"]:
                query[key] = prop["items"]["oneOf"][0]["const"]
            elif "default" in prop and prop["default"]:
                query[key] = prop["default"]
        except Exception:
            pass
    if startdate:
        query["startdate"] = startdate
    if enddate:
        query["enddate"] = enddate
    query["itemsPerPage"] = items_per_page
    query["startIndex"] = start_index
    return query
