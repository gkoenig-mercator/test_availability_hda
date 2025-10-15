# hda_utils/helpers.py
import threading

def run_with_timeout(method, timeout, *args, **kwargs):
    result = {}
    def wrapper():
        result["value"] = method(*args, **kwargs)
    thread = threading.Thread(target=wrapper)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError(f"Dataset check exceeded {timeout} seconds")
    return result.get("value")

def get_volume_in_Gb(matches):
    try:
        return int(matches.volume / 1e9)
    except Exception:
        return None
