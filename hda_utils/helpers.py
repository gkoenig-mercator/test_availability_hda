# hda_utils/helpers.py
import multiprocessing

def run_with_timeout(method, timeout, *args, **kwargs):
    def target(queue):
        try:
            result = method(*args, **kwargs)
            queue.put(("ok", result))
        except Exception as e:
            queue.put(("error", e))

    queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=target, args=(queue,))
    process.start()
    process.join(timeout)

    if process.is_alive():
        process.terminate()
        process.join()
        raise TimeoutError(f"Dataset check exceeded {timeout} seconds")

    if not queue.empty():
        status, value = queue.get()
        if status == "error":
            raise value
        return value


def get_volume_in_Gb(matches):
    try:
        return int(matches.volume / 1e9)
    except Exception:
        return None
