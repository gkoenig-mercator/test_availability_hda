def get_duration_in_seconds_from_two_utc(start_time, end_time):
    duration = end_time - start_time

    duration_seconds = duration.total_seconds()

    return int(duration_seconds)