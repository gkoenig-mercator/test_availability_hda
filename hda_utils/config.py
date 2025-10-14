# hda_utils/config.py
from hda import Client, Configuration

def get_client():
    config = Configuration(path='../.hdarc')
    return Client(config=config, retry_max=3, sleep_max=1)
