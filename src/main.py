from autoline import autoline_extract
from autoline import autoline_transform
from webmotors import webmotors_extract
from webmotors import webmotors_transform
import requests
from requests.adapters import HTTPAdapter, Retry
import logging

def run() -> None:
    try:
        logging.basicConfig(filename="./pipeline.log",filemode='w',format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',datefmt='%H:%M:%S', level=logging.DEBUG)
        client = requests.Session()
        retries = Retry(total=5, connect=3, backoff_factor=2, status_forcelist=[502,503,504])
        client.mount('http://', HTTPAdapter(max_retries=retries))
        client.mount('https://', HTTPAdapter(max_retries=retries))

        autoline_transform.run(autoline_extract.run(50, client))
        webmotors_transform.run(webmotors_extract.run(50, client))
    except Exception as E:
        logging.error(E)

if __name__ == '__main__':
    run()
