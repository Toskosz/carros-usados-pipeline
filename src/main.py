from autoline import autoline_extract
from autoline import autoline_transform
from webmotors import webmotors_extract
from webmotors import webmotors_transform
import logging

def run() -> None:
    try:
        logging.basicConfig(filename="./pipeline.log",
                            filemode='w',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S', 
                            level=logging.DEBUG)
                            
        autoline_transform.run(autoline_extract.run(500))
        webmotors_transform.run(webmotors_extract.run(500))
    except Exception as E:
        logging.error(E)

if __name__ == '__main__':
    run()
