from autoline import autoline_extract
from autoline import autoline_transform
from webmotors import webmotors_extract
from webmotors import webmotors_transform
from util.error_email import notify_erro
import logging

def run() -> None:
    try:
        notify_erro("Comecando extracao.")
        logging.basicConfig(filename="./pipeline.log",
                            filemode='w',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S', 
                            level=logging.DEBUG)
                            
        autoline_transform.run(autoline_extract.run(20))
        webmotors_transform.run(webmotors_extract.run(20))
    except Exception as E:
        logging.error(E)
        notify_erro(E)

if __name__ == '__main__':
    run()
