from autoline.autoline_extract import AutolineExtract
from autoline.autoline_transform import AutolineTransform
from webmotors.webmotors_etl import WebmotorsExtract
from webmotors.webmotors_transform import WebmotorsTransform

def run() -> None:
    autoline_extract = AutolineExtract()
    autoline_extract.run(2000)

    autoline_transform = AutolineTransform()
    autoline_transform.run()

    webmotors_extract = WebmotorsExtract()
    webmotors_extract.run(2000)

    webmotors_transform = WebmotorsTransform()
    webmotors_transform.run()

if __name__ == '__main__':
    run()