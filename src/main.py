from autoline.autoline_extract import AutolineExtract
from autoline.autoline_transform import AutolineTransform
from webmotors.webmotors_extract import WebmotorsExtract
from webmotors.webmotors_transform import WebmotorsTransform

def run() -> None:
    autoline_extract = AutolineExtract()
    data = autoline_extract.run(2000)

    autoline_transform = AutolineTransform()
    autoline_transform.run(data)

    webmotors_extract = WebmotorsExtract()
    data = webmotors_extract.run(2000)

    webmotors_transform = WebmotorsTransform()
    webmotors_transform.run(data)

if __name__ == '__main__':
    run()