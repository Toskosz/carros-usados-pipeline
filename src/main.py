from autoline.autoline_extract import AutolineExtract
from autoline.autoline_transform import AutolineTransform
from webmotors.webmotors_extract import WebmotorsExtract
from webmotors.webmotors_transform import WebmotorsTransform
from util.error_email import notify_erro

def run() -> None:
    try:
        autoline_extract = AutolineExtract()
        data = autoline_extract.run(100)
        autoline_transform = AutolineTransform()
        autoline_transform.run(data)

        webmotors_extract = WebmotorsExtract()
        data = webmotors_extract.run(100)
        webmotors_transform = WebmotorsTransform()
        webmotors_transform.run(data)
    except Exception as E:
        notify_erro(E)

if __name__ == '__main__':
    run()