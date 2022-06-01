from autoline import autoline_extract
from autoline import autoline_transform
from webmotors import webmotors_extract
from webmotors import webmotors_transform
from util.error_email import notify_erro

def run() -> None:
    try:
        autoline_transform.run(autoline_extract.run(100))
        webmotors_transform.run(webmotors_extract.run(100))
    except Exception as E:
        notify_erro(E)

if __name__ == '__main__':
    run()