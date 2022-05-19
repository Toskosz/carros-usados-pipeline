from autoline.autoline_extract import AutolineExtract
from autoline.autoline_transform import AutolineTransform
from webmotors.webmotors_extract import WebmotorsExtract
from webmotors.webmotors_transform import WebmotorsTransform
from util.error_email import notify_erro
import warnings

def run() -> None:
    try:
        autoline_extract = AutolineExtract()
        print("[LOG] Extração inicializada.")
        data = autoline_extract.run(20)
        print("[LOG] Extração finalizada.")

        autoline_transform = AutolineTransform()
        autoline_transform.run(data)

        webmotors_extract = WebmotorsExtract()
        print("[LOG] Extração inicializada.")
        data = webmotors_extract.run(20)
        print("[LOG] Extração finalizada.")
        
        webmotors_transform = WebmotorsTransform()
        webmotors_transform.run(data)
    except:
        notify_erro()

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    run()