from autoline.autoline_extract import AutolineExtract
from autoline.autoline_transform import AutolineTransform
from webmotors.webmotors_extract import WebmotorsExtract
from webmotors.webmotors_transform import WebmotorsTransform
import warnings

def run() -> None:
    autoline_extract = AutolineExtract()
    print("Extração inicializada.")
    data = autoline_extract.run(20)
    print("Extração finalizada.")

    autoline_transform = AutolineTransform()
    autoline_transform.run(data)
    
    #webmotors_extract = WebmotorsExtract()
    #data = webmotors_extract.run(25)
    
    #webmotors_transform = WebmotorsTransform()
    #webmotors_transform.run(data)

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    run()