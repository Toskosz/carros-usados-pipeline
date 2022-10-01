import pandas as pd
from bs4 import BeautifulSoup
import math
import logging


def __get_req_headers():
    return {'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'api2.autoline.com.br',
        'Origin': 'https://www.autoline.com.br',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0'}

def __extract_data(data):
    return {k : data[k] for k in ['AdId','AdditionalInformation','BodyTypeName','BuiltYear','CityName','ColorName','DoorNumberName','Email','EngineTypeName','Features'
    ,'FuelTypeName','IsArmored','IsCollectorVehicle','IsDisabilityAdapted','IsEligibleforFinancing','IsFinanced','IsManufacturerWarrantyActive','IsOneOwnerUsed','IsPaid'
    ,'IsRegistrationPaid','IsSellerPj','IsSwapNotAccepted','IsTaxPaid','Km','LinkAnuncio','MakeName','MobilePhoneNumber','ModelName','ModelYear','Neighborhood','PhoneNumber'
    ,'Price','PriceFipe','RegistrationPlate','SecondaryColorName','SegmentName','SellerAddress1','SellerAddress2','SellerDocumentNumber','SellerName','StateAbbreviation'
    ,'StateName','TransmissionName','TypeSellerName','VersionName','WhatsAppNumber']}

def __get_cars_ids(n_cars, client) -> list:
    ids = []
    pages = math.ceil(n_cars/24)

    for i in range(1, pages + 1):
        soup = BeautifulSoup(client.get(url="https://busca.autoline.com.br/comprar/carros/novos-seminovos-usados/todos-os-estados/todas-as-cidades/todas-as-marcas/todos-os-modelos/todas-as-versoes/todos-os-anos/todas-as-cores/todos-os-precos/?sort=20&page=" + str(i)).text, features="html.parser")

        infos = soup.find_all("li", attrs={'class':'nm-product-item'})
        for info in infos:
            try:
                ids.append(info['data-pid'])
            except:
                continue
    return ids

def __get_recent_cars(max_batch_size, client) -> pd.DataFrame:

    client.headers.update(__get_req_headers())

    cars = pd.DataFrame(columns=['AdId','AdditionalInformation','BodyTypeName','BuiltYear','CityName','ColorName','DoorNumberName','Email','EngineTypeName','Features'
    ,'FuelTypeName','IsArmored','IsCollectorVehicle','IsDisabilityAdapted','IsEligibleforFinancing','IsFinanced','IsManufacturerWarrantyActive','IsOneOwnerUsed','IsPaid'
    ,'IsRegistrationPaid','IsSellerPj','IsSwapNotAccepted','IsTaxPaid','Km','LinkAnuncio','MakeName','MobilePhoneNumber','ModelName','ModelYear','Neighborhood','PhoneNumber'
    ,'Price','PriceFipe','RegistrationPlate','SecondaryColorName','SegmentName','SellerAddress1','SellerAddress2','SellerDocumentNumber','SellerName','StateAbbreviation'
    ,'StateName','TransmissionName','TypeSellerName','VersionName','WhatsAppNumber'])

    for id in __get_cars_ids(max_batch_size, client):
        cars = cars.append(__extract_data(client.get(url=''.join(["https://api2.autoline.com.br/api/pub/", str(id)])).json()), ignore_index=True)

    cars.columns = ["AD_ID","INFORMACOES_ADICIONAIS","CORPO_VEICULO","ANO_FABRICACAO","CIDADE","COR","QNTD_PORTAS","EMAIL","MOTOR","RECURSOS","COMBUSTIVEL","BLINDADO"
    ,"COLECIONADOR","ADAPTADO_DEFICIENCIA","FINANCIAVEL","FINANCIADO","GARANTIA_DE_FABRICA","DONO_UNICO","QUITADO","REGISTRAMENTO_PAGO","VENDEDOR_PJ","ACEITA_TROCA","IMPOSTOS_PAGOS"
    ,"KILOMETRAGEM","LINK_AD","FABRICANTE","CELULAR","MODELO","ANO_MODELO","BAIRRO","TELEFONE","PRECO","PRECO_FIPE","PLACA","COR_SECUNDARIA","TIPO_VEICULO","ENDERECO"
    ,"COMPLEMENTO_ENDERECO","DOCUMENTO_VENDEDOR","NOME_VENDEDOR","UF","ESTADO","TRANSMISSAO","TIPO_VENDEDOR","VERSAO","WHATSAPP"]

    return cars.head(max_batch_size)

def run(max_batch_size, client):
    logging.info("[LOG] Extracting...")
    data = __get_recent_cars(max_batch_size, client)
    # TODO: SEND RAW EXTRACT TO S3
    logging.info("[LOG] Done extracting.")
    return data
