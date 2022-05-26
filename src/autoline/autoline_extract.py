from time import sleep
import requests
import pandas as pd
from bs4 import BeautifulSoup
import math

class AutolineExtract:

    def __init__(self) -> None:
        self.req_headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'api2.autoline.com.br',
            'Origin': 'https://www.autoline.com.br',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0'
        }

    # Extract only the data we want
    def __extract_data(self, data):
        tmp_row = {}

        tmp_row['AD_ID'] = data['AdId']
        tmp_row['INFORMACOES_ADICIONAIS'] = data['AdditionalInformation']
        tmp_row['CORPO_VEICULO'] = data['BodyTypeName']
        tmp_row['ANO_FABRICACAO'] = data['BuiltYear']
        tmp_row['CIDADE'] = data['CityName']
        tmp_row['COR'] = data['ColorName']
        tmp_row['QNTD_PORTAS'] = data['DoorNumberName']
        tmp_row['EMAIL'] = data['Email']
        tmp_row['MOTOR'] = data['EngineTypeName']
        tmp_row['RECURSOS'] = data['Features']
        tmp_row['COMBUSTIVEL'] = data['FuelTypeName']
        tmp_row['BLINDADO'] = data['IsArmored']
        tmp_row['COLECIONADOR'] = data['IsCollectorVehicle']
        tmp_row['ADAPTADO_DEFICIENCIA'] = data['IsDisabilityAdapted']
        tmp_row['FINANCIAVEL'] = data['IsEligibleforFinancing']
        tmp_row['FINANCIADO'] = data['IsFinanced']
        tmp_row['GARANTIA_DE_FABRICA'] = data['IsManufacturerWarrantyActive']
        tmp_row['DONO_UNICO'] = data['IsOneOwnerUsed']
        tmp_row['QUITADO'] = data['IsPaid']
        tmp_row['REGISTRAMENTO_PAGO'] = data['IsRegistrationPaid']
        tmp_row['VENDEDOR_PJ'] = data['IsSellerPj']
        tmp_row['ACEITA_TROCA'] = data['IsSwapNotAccepted']
        tmp_row['IMPOSTOS_PAGOS'] = data['IsTaxPaid']
        tmp_row['KILOMETRAGEM'] = data['Km']
        tmp_row['LINK_AD'] = data['LinkAnuncio']
        tmp_row['FABRICANTE'] = data['MakeName']
        tmp_row['CELULAR'] = data['MobilePhoneNumber']
        tmp_row['MODELO'] = data['ModelName']
        tmp_row['ANO_MODELO'] = data['ModelYear']
        tmp_row['BAIRRO'] = data['Neighborhood']
        tmp_row['TELEFONE'] = data['PhoneNumber']
        tmp_row['PRECO'] = data['Price']
        tmp_row['PRECO_FIPE'] = data['PriceFipe']
        tmp_row['PLACA'] = data['RegistrationPlate']
        tmp_row['COR_SECUNDARIA'] = data['SecondaryColorName']
        tmp_row['TIPO_VEICULO'] = data['SegmentName']
        tmp_row['ENDERECO'] = data['SellerAddress1']
        tmp_row['COMPLEMENTO_ENDERECO'] = data['SellerAddress2']
        tmp_row['DOCUMENTO_VENDEDOR'] = data['SellerDocumentNumber']
        tmp_row['NOME_VENDEDOR'] = data['SellerName']
        tmp_row['UF'] = data['StateAbbreviation']
        tmp_row['ESTADO'] = data['StateName']
        tmp_row['TRANSMISSAO'] = data['TransmissionName']
        tmp_row['TIPO_VENDEDOR'] = data['TypeSellerName']
        tmp_row['VERSAO'] = data['VersionName']
        tmp_row['WHATSAPP'] = data['WhatsAppNumber']

        return tmp_row

    def __get_cars_ids(self, n_cars) -> list:

        ids = []
        pages = math.ceil(n_cars/24)

        for i in range(1, pages + 1):
            url = "https://busca.autoline.com.br/comprar/carros/novos-seminovos-usados/todos-os-estados/todas-as-cidades/todas-as-marcas/todos-os-modelos/todas-as-versoes/todos-os-anos/todas-as-cores/todos-os-precos/?sort=20&page=" + str(i)
            
            response = requests.get(url=url)
            html = response.text
            soup = BeautifulSoup(html, features="html.parser")

            infos = soup.find_all("li", attrs={'class':'nm-product-item'})
            for info in infos:
                try:
                    ids.append(info['data-pid'])
                except:
                    continue
        return ids

    def __get_recent_cars(self, max_batch_size) -> pd.DataFrame:

        cars = pd.DataFrame(columns=['AD_ID','INFORMACOES_ADICIONAIS','CORPO_VEICULO','ANO_FABRICACAO','CIDADE','COR',
            'QNTD_PORTAS','EMAIL','MOTOR','RECURSOS','COMBUSTIVEL','BLINDADO','COLECIONADOR','ADAPTADO_DEFICIENCIA','FINANCIAVEL','FINANCIADO','GARANTIA_DE_FABRICA','DONO_UNICO',
            'QUITADO','REGISTRAMENTO_PAGO','VENDEDOR_PJ','ACEITA_TROCA','IMPOSTOS_PAGOS','KILOMETRAGEM','LINK_AD','FABRICANTE','CELULAR','MODELO','ANO_MODELO','BAIRRO',
            'TELEFONE','PRECO','PRECO_FIPE','PLACA','COR_SECUNDARIA','TIPO_VEICULO','ENDERECO','COMPLEMENTO_ENDERECO','DOCUMENTO_VENDEDOR',
            'NOME_VENDEDOR','UF','ESTADO','TRANSMISSAO','TIPO_VENDEDOR','VERSAO','WHATSAPP'])

        cars_ids = self.__get_cars_ids(max_batch_size)
        
        for id in cars_ids:
            data_api = "https://api2.autoline.com.br/api/pub/" + str(id)

            # API limit
            sleep(5)
            response = requests.get(url=data_api, headers=self.req_headers)
            data = response.json()

            cars = cars.append(self.__extract_data(data), ignore_index=True)

        return cars.head(max_batch_size)

    def run(self, max_batch_size):
        print("[LOG] Extracting...")
        data = self.__get_recent_cars(max_batch_size)
        # TODO: SEND RAW EXTRACT TO S3
        print("[LOG] Done extracting.")
        return data