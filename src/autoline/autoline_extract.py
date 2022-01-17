import requests
import pandas as pd
from datetime import datetime
import time
from time import sleep
from bs4 import BeautifulSoup
import math

class AutolineExtract:

    def __init__(self) -> None:
        self.req_headers = {
            'Host': 'api2.autoline.com.br',
            'Connection': 'keep-alive',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'sec-ch-ua-platform': '"Windows"',
            'Origin': 'https://www.autoline.com.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
        }

    def run(self, max_batch_size):
        data = self.__get_recent_cars(max_batch_size)
        now = datetime.now()
        str_hora = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
        data.to_csv('raw/autoline/'+str_hora+'.csv',index=False)
        return data

    def __extract_data(self, data):
        tmp_row = {}

        tmp_row['AD_ID'] = data['AdId']
        tmp_row['INFORMACOES_ADICIONAIS'] = data['AdditionalInformation']
        tmp_row['CORPO_VEICULO'] = data['BodyTypeName']
        tmp_row['ANO_FABRICACAO'] = data['BuiltYear']
        tmp_row['CIDADE'] = data['CityName']
        tmp_row['COR'] = data['ColorName']
        tmp_row['DATA_ATUALIZACAO_AUTOLINE'] = data['DataAtualizacao']
        tmp_row['DATA_CRIACAO_AD'] = data['DataCriacao']
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
        tmp_row['NOVO'] = data['IsNew']
        tmp_row['DONO_UNICO'] = data['IsOneOwnerUsed']
        tmp_row['QUITADO'] = data['IsPaid']
        tmp_row['REGISTRAMENTO_PAGO'] = data['IsRegistrationPaid']
        tmp_row['VENDEDOR_PJ'] = data['IsSellerPj']
        tmp_row['ACEITA_TROCA'] = data['IsSwapNotAccepted']
        tmp_row['IMPOSTOS_PAGOS'] = data['IsTaxPaid']
        tmp_row['ZEROKM'] = data['IsZeroKm']
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
        tmp_row['DATA_DE_REGISTRO'] = data['RegisterDate']
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
        tmp_row['DATA_ATT_AD'] = data['UpdatedDate']
        tmp_row['VERSAO'] = data['VersionName']
        tmp_row['WHATSAPP'] = data['WhatsAppNumber']

        return tmp_row

    def __get_carros_ids(self, qntd_carros) -> list:

        ids = []
        paginas = math.ceil(qntd_carros/24)

        for i in range(1, paginas + 1):
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

        carros = pd.DataFrame(columns=['AD_ID','INFORMACOES_ADICIONAIS','CORPO_VEICULO','ANO_FABRICACAO','CIDADE','COR','DATA_ATUALIZACAO_AUTOLINE','DATA_CRIACAO_AD',
            'QNTD_PORTAS','EMAIL','MOTOR','RECURSOS','COMBUSTIVEL','BLINDADO','COLECIONADOR','ADAPTADO_DEFICIENCIA','FINANCIAVEL','FINANCIADO','GARANTIA_DE_FABRICA','NOVO','DONO_UNICO',
            'QUITADO','REGISTRAMENTO_PAGO','VENDEDOR_PJ','ACEITA_TROCA','IMPOSTOS_PAGOS','ZEROKM','KILOMETRAGEM','LINK_AD','FABRICANTE','CELULAR','MODELO','ANO_MODELO','BAIRRO',
            'TELEFONE','PRECO','PRECO_FIPE','DATA_DE_REGISTRO','PLACA','COR_SECUNDARIA','TIPO_VEICULO','ENDERECO','COMPLEMENTO_ENDERECO','DOCUMENTO_VENDEDOR',
            'NOME_VENDEDOR','UF','ESTADO','TRANSMISSAO','TIPO_VENDEDOR','DATA_ATT_AD','VERSAO','WHATSAPP'])

        carros_ids = self.__get_carros_ids(max_batch_size)
        
        for id in carros_ids:
            data_api = "https://api2.autoline.com.br/api/pub/" + str(id)

            response = requests.get(url=data_api)
            data = response.json()

            carros = carros.append(self.__extract_data(data), ignore_index=True)

        return carros.head(max_batch_size)

'''
AdId: 15916733
AdditionalInformation: "CARRO MUITO NOVO NUNCA BATEU \n\nREVISADO COM GARANTIA DA LOJA \n\nDOC SEM NENHUM DÉBITO \n\nCOMPLETO G6  4 PORTAS \n\nAR CONDICIONADO \n\nDIREÇÃO HIDRÁULICA \n\nVIDROS E TRAVAS ELÉTRICAS \n\nACEITAMOS SEU USADO NA TROCA \n\nOPORTUNIDADE !!! MELHORES TAXAS DE FINANCIAMENTO\n\nPARCELAMOS EM ATÉ 18 X CARTÃO DE CRÉDITO TAXAS BAIXAS \n\nEM EXPOSIÇÃO AV ITAQUERA 1352 PRÓXIMO AV ARICANDUVA \n\nDE SEGUNDA A SÁBADO DAS 09:00HS ÁS 18:00HS \n\nRZERO MULTIMARCAS. Outros opcionais: Air bag do motorista, Porta-copos, Farol de neblina, Pára-choques na cor do veículo."
BodyTypeName: "Hatch"
BrakeSystemName: ""
BuiltYear: 2013
CityName: "São Paulo"
ColorName: "Preto"
CoolingTypeName: ""
DataAtualizacao: "0001-01-01T00:00:00"
DataCriacao: null
DoorNumberName: "Quatro"
Email: null
EngineTypeName: ""
Features: 
    0: "Direção Hidráulica"
    1: "Freios ABS"
    2: "Ar Condicionado"
    3: "Travas Elétricas"
    4: "Vidros Elétricos"
    5: "Ar Quente"
    6: "Limpador Traseiro"
    7: "Computador de Bordo"
    8: "Sensor de Chuva"
    9: "Desembaçador Traseiro"
    10: "Encosto de Cabeça Traseiro"
FuelTypeName: "Gasolina e Álcool"
IsArmored: false
IsCollectorVehicle: false
IsDisabilityAdapted: false
IsEligibleforFinancing: true
IsFinanced: false
IsManufacturerWarrantyActive: false
IsNew: false
IsOneOwnerUsed: false
IsPaid: false
IsRegistrationPaid: true
IsSellerPj: null
IsSwapNotAccepted: false
IsTaxPaid: true
IsZeroKm: "Usado"
Km: 58000
LinkAnuncio: null
MakeName: "VOLKSWAGEN"
MobilePhoneNumber: "11 43063613"
ModelName: "GOL"
ModelYear: 2013
Neighborhood: "Jardim Maringá"
OldPrice: 31890
PhoneNumber: "11 43063613"
Price: 31890
PriceFeirao: 31890
PriceFipe: null
Qtde: null
RegisterDate: "2021-10-21T20:39:32"
RegistrationPlate: "FJG0476"
SecondaryColorName: null
SegmentName: "Carro"
SellerAddress1: "Avenida Itaquera"
SellerAddress2: "1339"
SellerAdsCount: null
SellerDocumentNumber: 21797639000110
SellerLocation: "São Paulo/SP"
SellerName: "RZERO MULTIMARCAS"
SellerRealName: null
SellerType: null
StateAbbreviation: "SP"
StateName: "São Paulo"
TradingName: null
TransmissionName: "Manual"
TypeSellerName: "Loja"
UpdatedDate: "2021-10-21T20:45:30"
VersionName: "1.0 8V FLEX 4P MANUAL"
WhatsAppNumber: "971336337"
url: null
'''