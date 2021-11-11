import requests
import pandas as pd
from datetime import datetime
import time
from time import sleep
from warehouse import WarehouseEngine
from util.creds import get_warehouse_creds
import numpy as np

# Exemplo de resposta

'''
{
    "UniqueId": 39207015,
    "TITULO": "CHERY ARRIZO 6 1.5 VVT TURBO iFLEX GSX CVT",
    "FABRICANTE": "CHERY"
    "MODELO": "ARRIZO 6"
    "VERSAO": "1.5 VVT TURBO iFLEX GSX CVT"
    "ANO_FABRICACAO": "2021",
    "ANO_MODELO": 2022,
    "TRANSMISSAO": "Automática",
    "QNTD_PORTAS": "4",
    "CORPO_VEICULO": "Sedã",
    "OBSERVACOES": [
        {
            "Name": "Aceita troca"
        },
        {
            "Name": "Garantia de fábrica"
        }
    ],
    "BLINDADO": "N",
    "COR": "Cinza"
    "TIPO_VENDEDOR": "PJ",
    "CIDADE_VENDEDOR": "São Paulo",
    "ESTADO_VENDEDOR": "São Paulo (SP)",
    "TIPO_ANUNCIO": "Montadora"
    "ENTREGA_CARRO": true,
    "TROCA_COM_TROCO": true,
    "PRECO": 125990,
    "COMENTARIO": "Ótima Oportunidade!!! - CAOACHERY Atlântica, localizada na Avenida Atlântica, 179 - Interlagos - São Paulo/SP, Nossos vendedores aguardam por sua visita, aceitamos seu veículo usado na troca com excelente avaliação, oferecemos financiamento com excelentes taxas de juros, aceitamos cartão de credito mediante política da Caoa, seminovos periciados comercializados apenas com laudo aprovado sem restrição, Nos resguardamos no direito de possíveis erros de digitação  A CAOA agradece a sua preferência.",
    "FipePercent": 109
}
'''

class WebmotorsETL:

#   Extraction part

    def run() -> None:
        pass

    def __init__(self) -> None:
        self.req_headers = {
            'Accept':	'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection':	'keep-alive',
            'Host':	'www.webmotors.com.br',
            'Sec-Fetch-Dest':	'document',
            'Sec-Fetch-Mode':	'navigate',
            'Sec-Fetch-Site':	'none',
            'Sec-Fetch-User':	'?1',
            'Upgrade-Insecure-Requests':	'1',
            'User-Agent':	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0'
        }

    def __carrega_specs(tmp_row, specs) -> dict:

        tmp_row['TITULO'] = specs['Title']
        tmp_row['FABRICANTE'] = specs['Make']['Value']
        tmp_row['MODELO'] = specs['Model']['Value']
        tmp_row['VERSAO'] = specs['Version']['Value']
        tmp_row['ANO_FABRICACAO'] = specs['YearFabrication']
        tmp_row['ANO_MODELO'] = specs['YearModel']
        tmp_row['KILOMETRAGEM'] = specs['Odometer']
        tmp_row['TRANSMISSAO'] = specs['Transmission']
        tmp_row['QNTD_PORTAS'] = specs['NumberPorts']

        if 'BodyType' in specs.keys():
            tmp_row['CORPO_VEICULO'] = specs['BodyType']
        
        if 'VehicleAttributes' in specs.keys():
            atributos = specs['VehicleAttributes']
            obs = ''
            for atributo in atributos:
                for _, atributo_desc in atributo.items():
                    # dummy column
                    tmp_row[atributo_desc] = 1
                    # '.' for future split
                    obs += '.'+atributo_desc

            tmp_row['OBSERVACOES'] = obs
            
        tmp_row['BLINDADO'] = specs['Armored']
        tmp_row['COR'] = specs['Color']['Primary']

        return tmp_row

    def __carrega_vendedor(tmp_row, vendedor) -> dict:
        tmp_row['TIPO_VENDEDOR'] = vendedor['SellerType']

        if 'City' in vendedor.keys(): 
            tmp_row['CIDADE_VENDEDOR'] = vendedor['City']
        tmp_row['ESTADO_VENDEDOR'] = vendedor['State']
        tmp_row['AD_TYPE'] = vendedor['AdType']['Value']
        tmp_row['SCORE_VENDEDOR'] = vendedor['DealerScore']
        tmp_row['ENTREGA_CARRO'] = vendedor['CarDelivery']
        tmp_row['TROCA_COM_TROCO'] = vendedor['TrocaComTroco']

        return tmp_row

    def __carrega_precos(tmp_row, precos) -> dict:
        tmp_row['PRECO'] = precos['Price']
        tmp_row['PRECO_DESEJADO'] = precos['SearchPrice']
        return tmp_row

    def __carrega_carro_df_wh(self, carro, df) -> pd.DataFrame:
        tmp_row = {}
        tmp_row['ID'] = carro['UniqueId']

        specs = carro['Specification']
        tmp_row = self.__carrega_specs(tmp_row, specs)

        vendedor = carro['Seller']
        tmp_row = self.__carrega_vendedor(tmp_row, vendedor)
        
        
        precos = carro['Prices']
        tmp_row = self.__carrega_precos(tmp_row, precos)

        if 'LongComment' in carro.keys():
            tmp_row['COMENTARIO_DONO'] = carro['LongComment']

        if 'FipePercent' in carro.keys():
            tmp_row['PORCENTAGEM_FIPE'] = carro['FipePercent']

        tmp_row['DATA_EXTRACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        return tmp_row

    def get_recent_cars(self) -> pd.DataFrame:
        # DataFrame for batching
        carros_webmotors = pd.DataFrame(columns=['ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO','OBSERVACOES','BLINDADO','COR'
        ,'TIPO_VENDEDOR','CIDADE_VENDEDOR','ESTADO_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO','PORCENTAGEM_FIPE'])

        # requisitions counter, for the ETL we want to make 300
        timeout = time.time() + 60*30   # 5 minutes from now
        contador = 1

        while True:
            url = 'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros/estoque?o=8&actualPage='+str(contador)+'&displayPerPage=24&order=8&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false'
            
            # Makes request and handles possibility of a 500 response
            response = requests.get(url = url, headers=self.req_headers)
            while response.status_code >= 500:
                response = requests.get(url = url, headers=self.req_headers)


            data = response.json()
            carros = data['SearchResults']
            for carro in carros:
                carro_row = self.__carrega_carro_df_wh(carro, carros_webmotors)
                carros_webmotors = carros_webmotors.append(carro_row, ignore_index=True)

            # 30 minutes batching
            if time.time() > timeout:
                break

            # next page
            contador += 1

            # API restrictions(?)
            sleep(5)
            
        carros_webmotors.drop_duplicates(inplace=True)
        
        return carros_webmotors

#   Transforming part
    # todo: fill null values for dummy columns

    def clean_str_column(column):
        # removes accents
        column = column.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        # return uppercase
        return column.str.upper()

    def to_upper(column):
        # return uppercase
        return column.str.upper()
    
    def to_str(column):
        return column.astype(str)

    def compute_blindado_webmotors(column):
        return np.where(column == 'N', 0, 1)

    def compute_bool(column):
        return np.where(column == 'true', 1, 0)

    def to_float(column):
        return column.astype(float)

    
    