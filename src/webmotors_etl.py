import requests
import pandas as pd
from datetime import datetime
import time
from time import sleep
import unidecode

class WebmotorsExtract:

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

    def run(self, max_batch_size) -> None:
        data = self.__get_recent_cars(max_batch_size)
        now = datetime.now()
        str_hora = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
        data.to_csv('raw/webmotors/'+str_hora+'.csv',index=False)
        return data

#   Extraction part

    def __carrega_specs(self, specs) -> dict:
        tmp_row = {}
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
            # list of dicts
            tmp_row['ATRIBUTOS'] = specs['VehicleAttributes']
            
            
        tmp_row['BLINDADO'] = specs['Armored']
        tmp_row['COR'] = specs['Color']['Primary']

        return tmp_row

    def __carrega_vendedor(self, vendedor) -> dict:
        tmp_row = {}

        tmp_row['TIPO_VENDEDOR'] = vendedor['SellerType']

        if 'City' in vendedor.keys(): 
            tmp_row['CIDADE_VENDEDOR'] = vendedor['City']

        tmp_row['ESTADO_VENDEDOR'] = vendedor['State']

        tmp_row['AD_TYPE'] = vendedor['AdType']['Value']
        tmp_row['SCORE_VENDEDOR'] = vendedor['DealerScore']
        tmp_row['ENTREGA_CARRO'] = vendedor['CarDelivery']
        tmp_row['TROCA_COM_TROCO'] = vendedor['TrocaComTroco']

        return tmp_row

    def __carrega_precos(self, precos) -> dict:
        tmp_row = {}
        tmp_row['PRECO'] = precos['Price']
        tmp_row['PRECO_DESEJADO'] = precos['SearchPrice']
        return tmp_row

    def __get_optionals(self, op_url):
        tmp_row = {}

        op_response = requests.get(url = op_url, headers=self.req_headers)
        while op_response.status_code >= 500:
            op_response = requests.get(url = op_url, headers=self.req_headers)

        sleep(5)
        op_data = op_response.json()

        try:
            tmp_specs = op_data['Specification']
        except Exception as E:
            print(op_url)
            raise(E)

        if 'Fuel' in tmp_specs:
            tmp_row['COMBUSTIVEL'] = tmp_specs['Fuel']

        if 'Optionals' in tmp_specs:
            tmp_row['OPTIONALS'] = tmp_specs['Optionals']

        return tmp_row

    # Moves the data to a dict to be appended to the batch dataframe
    # with the right column names
    def __carrega_carro(self, carro) -> dict:
        tmp_row = {}
        tmp_row['AD_ID'] = carro['UniqueId']

        # SPECS
        specs = carro['Specification']
        tmp_row.update(self.__carrega_specs(specs))

        # VENDEDOR
        vendedor = carro['Seller']
        tmp_row.update(self.__carrega_vendedor(vendedor))
        
        # PRECOS
        precos = carro['Prices']
        tmp_row.update(self.__carrega_precos(precos))

        if 'LongComment' in carro.keys():
            tmp_row['COMENTARIO_DONO'] = carro['LongComment']

        if 'FipePercent' in carro.keys():
            tmp_row['PORCENTAGEM_FIPE'] = carro['FipePercent']
        
        # OPTIONALS
        optionals_url = self.__make_opt_url(tmp_row)
        optionals = self.__get_optionals(optionals_url)
        tmp_row.update(optionals)

        return tmp_row

    def __make_opt_url(self,tmp_row):
        tmp_versao = unidecode.unidecode(tmp_row['VERSAO'])
        tmp_fabricante = unidecode.unidecode(tmp_row['FABRICANTE'])
        tmp_modelo = unidecode.unidecode(tmp_row['MODELO'])
        # example:
        # https://www.webmotors.com.br/api/detail/car/chery/arrizo-6-pro/15-vvt-turbo-iflex-cvt/4-portas/2021-2022/39546505
        op_url = "https://www.webmotors.com.br/api/detail/car/" + tmp_fabricante.lower().replace(' ','-') + "/" + tmp_modelo.replace(' ', '-').lower() + "/" + \
        tmp_versao.replace('.','').replace(' ','-').lower() + "/" + tmp_row['QNTD_PORTAS'] + "-portas/" + tmp_row['ANO_FABRICACAO'] + "-" + str(int(tmp_row['ANO_MODELO'])) + "/" + \
        str(tmp_row['AD_ID'])
        
        return op_url
        

    def __get_recent_cars(self, max_batch_size) -> pd.DataFrame:
        # DataFrame for batching
        carros_webmotors = pd.DataFrame(columns=['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
        'ATRIBUTOS','BLINDADO','COR','TIPO_VENDEDOR','CIDADE_VENDEDOR','ESTADO_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO',
        'PORCENTAGEM_FIPE','OPTIONALS','COMBUSTIVEL'])

        contador = 1

        while len(carros_webmotors.index) <= max_batch_size:
            url = 'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros/estoque?o=8&actualPage='+str(contador)+'&displayPerPage=24&order=8&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false'
            
            # Makes request and handles possibility of a 500 response
            response = requests.get(url = url, headers=self.req_headers)
            while response.status_code >= 500:
                response = requests.get(url = url, headers=self.req_headers)

            # API restrictions(?)
            sleep(5)

            data = response.json()
            carros = data['SearchResults']
            for carro in carros:
                carro_row = self.__carrega_carro(carro)
                carros_webmotors = carros_webmotors.append(carro_row, ignore_index=True)

            # next page
            contador += 1

        return carros_webmotors.head(max_batch_size)
