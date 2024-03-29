import pandas as pd
from unidecode import unidecode
import logging


def __get_req_headers():
    return {'Accept':	'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
    'Connection':	'keep-alive',
    'Host':	'www.webmotors.com.br',
    'Sec-Fetch-Dest':	'document',
    'Sec-Fetch-Mode':	'navigate',
    'Sec-Fetch-Site':	'none',
    'Sec-Fetch-User':	'?1',
    'Upgrade-Insecure-Requests':	'1',
    'User-Agent':	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0'}

def __load_specs(specs) -> dict:
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

def __load_seller(seller) -> dict:
    tmp_row = {}

    tmp_row['TIPO_VENDEDOR'] = seller['SellerType']

    if 'City' in seller.keys():
        tmp_row['CIDADE_VENDEDOR'] = seller['City']

    tmp_row['ESTADO_VENDEDOR'] = seller['State']

    tmp_row['TIPO_ANUNCIO'] = seller['AdType']['Value']
    tmp_row['ENTREGA_CARRO'] = seller['CarDelivery']
    tmp_row['TROCA_COM_TROCO'] = seller['TrocaComTroco']

    return tmp_row

def __load_prices(prices) -> dict:
    tmp_row = {}
    tmp_row['PRECO'] = prices['Price']
    tmp_row['PRECO_DESEJADO'] = prices['SearchPrice']
    return tmp_row

def __get_optionals(op_url, client):
    tmp_row = {}

    op_response = client.get(url = op_url)

    try:
        tmp_specs = op_response.json()['Specification']
    except Exception as E:
        logging.error(op_url)
        raise(E)

    if 'Fuel' in tmp_specs:
        tmp_row['COMBUSTIVEL'] = tmp_specs['Fuel']

    if 'Optionals' in tmp_specs:
        tmp_row['OPTIONALS'] = tmp_specs['Optionals']

    return tmp_row

def __load_car(car, client) -> dict:
    tmp_row = {}
    tmp_row['AD_ID'] = car['UniqueId']

    tmp_row.update(__load_specs(car['Specification']))
    tmp_row.update(__load_seller(car['Seller']))
    tmp_row.update(__load_prices(car['Prices']))

    if 'LongComment' in car.keys():
        tmp_row['COMENTARIO_DONO'] = car['LongComment']

    if 'FipePercent' in car.keys():
        tmp_row['PORCENTAGEM_FIPE'] = car['FipePercent']

    tmp_row.update(__get_optionals(__make_opt_url(tmp_row), client))

    return tmp_row

def __make_opt_url(tmp_row):
    tmp_versao = unidecode(tmp_row['VERSAO'])
    tmp_fabricante = unidecode(tmp_row['FABRICANTE'])
    tmp_modelo = unidecode(tmp_row['MODELO'])

    op_url = "https://www.webmotors.com.br/api/detail/car/" + tmp_fabricante.lower().replace(' ','-') + "/" + tmp_modelo.replace(' ', '-').lower() + "/" + \
    tmp_versao.replace('.','').replace(' ','-').lower() + "/" + tmp_row['QNTD_PORTAS'] + "-portas/" + tmp_row['ANO_FABRICACAO'] + "-" + str(int(tmp_row['ANO_MODELO'])) + "/" + \
    str(tmp_row['AD_ID'])

    return op_url

def __get_recent_cars(max_batch_size, client) -> pd.DataFrame:

    client.headers.update(__get_req_headers())

    cars = pd.DataFrame(columns=['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
    'ATRIBUTOS','BLINDADO','COR','TIPO_VENDEDOR','CIDADE_VENDEDOR','ESTADO_VENDEDOR','TIPO_ANUNCIO','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO',
    'PORCENTAGEM_FIPE','OPTIONALS','COMBUSTIVEL'])

    counter = 1

    while len(cars.index) <= max_batch_size:
        url = ''.join(['https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros/estoque?o=8&actualPage='
        , str(counter), '&displayPerPage=24&order=8&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false'])

        response = client.get(url = url)

        for car in response.json()['SearchResults']:
            cars = cars.append(__load_car(car, client), ignore_index=True)

        counter += 1

    return cars.head(max_batch_size)

def run(max_batch_size, client):
    logging.info("[LOG] Extracting...")
    data = __get_recent_cars(max_batch_size, client)
    # TODO: Load raw data to S3
    logging.info("[LOG] Done extracting.")
    return data
