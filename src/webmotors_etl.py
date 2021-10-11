import requests
import pandas as pd
import time
from time import sleep


req_headers = {
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

def carrega_carro_df_wh(carro, df):
    tmp_row = {}
    tmp_row['ID'] = carro['UniqueId']

    specs = carro['Specification']
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
                obs += atributo_desc

        tmp_row['OBSERVACOES'] = obs
        
    tmp_row['BLINDADO'] = specs['Armored']
    tmp_row['COR'] = specs['Color']['Primary']

    vendedor = carro['Seller']
    tmp_row['TIPO_VENDEDOR'] = vendedor['SellerType']

    if 'City' in vendedor.keys(): 
        tmp_row['CIDADE_VENDEDOR'] = vendedor['City']
    tmp_row['ESTADO_VENDEDOR'] = vendedor['State']
    tmp_row['AD_TYPE'] = vendedor['AdType']['Value']
    tmp_row['SCORE_VENDEDOR'] = vendedor['DealerScore']
    tmp_row['ENTREGA?'] = vendedor['CarDelivery']
    tmp_row['TROCA_COM_TROCO'] = vendedor['TrocaComTroco']
    
    precos = carro['Prices']
    tmp_row['PRECO'] = precos['Price']
    tmp_row['PRECO_DESEJADO'] = precos['SearchPrice']

    if 'LongComment' in carro.keys():
        tmp_row['COMENTARIO_DONO'] = carro['LongComment']

    if 'FipePercent' in carro.keys():
        tmp_row['PORCENTAGEM_FIPE'] = carro['FipePercent']

    df = df.append(tmp_row, ignore_index=True)
    
    return df


def get_recent_cars():
    # DataFrame for batching
    carros_webmotors = pd.DataFrame(columns=['ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO','OBSERVACOES','BLINDADO','COR'
    ,'TIPO_VENDEDOR','CIDADE_VENDEDOR','ESTADO_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA?','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO','PORCENTAGEM_FIPE'])

    # requisitions counter, for the ETL we want to make 300
    timeout = time.time() + 60*30   # 5 minutes from now
    contador = 1

    while True:
        url = 'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros/estoque?o=8&actualPage='+str(contador)+'&displayPerPage=24&order=8&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false'
        
        # Makes request and handles possibility of a 500 response
        response = requests.get(url = url, headers=req_headers)
        while response.status_code >= 500:
            response = requests.get(url = url, headers=req_headers)


        data = response.json()
        carros = data['SearchResults']
        for carro in carros:
            carros_webmotors = carrega_carro_df(carro, carros_webmotors)

        if time.time() > timeout:
            break

        # next page
        contador += 1

        # API restrictions(?)
        sleep(5)
        
    carros_webmotors.drop_duplicates(inplace=True)
    
    return carros_webmotors


