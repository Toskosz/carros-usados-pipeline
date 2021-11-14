from pandas.io.formats.format import SeriesFormatter
import requests
import pandas as pd
from datetime import datetime
import time
from time import sleep
from util.creds import get_warehouse_creds
import numpy as np
import psycopg2.extras as p
from util.warehouse import WarehouseConnection

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
    "KILOMETRAGEM":12319
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
    'Airbag':'AIRBAG'
    'Alarme':'ALARME'
    'Ar condicionado':'AR_CONDICIONADO'
    'Ar quente':'AR_QUENTE'
    'Banco com regulagem de altura':'BANCO_REGULA_ALTURA'
    'Bancos dianteiros com aquecimento':'BANCO_COM_AQUECIMENTO'
    'Bancos em couro':'BANCO_DE_COURO'
    'Capota marítima':'CAPOTA_MARITIMA'
    'CD e mp3 player':'MP3_CD_PLAYER'
    'CD player':'CD_PLAYER'
    'Computador de bordo':'COMPUTAR_DE_BORDO'
    'Controle automático de velocidade':'CONTROLE_AUTOMATICO_VEL'
    'Controle de tração':'CONTROLE_TRACAO'
    'Desembaçador traseiro':'DESEMBACADOR_TRASEIRO'
    'Direção hidráulica':'DIR_HIDRAULICA'
    'Disqueteira':'DISQUETEIRA'
    'DVD player':'DVD_PLAYER'
    'Encosto de cabeça traseiro':'ENCOSTO_CABECA_TRASEIRO'
    'Farol de xenônio':'FAROL_DE_XENONIO'
    'Freio abs':'FREIO_ABS'
    'GPS':'GPS'
    'Limpador traseiro':'LIMPADOR_TRASEIRO'
    'Protetor de caçamba':'PROTETOR_CACAMBA'
    'Rádio':'RADIO'
    'Rádio e toca fitas':'RADIO_TOCAFICA'
    'Retrovisor fotocrômico':'RETROVISOR_FOTOCROMICO'
    'Retrovisores elétricos':'RETROVISOR_ELETRICO'
    'Rodas de liga leve':'RODAS_LIGA_LEVE'
    'Sensor de chuva':'SENSOR_DE_CHUVA'
    'Sensor de estacionamento':'SENSOR_DE_ESTACIONAMENTO'
    'Teto solar':'TETO_SOLAR'
    'Tração 4x4':'TRACAO_QUATRO_POR_QUATRO'
    'Travas elétricas':'TRAVAS_ELETRICAS'
    'Vidros elétricos':'VIDROS_ELETRICOS'
    'Volante com regulagem de altura':'VOLANTE_REG_ALTURA'
}
'''

class WebmotorsETL:

    def __init__(self) -> None:
        self.dummy_columns = {
            'Aceita troca':'ACEITA_TROCA',
            'Alienado':'ALIENADO',
            'Garantia de fábrica':'GARANTIA_DE_FABRICA',
            'IPVA pago':'IPVA_PAGO',
            'Licenciado':'LICENCIADO',
            'Todas as revisões feitas pela agenda do carro':'REVISOES_PELA_AGENDA_CARRO',
            'Todas as revisões feitas pela concessionária':'REVISOES_PELA_CONCESSIONARIA',
            'Único dono':'UNICO_DONO',
            'Airbag':'AIRBAG',
            'Alarme':'ALARME',
            'Ar condicionado':'AR_CONDICIONADO',
            'Ar quente':'AR_QUENTE',
            'Banco com regulagem de altura':'BANCO_REGULA_ALTURA',
            'Bancos dianteiros com aquecimento':'BANCO_COM_AQUECIMENTO',
            'Bancos em couro':'BANCO_DE_COURO',
            'Capota marítima':'CAPOTA_MARITIMA',
            'CD e mp3 player':'MP3_CD_PLAYER',
            'CD player':'CD_PLAYER',
            'Computador de bordo':'COMPUTAR_DE_BORDO',
            'Controle automático de velocidade':'CONTROLE_AUTOMATICO_VEL',
            'Controle de tração':'CONTROLE_TRACAO',
            'Desembaçador traseiro':'DESEMBACADOR_TRASEIRO',
            'Direção hidráulica':'DIR_HIDRAULICA',
            'Disqueteira':'DISQUETEIRA',
            'DVD player':'DVD_PLAYER',
            'Encosto de cabeça traseiro':'ENCOSTO_CABECA_TRASEIRO',
            'Farol de xenônio':'FAROL_DE_XENONIO',
            'Freio abs':'FREIO_ABS',
            'GPS':'GPS',
            'Limpador traseiro':'LIMPADOR_TRASEIRO',
            'Protetor de caçamba':'PROTETOR_CACAMBA',
            'Rádio':'RADIO',
            'Rádio e toca fitas':'RADIO_TOCAFICA',
            'Retrovisor fotocrômico':'RETROVISOR_FOTOCROMICO',
            'Retrovisores elétricos':'RETROVISOR_ELETRICO',
            'Rodas de liga leve':'RODAS_LIGA_LEVE',
            'Sensor de chuva':'SENSOR_DE_CHUVA',
            'Sensor de estacionamento':'SENSOR_DE_ESTACIONAMENTO',
            'Teto solar':'TETO_SOLAR',
            'Tração 4x4':'TRACAO_QUATRO_POR_QUATRO',
            'Travas elétricas':'TRAVAS_ELETRICAS',
            'Vidros elétricos':'VIDROS_ELETRICOS',
            'Volante com regulagem de altura':'VOLANTE_REG_ALTURA'
        }

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

        self.columns_func_assigns = {
            "TITULO": [self.clean_str_column],
            "FABRICANTE": [self.clean_str_column],
            "MODELO": [self.clean_str_column],
            "VERSAO": [self.clean_str_column],
            "ANO_FABRICACAO": [self.to_str],
            "ANO_MODELO": [self.to_str],
            "KILOMETRAGEM":[self.to_float],
            "TRANSMISSAO": [self.clean_str_column],
            "QNTD_PORTAS": [self.to_str],
            "CORPO_VEICULO": [self.clean_str_column],
            "BLINDADO": [self.compute_blindado_webmotors],
            "COR": [self.clean_str_column],
            "TIPO_VENDEDOR": [self.clean_str_column],
            "CIDADE_VENDEDOR": [self.clean_str_column],
            "ESTADO_VENDEDOR": [self.clean_str_column],
            "TIPO_ANUNCIO": [self.clean_str_column],
            "ENTREGA_CARRO": [self.compute_bool],
            "TROCA_COM_TROCO": [self.compute_bool],
            "PRECO": [self.to_float],
            "PORCENTAGEM_FIPE": [self.to_float]
        }

    def run(self) -> None:
        data = self.get_recent_cars()
        clean_data = self.clean_data(data)
        clean_data['DATA_CARGA'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            p.execute_batch(curr, self._get_exchange_insert_query(), clean_data.values)

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
            atributos = specs['VehicleAttributes']
            for atributo in atributos:
                for _, atributo_desc in atributo.items():
                    column_name = self.dummy_columns[atributo_desc]
                    tmp_row[column_name] = 1
            
        tmp_row['BLINDADO'] = specs['Armored']
        tmp_row['COR'] = specs['Color']['Primary']

        return tmp_row

    def __carrega_vendedor(vendedor) -> dict:
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

    def __carrega_precos(precos) -> dict:
        tmp_row = {}
        tmp_row['PRECO'] = precos['Price']
        tmp_row['PRECO_DESEJADO'] = precos['SearchPrice']
        return tmp_row

    def __get_optionals(self,id):
        tmp_row = {}
        op_url = "https://www.webmotors.com.br/api/detail/car/" + id
        op_response = requests.get(url = op_url, headers=self.req_headers)
        op_data = op_response.json()
        tmp_specs = op_data['Specification']
        optionals = tmp_specs['Optionals']

        for option in optionals:
            column_name = self.dummy_columns[option['Name']]
            tmp_row[column_name] = 1

        return tmp_row

    def __carrega_carro(self, carro) -> pd.DataFrame:
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
        optionals = self.__get_optionals(tmp_row['AD_ID'])
        tmp_row.update(optionals)

        return tmp_row

    def get_recent_cars(self) -> pd.DataFrame:
        # DataFrame for batching
        carros_webmotors = pd.DataFrame(columns=['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
                'ACEITA_TROCA','ALIENADO','GARANTIA_DE_FABRICA','IPVA_PAGO','LICENCIADO','REVISOES_PELA_AGENDA_CARRO','REVISOES_PELA_CONCESSIONARIA','UNICO_DONO','BLINDADO','COR','TIPO_VENDEDOR',
                'CIDADE_VENDEDOR','ESTADO_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO','PORCENTAGEM_FIPE','AIRBAG',
                'ALARME','AR_CONDICIONADO','AR_QUENTE','BANCO_REGULA_ALTURA','BANCO_COM_AQUECIMENTO','BANCO_DE_COURO','CAPOTA_MARITIMA','MP3_CD_PLAYER','CD_PLAYER','COMPUTAR_DE_BORDO',
                'CONTROLE_AUTOMATICO_VEL','CONTROLE_TRACAO','DESEMBACADOR_TRASEIRO','DIR_HIDRAULICA','DISQUETEIRA','DVD_PLAYER','ENCOSTO_CABECA_TRASEIRO','FAROL_DE_XENONIO','FREIO_ABS',
                'GPS','LIMPADOR_TRASEIRO','PROTETOR_CACAMBA','RADIO','RADIO_TOCAFICA','RETROVISOR_FOTOCROMICO','RETROVISOR_ELETRICO','RODAS_LIGA_LEVE','SENSOR_DE_CHUVA',
                'SENSOR_DE_ESTACIONAMENTO','TETO_SOLAR','TRACAO_QUATRO_POR_QUATRO','TRAVAS_ELETRICAS','VIDROS_ELETRICOS','VOLANTE_REG_ALTURA'])

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
                carro_row = self.__carrega_carro(carro)
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

    def clean_data(self,data):
        # keeping it functional programming(?)
        resulting_data = data.copy()

        for coluna, lst_f in self.columns_func_assigns.items():
            for f in lst_f:
                resulting_data[coluna] = f(resulting_data[coluna])
        
        # if dummy columns = 0 zero then car doesnt have feature so "0"
        resulting_data[self.dummy_columns] = resulting_data[self.dummy_columns].fillna(value=0)
        
        # data unavailable
        resulting_data = resulting_data.fillna(value="INDISPONIVEL")
        del resulting_data['OBSERVACOES']
        return resulting_data

    def clean_str_column(column):
        # removes accents
        normalized_column = column.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        # return uppercase
        return normalized_column.str.upper()

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

#   Loading part
    
    def _get_exchange_insert_query(self) -> str:
        return '''
        INSERT INTO STG.WEBMOTORS (
            AD_ID,
            TITULO,
            FABRICANTE,
            MODELO,
            VERSAO,
            ANO_FABRICACAO,
            ANO_MODELO,
            KILOMETRAGEM,
            TRANSMISSAO,
            QNTD_PORTAS,
            CORPO_VEICULO,
            ACEITA_TROCA,
            ALIENADO,
            GARANTIA_DE_FABRICA,
            IPVA_PAGO,
            LICENCIADO,
            REVISOES_PELA_AGENDA_CARRO,
            REVISOES_PELA_CONCESSIONARIA,
            UNICO_DONO,
            BLINDADO,
            COR,
            TIPO_VENDEDOR,
            CIDADE_VENDEDOR,
            ESTADO_VENDEDOR,
            AD_TYPE,
            SCORE_VENDEDOR,
            ENTREGA_CARRO,
            TROCA_COM_TROCO,
            PRECO,
            PRECO_DESEJADO,
            COMENTARIO_DONO,
            PORCENTAGEM_FIPE,
            AIRBAG,
            ALARME,
            AR_CONDICIONADO,
            AR_QUENTE,
            BANCO_REGULA_ALTURA,
            BANCO_COM_AQUECIMENTO,
            BANCO_DE_COURO,
            CAPOTA_MARITIMA,
            MP3_CD_PLAYER,
            CD_PLAYER,
            COMPUTAR_DE_BORDO,
            CONTROLE_AUTOMATICO_VEL,
            CONTROLE_TRACAO,
            DESEMBACADOR_TRASEIRO,
            DIR_HIDRAULICA,
            DISQUETEIRA,
            DVD_PLAYER,
            ENCOSTO_CABECA_TRASEIRO,
            FAROL_DE_XENONIO,
            FREIO_ABS,
            GPS,
            LIMPADOR_TRASEIRO,
            PROTETOR_CACAMBA,
            RADIO,
            RADIO_TOCAFICA,
            RETROVISOR_FOTOCROMICO,
            RETROVISOR_ELETRICO,
            RODAS_LIGA_LEVE,
            SENSOR_DE_CHUVA,
            SENSOR_DE_ESTACIONAMENTO,
            TETO_SOLAR,
            TRACAO_QUATRO_POR_QUATRO,
            TRAVAS_ELETRICAS,
            VIDROS_ELETRICOS,
            VOLANTE_REG_ALTURA,
            DATA_CARGA
        )
        VALUES (
            %(AD_ID)s,
            %(TITULO)s,
            %(FABRICANTE)s,
            %(MODELO)s,
            %(VERSAO)s,
            %(ANO_FABRICACAO)s,
            %(ANO_MODELO)s,
            %(KILOMETRAGEM)s,
            %(TRANSMISSAO)s,
            %(QNTD_PORTAS)s,
            %(CORPO_VEICULO)s,
            %(ACEITA_TROCA)s,
            %(ALIENADO)s,
            %(GARANTIA_DE_FABRICA)s,
            %(IPVA_PAGO)s,
            %(LICENCIADO)s,
            %(REVISOES_PELA_AGENDA_CARRO)s,
            %(REVISOES_PELA_CONCESSIONARIA)s,
            %(UNICO_DONO)s,
            %(BLINDADO)s,
            %(COR)s,
            %(TIPO_VENDEDOR)s,
            %(CIDADE_VENDEDOR)s,
            %(ESTADO_VENDEDOR)s,
            %(AD_TYPE)s,
            %(SCORE_VENDEDOR)s,
            %(ENTREGA_CARRO)s,
            %(TROCA_COM_TROCO)s,
            %(PRECO)s,
            %(PRECO_DESEJADO)s,
            %(COMENTARIO_DONO)s,
            %(PORCENTAGEM_FIPE)s,
            %(AIRBAG)s,
            %(ALARME)s,
            %(AR_CONDICIONADO)s,
            %(AR_QUENTE)s,
            %(BANCO_REGULA_ALTURA)s,
            %(BANCO_COM_AQUECIMENTO)s,
            %(BANCO_DE_COURO)s,
            %(CAPOTA_MARITIMA)s,
            %(MP3_CD_PLAYER)s,
            %(CD_PLAYER)s,
            %(COMPUTAR_DE_BORDO)s,
            %(CONTROLE_AUTOMATICO_VEL)s,
            %(CONTROLE_TRACAO)s,
            %(DESEMBACADOR_TRASEIRO)s,
            %(DIR_HIDRAULICA)s,
            %(DISQUETEIRA)s,
            %(DVD_PLAYER)s,
            %(ENCOSTO_CABECA_TRASEIRO)s,
            %(FAROL_DE_XENONIO)s,
            %(FREIO_ABS)s,
            %(GPS)s,
            %(LIMPADOR_TRASEIRO)s,
            %(PROTETOR_CACAMBA)s,
            %(RADIO)s,
            %(RADIO_TOCAFICA)s,
            %(RETROVISOR_FOTOCROMICO)s,
            %(RETROVISOR_ELETRICO)s,
            %(RODAS_LIGA_LEVE)s,
            %(SENSOR_DE_CHUVA)s,
            %(SENSOR_DE_ESTACIONAMENTO)s,
            %(TETO_SOLAR)s,
            %(TRACAO_QUATRO_POR_QUATRO)s,
            %(TRAVAS_ELETRICAS)s,
            %(VIDROS_ELETRICOS)s,
            %(VOLANTE_REG_ALTURA)s,
            %(DATA_CARGA)s
        );
        '''
