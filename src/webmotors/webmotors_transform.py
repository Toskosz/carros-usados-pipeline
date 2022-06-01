from datetime import datetime
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
from unidecode import unidecode
import psycopg2.extras as p
import numpy as np
import pandas as pd
import re

def __get_dummy_columns(): 
    return {'Aceita troca':'ACEITA_TROCA',
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
        'Rádio e toca fitas':'RADIO_TOCAFITA',
        'Retrovisor fotocrômico':'RETROVISOR_FOTOCROMICO',
        'Retrovisores elétricos':'RETROVISOR_ELETRICO',
        'Rodas de liga leve':'RODAS_LIGA_LEVE',
        'Sensor de chuva':'SENSOR_DE_CHUVA',
        'Sensor de estacionamento':'SENSOR_DE_ESTACIONAMENTO',
        'Teto solar':'TETO_SOLAR',
        'Tração 4x4':'TRACAO_QUATRO_POR_QUATRO',
        'Travas elétricas':'TRAVAS_ELETRICAS',
        'Vidros elétricos':'VIDROS_ELETRICOS',
        'Volante com regulagem de altura':'VOLANTE_REG_ALTURA'}

def __get_columns_func_assigns():
    return {"TITULO": [__clean_str_column],
        "FABRICANTE": [__clean_str_column],
        "MODELO": [__clean_str_column],
        "VERSAO": [__clean_str_column],
        "ANO_MODELO": [__to_str],
        "KILOMETRAGEM":[__to_float],
        "TRANSMISSAO": [__clean_str_column],
        "CORPO_VEICULO": [__clean_str_column],
        "BLINDADO": [__compute_BLINDADO],
        "COR": [__clean_str_column],
        "TIPO_VENDEDOR": [__clean_str_column],
        "CIDADE_VENDEDOR": [__clean_str_column],
        "ESTADO_VENDEDOR": [__clean_str_column],
        "UF_VENDEDOR": [__clean_str_column],
        "TIPO_ANUNCIO": [__clean_str_column],
        "ENTREGA_CARRO": [__compute_bool],
        "TROCA_COM_TROCO": [__compute_bool],
        "PRECO": [__to_float],
        "PORCENTAGEM_FIPE": [__to_float],
        "COMBUSTIVEL": [__clean_str_column],
        "COMENTARIO_DONO": [__clean_str_column]}

def __create_dummy_columns(linha):
    for original_name, column_name in __get_dummy_columns.items():
        if original_name in str(linha['ATRIBUTOS']) or original_name in str(linha['OPTIONALS']):
            linha[column_name] = True
        else:
            linha[column_name] = False

    return linha

def __properly_fill_na(df):  
    str_cols = ['TITULO', 'FABRICANTE', 'MODELO', 'VERSAO', 'ANO_FABRICACAO', 'ANO_MODELO', 
    'TRANSMISSAO', 'QNTD_PORTAS', 'CORPO_VEICULO', 'COR', 'TIPO_VENDEDOR', 'CIDADE_VENDEDOR', 
    'ESTADO_VENDEDOR', 'UF_VENDEDOR', 'TIPO_ANUNCIO', 'COMENTARIO_DONO', 'COMBUSTIVEL']
    num_cols = ['KILOMETRAGEM', 'PRECO', 'PRECO_DESEJADO', 'PORCENTAGEM_FIPE']
    bool_cols = ['BLINDADO', 'ENTREGA_CARRO', 'TROCA_COM_TROCO']

    df[num_cols] = df[num_cols].fillna(value=0)
    df[bool_cols] = df[bool_cols].fillna(value=False)
    df[str_cols] = df[str_cols].fillna(value="INDISPONIVEL")
        
    return df

def __to_str(column):
    return column.astype(str)

def __compute_bool(column):
    return np.where(column=="true", True, False)

def __to_float(column):
    return column.astype(float)

def __clean_str_column(column):
    return column.map(lambda x: re.sub('[!,*)@#%(&$_?.^]', '', unidecode(x.strip())) if not pd.isna(x) else x).str.upper()

def __compute_BLINDADO(column):
    return np.where(column=="S", True, False)

def __load_data(data):
    with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
        curr.execute('truncate table stg.webmotors')
        p.execute_batch(curr, __get_exchange_insert_query(data,"stg.webmotors"), data.values)
        curr.execute(open("src/sql_scripts/webmotors_to_star_schema.sql", "r").read())

def __get_exchange_insert_query(self,df,table) -> str:
    df_columns = list(df)
    
    return "INSERT INTO {} ({}) {}".format(table, ", ".join(df_columns), "VALUES ({})".format(", ".join(["%s" for _ in df_columns])))

def run(default_dataframe) -> None:
    df_with_dummys = default_dataframe.apply(__create_dummy_columns, axis=1).drop(['ATRIBUTOS', 'OPTIONALS'], axis=1)

    # separation of UF and ESTADO from ESTADO column
    df_with_dummys['UF_VENDEDOR'] = df_with_dummys["ESTADO_VENDEDOR"].apply(lambda st: st[st.find("(")+1:st.find(")")])
    df_with_dummys['ESTADO_VENDEDOR'] = df_with_dummys["ESTADO_VENDEDOR"].apply(lambda st: st[:st.find("(")])

    for coluna, lst_f in __get_columns_func_assigns.items():
        for f in lst_f:
            df_with_dummys[coluna] = f(df_with_dummys[coluna])

    df_with_dummys['DATA_CARGA'] = datetime.now()
    df_with_dummys['WEBSITE'] = "WEBMOTORS"

    data_to_load = __properly_fill_na(df_with_dummys)

    print("[LOG] Finished transformations")

    __load_data(data_to_load)
    print("[LOG] Finished load to DB")