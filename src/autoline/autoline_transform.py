from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
from datetime import datetime
import psycopg2.extras as p
import numpy as np
from unidecode import unidecode
import re
import pandas as pd
import logging


def __get_dummy_columns():
    return {'Ar Condicionado':'AR_CONDICIONADO','Teto Solar':'TETO_SOLAR',
        'Bancos em Couro':'BANCO_DE_COURO',
        'Alarme':'ALARME',
        'ABS':'FREIO_ABS',
        'Sensor de Estacionamento':'SENSOR_DE_ESTACIONAMENTO',
        'Computador de Bordo':'COMPUTAR_DE_BORDO',
        'Air Bag':'AIRBAG',
        'Ar Quente':'AR_QUENTE',
        'Rodas de Liga Leve':'RODAS_LIGA_LEVE',
        'Air Bag Duplo':'AIRBAG_DUPLO',
        'Volante com Regulagem de Altura':'VOLANTE_REG_ALTURA',
        'Farol De Milha':'FAROL_DE_MILHA',
        'Banco com Ajuste de Altura':'BANCO_REGULA_ALTURA',
        'CD e MP3 Player':'MP3_CD_PLAYER',
        'Vidros Elétricos':'VIDROS_ELETRICOS',
        'Travas Elétricas':'TRAVAS_ELETRICAS',
        'Desembaçador Traseiro':'DESEMBACADOR_TRASEIRO',
        'Direção Hidráulica':'DIR_HIDRAULICA',
        'Retrovisores Elétricos':'RETROVISOR_ELETRICO',
        'Limpador Traseiro':'LIMPADOR_TRASEIRO',
        'Encosto de Cabeça Traseiro':'ENCOSTO_CABECA_TRASEIRO',
        'Direção Elétrica':'DIR_ELETRICA',
        'Rádio AM/FM':'RADIO',
        'Kit Multimídia':'KIT_MULTIMIDIA',
        'Controle de Tração':'CONTROLE_TRACAO',
        'Controle Automático de Velocidade':'CONTROLE_AUTOMATICO_VEL',
        'GPS':'GPS',
        'CD Player':'CD_PLAYER',
        'Faróis de Neblina':'FAROL_NEBLINA',
        'Retrovisor Fotocrômico':'RETROVISOR_FOTOCROMICO',
        'Sensor de Chuva':'SENSOR_DE_CHUVA',
        'Tração 4x4':'TRACAO_QUATRO_POR_QUATRO',
        'Piloto Automático':'PILOTO_AUTOMATICO',
        'Protetor de Caçamba':'PROTETOR_CACAMBA',
        'Capota Marítima':'CAPOTA_MARITIMA',
        'DVD Player':'DVD_PLAYER',
        'Farol Xenônio':'FAROL_DE_XENONIO',
        'Bancos com Aquecimento':'BANCO_COM_AQUECIMENTO',
        'Rádio e Toca Fitas':'RADIO_TOCAFITA',
        'Disqueteira':'DISQUETEIRA',
        'Escapamento Esportivo':'ESCAPAMENTO_ESPORTIVO'}

def __get_columns_func_assigns():
    return {"INFORMACOES_ADICIONAIS": [__remove_jump_line],
        "CORPO_VEICULO": [__clean_str_column],
        "ANO_FABRICACAO": [__to_str],
        "CIDADE": [__clean_str_column],
        "COR": [__clean_str_column],
        "QNTD_PORTAS": [__clean_str_column, __to_number],
        "COMBUSTIVEL": [__clean_str_column],
        "BLINDADO": [__compute_bool],
        "COLECIONADOR": [__compute_bool],
        "ADAPTADO_DEFICIENCIA": [__compute_bool],
        "FINANCIAVEL": [__compute_bool],
        "FINANCIADO": [__compute_bool],
        "GARANTIA_DE_FABRICA": [__compute_bool],
        "DONO_UNICO": [__compute_bool],
        "QUITADO": [__compute_bool],
        "REGISTRAMENTO_PAGO": [__compute_bool],
        "VENDEDOR_PJ": [__compute_bool],
        "ACEITA_TROCA": [__compute_inverse_bool],
        "IMPOSTOS_PAGOS": [__compute_bool],
        "KILOMETRAGEM": [__to_float],
        "FABRICANTE": [__clean_str_column],
        "MODELO": [__clean_str_column],
        "ANO_MODELO": [__to_str],
        "BAIRRO": [__clean_str_column],
        "PRECO": [__to_float],
        "PRECO_FIPE": [__fix_empty_string, __to_float],
        "COR_SECUNDARIA": [__clean_str_column],
        "TIPO_VEICULO": [__clean_str_column],
        "ENDERECO": [__clean_str_column],
        "COMPLEMENTO_ENDERECO": [__clean_str_column],
        "DOCUMENTO_VENDEDOR": [__to_str],
        "NOME_VENDEDOR": [__clean_str_column],
        "UF": [__clean_str_column],
        "ESTADO": [__clean_str_column],
        "TRANSMISSAO": [__clean_str_column],
        "TIPO_VENDEDOR": [__clean_str_column],
        "VERSAO": [__clean_str_column],
        "PLACA": [__fix_empty_string],
        "MOTOR": [__clean_str_column, __fix_empty_string]}

def __create_dummy_columns(linha):
    linha['RECURSOS'] = str(linha['RECURSOS'])

    for original_name, column_name in __get_dummy_columns().items():
        if original_name in str(linha['RECURSOS']):
            linha[column_name] = True
        else:
            linha[column_name] = False

    return linha

def __properly_fill_na(df):

    num_cols = ['KILOMETRAGEM','PRECO','PRECO_FIPE']
    bool_cols = ['BLINDADO','COLECIONADOR','ADAPTADO_DEFICIENCIA','FINANCIAVEL','FINANCIADO',
    'GARANTIA_DE_FABRICA','DONO_UNICO','QUITADO','REGISTRAMENTO_PAGO','VENDEDOR_PJ',
    'ACEITA_TROCA','IMPOSTOS_PAGOS']
    str_cols = ['INFORMACOES_ADICIONAIS', 'CORPO_VEICULO', 'ANO_FABRICACAO', 'CIDADE', 'COR', 
    'QNTD_PORTAS', 'EMAIL', 'MOTOR', 'COMBUSTIVEL', 'LINK_AD', 'FABRICANTE', 'CELULAR', 'MODELO', 
    'ANO_MODELO', 'BAIRRO', 'TELEFONE', 'PLACA', 'COR_SECUNDARIA', 'TIPO_VEICULO', 'ENDERECO', 
    'COMPLEMENTO_ENDERECO', 'DOCUMENTO_VENDEDOR', 'NOME_VENDEDOR', 'UF', 'ESTADO', 'TRANSMISSAO', 
    'TIPO_VENDEDOR', 'VERSAO', 'WHATSAPP']

    df[num_cols] = df[num_cols].fillna(value=0)
    df[bool_cols] = df[bool_cols].fillna(value=False)
    df[str_cols] = df[str_cols].fillna(value="INDISPONIVEL")

    return df

def __to_number(column):
    return column.map({'ZERO': '0', 'UM': '1', 'DOIS': '2',
        'TRES': '3', 'QUATRO': '4', 'CINCO': '5', 'SEIS': '6', 
        'SETE': '7', 'OITO': '8', 'NOVE': '9'}, na_action='ignore')

def __to_float(column):
    return column.astype(float)

def __to_str(column):
    return column.astype(str)

def __compute_bool(column):
    return np.where(column=='VERDADEIRO', True, False)

def __fix_empty_string(column):
    return np.where(column=="", "INDISPONIVEL", column)

def __compute_inverse_bool(column):
    return np.where(column=='VERDADEIRO', False, True)

def __remove_jump_line(column):
    return column.replace(to_replace=r"\\n", value="", regex=True)

def __clean_str_column(column):
    return column.map(lambda x: re.sub('[!,*)@#%(&$_?.^]', '', unidecode(x.strip())) if not pd.isna(x) else x).str.upper()

def __load_data(data):
    with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
        curr.execute('truncate table stg.autoline')
        p.execute_batch(curr, __get_exchange_insert_query(data,"stg.autoline"), data.values)
        curr.execute(open("/code/src/sql_scripts/autoline_to_star_schema.sql", "r").read())

def __get_exchange_insert_query(df,table) -> str:
    df_columns = list(df)
    return "INSERT INTO {} ({}) {}".format(table, ", ".join(df_columns), "VALUES ({})".format(", ".join(["%s" for _ in df_columns])))

def run(default_dataframe) -> None:
    df_with_dummys = default_dataframe.apply(__create_dummy_columns, axis=1).drop('RECURSOS', axis=1)

    for coluna, lst_f in __get_columns_func_assigns().items():
        for f in lst_f:
            df_with_dummys[coluna] = f(df_with_dummys[coluna])

    df_with_dummys['DATA_CARGA'] = datetime.now()
    df_with_dummys['WEBSITE'] = "AUTOLINE"

    data_to_load = __properly_fill_na(df_with_dummys)

    logging.info("[LOG] Finished transformations")
    logging.info(str(data_to_load.shape))
    __load_data(data_to_load)
    logging.info("[LOG] Finished load to DB")
