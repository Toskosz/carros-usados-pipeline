from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
import datetime
import ast
import numpy as np
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p

class AutolineTransform:

    def __init__(self) -> None:
        self.files_path = "raw/autoline/"
        self.spark = SparkSession.builder.appName("autoline transformation").getOrCreate()

        self.dummy_columns = {
            'Ar Condicionado':'AR_CONDICIONADO',
            'Teto Solar':'TETO_SOLAR',
            'Bancos em Couro':'BANCO_DE_COURO',
            'Alarme':'ALARME',
            'Freios ABS':'FREIO_ABS',
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
            'Escapamento Esportivo':'ESCAPAMENTO_ESPORTIVO',
            'ABS':'FREIO_ABS'
        }

        self.columns_func_assigns = {
            "INFORMACOES_ADICIONAIS": [limpar_string, tirarcontrabarran],
            "CORPO_VEICULO": [limpar_string],
            "ANO_FABRICACAO": [to_str],
            "CIDADE": [limpar_string],
            "COR": [limpar_string],
            "DATA_ATUALIZACAO_AUTOLINE": [to_datetime],
            "DATA_CRIACAO_AD": [to_datetime],
            "QNTD_PORTAS": [to_number],
            "COMBUSTIVEL": [limpar_string],
            "BLINDADO": [bool_to_int],
            "COLECIONADOR": [bool_to_int],
            "ADAPTADO_DEFICIENCIA": [bool_to_int],
            "FINANCIAVEL": [bool_to_int],
            "FINANCIADO": [bool_to_int],
            "GARANTIA_DE_FABRICA": [bool_to_int],
            "NOVO": [bool_to_int],
            "DONO_UNICO": [bool_to_int],
            "QUITADO": [bool_to_int],
            "REGISTRAMENTO_PAGO": [bool_to_int],
            "VENDEDOR_PJ": [bool_to_int],
            "NAO_ACEITA_TROCA": [bool_to_int],
            "IMPOSTOS_PAGOS": [bool_to_int],
            "ZEROKM": [limpar_string],
            "KILOMETRAGEM": [to_float],
            "FABRICANTE": [limpar_string],
            "MODELO": [limpar_string],
            "ANO_MODELO": [to_str],
            "BAIRRO": [limpar_string],
            "PRECO": [to_float],
            "PRECO_FIPE": [],
            "DATA_DE_REGISTRO": [to_datetime],
            "COR_SECUNDARIA": [limpar_string],
            "TIPO_VEICULO": [limpar_string],
            "ENDERECO": [limpar_string],
            "COMPLEMENTO_ENDERECO": [limpar_string],
            "DOCUMENTO_VENDEDOR": [to_str],
            "NOME_VENDEDOR": [limpar_string],
            "UF": [limpar_string],
            "ESTADO": [limpar_string],
            "TRANSMISSAO": [limpar_string],
            "TIPO_VENDEDOR": [limpar_string],
            "DATA_ATT_AD": [to_datetime],
            "VERSAO": [limpar_string],
        }