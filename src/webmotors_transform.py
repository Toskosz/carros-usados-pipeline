from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
from pyspark.sql.functions import regexp_replace, to_timestamp, udf, translate, upper, substring_index
from pyspark.sql.types import StringType
import unicodedata
import datetime
import ast
import numpy as np
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p
import sys

class WebmotorsTransform:

    def __init__(self) -> None:
        self.files_path = "raw/webmotors/"
        self.spark = SparkSession.builder.appName("webmotors transformation").getOrCreate()
        self.matching_string, self.replace_string = self.__make_trans()

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
            'Volante com regulagem de altura':'VOLANTE_REG_ALTURA'
        }

        self.columns_func_assigns = {
            "TITULO": [self.__clean_str_column],
            "FABRICANTE": [self.__clean_str_column],
            "MODELO": [self.__clean_str_column],
            "VERSAO": [self.__clean_str_column],
            # "ANO_FABRICACAO": [self.__to_str],    # todo: 20 -> 2020 verify this and
            "ANO_MODELO": [self.__to_str],          # todo: 20 -> 2020 correct it
            "KILOMETRAGEM":[self.__to_float],
            "TRANSMISSAO": [self.__clean_str_column],
            # "QNTD_PORTAS": [self.__to_str],
            "CORPO_VEICULO": [self.__clean_str_column],
            "BLINDADO": [self.__compute_BLINDADO],
            "COR": [self.__clean_str_column],
            "TIPO_VENDEDOR": [self.__clean_str_column],
            "CIDADE_VENDEDOR": [self.__clean_str_column],
            "ESTADO_VENDEDOR": [self.__clean_str_column],
            "UF_VENDEDOR": [self.__clean_str_column],
            "TIPO_ANUNCIO": [self.__clean_str_column],
            "ENTREGA_CARRO": [self.__compute_bool],
            "TROCA_COM_TROCO": [self.__compute_bool],
            "PRECO": [self.__to_float],
            "PORCENTAGEM_FIPE": [self.__to_float],
            "COMBUSTIVEL": [self.__clean_str_column],
            "COMENTARIO_DONO": [self.__clean_str_column]
        }

    def run(self, last_file=None) -> None:
        if not last_file:
            last_file = self.__get_last_file()
        
        data = self.spark.read.csv(self.files_path + last_file + ".csv", header=True)
        
        # todo: maybe the below process is slow. investigate later.
        # updates dummy columns to data
        for original_name, column_name in self.dummy_columns:
            data = data.withColumn(column_name, self.__has_att(original_name, data.ATRIBUTOS, data.OPTIONALS))

        # drop atributos and optionals column
        data_with_dummy_columns = data.drop("ATRIBUTOS","OPTIONALS")

        # separation of UF and ESTADO from ESTADO column
        data_with_uf = data_with_dummy_columns.withColumn("UF_VENDEDOR", self.__compute_UF(data_with_dummy_columns.ESTADO_VENDEDOR))
        data_to_type_compute = data_with_uf.withColumn("ESTADO_VENDEDOR", self.__compute_ESTADO(data_with_uf.ESTADO_VENDEDOR))
        
        # types, string cleaning, computes special columns
        for coluna, lst_f in self.columns_func_assigns.items():
            for f in lst_f:
                data_to_type_compute = data_to_type_compute.withColumn(coluna, f(data[coluna]))

        # fills na values and creates DATA_CARGA column with datetime of load
        data_filled_na = data_to_type_compute.na.fill("INDISPONIVEL")
        data_to_load = data_filled_na.withColumn("DATA_CARGA", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

        # Uses pandas dataframe to make the load because i cant do it with
        # pyspark at the moment
        # todo: load with pyspark dataframe
        pandas_dataframe = data_to_load.toPandas()

        self.__load_data(pandas_dataframe.values)
        
        self.spark.stop()

    def __make_trans():
        matching_string = ""
        replace_string = ""

        for i in range(ord(" "), sys.maxunicode):
            name = unicodedata.name(chr(i), "")
            if "WITH" in name:
                try:
                    base = unicodedata.lookup(name.split(" WITH")[0])
                    matching_string += chr(i)
                    replace_string += base
                except KeyError:
                    pass

        return matching_string, replace_string

    def __to_str(column):
        return column.cast(StringType())

    def __compute_bool(column):
        boolDict = {'true':1,'false':0}

        map_func = udf(lambda row : boolDict.get(row,row))
        return map_func(column)

    def __to_float(column):
        return column.cast('float')

    # removes special characters and uppercase it
    def __clean_str_column(self, column):
        normalized_column = translate(regexp_replace(column, "\p{M}", ""), self.matching_string, self.replace_string)
        return upper(normalized_column)

    # get most recent csv data file
    def __get_last_file(self):
        # get all csv files
        files = {f.removesuffix(".csv") : datetime.strptime(f.removesuffix(".csv"), '%Y%m%d%H') for f in listdir(self.files_path) if isfile(join(self.files_path, f))}
        # latest file
        return max(files, key=files.get)

    # computes column BLINDADO
    def __compute_BLINDADO(blindado_column):
        boolDict = {'S':1,'N':0}

        map_func = udf(lambda row : boolDict.get(row,row))
        return map_func(blindado_column)

    # separates the ESTADO_VENDEDOR into two colums, ESTADO_VENDEDOR and UF_VENDEDOR
    def __compute_UF(estados):
        estados_tmp = substring_index(estados, '(', -1) # DF)
        estados_tmp = substring_index(estados, ')', 1) # DF
        return estados_tmp

    # computes new estado values without uf
    def __compute_ESTADO(estados):
        estados_tmp = substring_index(estados, '(', 1)
        return estados_tmp

    # verifies if dummy column exists in the row atributos and optionals
    def __has_att(original_name,atts_atributos, atts_optionals):
        atts_atributos_val = atts_atributos.replace('[', '')
        atts_atributos_val = atts_atributos_val.replace(']', '')

        atts_optionals_val = atts_optionals.replace('[', '')
        atts_optionals_val = atts_optionals_val.replace(']', '')

        atts_val = atts_atributos_val + ',' + atts_optionals

        # string representation of dicts to actual list of dicts
        atts_dict = ast.literal_eval(atts_val)

        for att_dict in atts_dict:
            for _, attribute_desc in att_dict.items():
                if original_name == attribute_desc:
                    return 1
    
        return 0

    def __load_data(self,data):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            p.execute_batch(curr, self.__get_exchange_insert_query(), data)

    def __get_exchange_insert_query() -> str:
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
            UF_VENDEDOR,
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
            RADIO_TOCAFITA,
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
            COMBUSTIVEL,
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
            %(UF_VENDEDOR)s,
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
            %(RADIO_TOCAFITA)s,
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
            %(COMBUSTIVEL)s,
            %(DATA_CARGA)s
        );
        '''