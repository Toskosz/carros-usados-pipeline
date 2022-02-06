from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, to_timestamp, udf, translate, upper
import datetime
import ast
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p
from pyspark.sql.types import StringType
import unicodedata
import sys

class AutolineTransform:

    def __init__(self) -> None:
        self.files_path = "raw/autoline/"
        self.spark = SparkSession.builder.appName("autoline transformation").getOrCreate()
        self.matching_string, self.replace_string = self.__make_trans()

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
            "INFORMACOES_ADICIONAIS": [self.__clean_str_column, self.__remove_jump_line],
            "CORPO_VEICULO": [self.__clean_str_column],
            "ANO_FABRICACAO": [self.__to_str],
            "CIDADE": [self.__clean_str_column],
            "COR": [self.__clean_str_column],
            "DATA_ATUALIZACAO_AUTOLINE": [self.__fix_date_type],
            "DATA_CRIACAO_AD": [self.__fix_date_type],
            "QNTD_PORTAS": [self.__clean_str_column,self.__to_number],
            "COMBUSTIVEL": [self.__clean_str_column],
            "BLINDADO": [self.__compute_bool],
            "COLECIONADOR": [self.__compute_bool],
            "ADAPTADO_DEFICIENCIA": [self.__compute_bool],
            "FINANCIAVEL": [self.__compute_bool],
            "FINANCIADO": [self.__compute_bool],
            "GARANTIA_DE_FABRICA": [self.__compute_bool],
            "NOVO": [self.__compute_bool],
            "DONO_UNICO": [self.__compute_bool],
            "QUITADO": [self.__compute_bool],
            "REGISTRAMENTO_PAGO": [self.__compute_bool],
            "VENDEDOR_PJ": [self.__compute_bool],
            "ACEITA_TROCA": [self.__compute_inverse_bool],
            "IMPOSTOS_PAGOS": [self.__compute_bool],
            "ZEROKM": [self.__clean_str_column],
            "KILOMETRAGEM": [self.__to_float],
            "FABRICANTE": [self.__clean_str_column],
            "MODELO": [self.__clean_str_column],
            "ANO_MODELO": [self.__to_str],
            "BAIRRO": [self.__clean_str_column],
            "PRECO": [self.__to_float],
            "PRECO_FIPE": [],
            "DATA_DE_REGISTRO": [self.__fix_date_type],
            "COR_SECUNDARIA": [self.__clean_str_column],
            "TIPO_VEICULO": [self.__clean_str_column],
            "ENDERECO": [self.__clean_str_column],
            "COMPLEMENTO_ENDERECO": [self.__clean_str_column],
            "DOCUMENTO_VENDEDOR": [self.__to_str],
            "NOME_VENDEDOR": [self.__clean_str_column],
            "UF": [self.__clean_str_column],
            "ESTADO": [self.__clean_str_column],
            "TRANSMISSAO": [self.__clean_str_column],
            "TIPO_VENDEDOR": [self.__clean_str_column],
            "DATA_ATT_AD": [self.__fix_date_type],
            "VERSAO": [self.__clean_str_column],
        }

    def __to_number(column):
        doorsDict = {'ZERO':'0','UM':'1','DOIS':'2','TRES':'3','QUATRO':'4','CINCO':'5','SEIS':'6',
        'SETE':'7', 'OITO':'8', 'NOVE':'9', 'DEZ':'10'}

        map_func = udf(lambda row : doorsDict.get(row,row))
        return map_func(column)

    def __to_float(column):
        return column.cast('float')

    def __to_str(column):
        return column.cast(StringType())

    def __compute_bool(column):
        boolDict = {'VERDADEIRO':1,'FALSO':0}

        map_func = udf(lambda row : boolDict.get(row,row))
        return map_func(column)

    def __compute_inverse_bool(column):
        boolDict = {'VERDADEIRO':0,'FALSO':1}

        map_func = udf(lambda row : boolDict.get(row,row))
        return map_func(column)

    def __fix_date_type(column):
        column_fixed = regexp_replace(column, "T", " ")
        return to_timestamp(column_fixed, 'yyyy-MM-dd HH:mm:ss')

    def __remove_jump_line(column):
        return regexp_replace(column, "\\n", "")

    def __make_trans(self):
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

    def __clean_str_column(self, c):
        normalized_column = translate(regexp_replace(c, "\p{M}", ""), self.matching_string, self.replace_string)
        return upper(normalized_column)

    # verifies if dummy column exists in the row recursos
    def __has_att(original_name,atts_recursos):
        recursos = atts_recursos.replace('[', '')
        recursos = recursos.replace(']', '')

        # string representation of dicts to actual list of dicts
        recursos_dict = ast.literal_eval(recursos)

        for recurso_dict in recursos_dict:
            for _, recurso_desc in recurso_dict.items():
                if original_name == recurso_desc:
                    return 1

        return 0

    def run(self, default_dataframe=None,last_file=None) -> None:
        try:
            if last_file:
                data = self.spark.read.csv(self.files_path + last_file + ".csv", header=True)
            else:
                data = self.spark.createDataFrame(default_dataframe)
            
            for original_name, column_name in self.dummy_columns:
                data = data.withColumn(column_name, self.__has_att(original_name, data.RECURSOS))

            # drop atributos and optionals column
            data_to_type_compute = data.drop("RECURSOS")

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
        except:
            self.spark.stop()

    def __load_data(self,data):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            p.execute_batch(curr, self.__get_exchange_insert_query(), data)

    def __get_exchange_insert_query() -> str:
        return '''
        INSERT INTO STG.AUTOLINE (
            AD_ID,
            INFORMACOES_ADICIONAIS,
            CORPO_VEICULO,
            ANO_FABRICACAO,
            CIDADE,
            COR,
            DATA_ATUALIZACAO_AUTOLINE,
            DATA_CRIACAO_AD,
            QNTD_PORTAS,
            EMAIL,
            MOTOR,
            COMBUSTIVEL,
            BLINDADO,
            COLECIONADOR,
            ADAPTADO_DEFICIENCIA,
            FINANCIAVEL,
            FINANCIADO,
            GARANTIA_DE_FABRICA,
            NOVO,
            DONO_UNICO,
            QUITADO,
            REGISTRAMENTO_PAGO,
            VENDEDOR_PJ,
            NAO_ACEITA_TROCA,
            IMPOSTOS_PAGOS,
            ZEROKM,
            KILOMETRAGEM,
            LINK_AD,
            FABRICANTE,
            CELULAR,
            MODELO,
            ANO_MODELO,
            BAIRRO,
            TELEFONE,
            PRECO,
            PRECO_FIPE,
            DATA_DE_REGISTRO,
            PLACA,
            COR_SECUNDARIA,
            TIPO_VEICULO,
            ENDERECO,
            COMPLEMENTO_ENDERECO,
            DOCUMENTO_VENDEDOR,
            NOME_VENDEDOR,
            UF,
            ESTADO,
            TRANSMISSAO,
            TIPO_VENDEDOR,
            DATA_ATT_AD,
            VERSAO,
            WHATSAPP,
            AR_CONDICIONADO,
            TETO_SOLAR,
            BANCO_DE_COURO,
            ALARME,
            FREIO_ABS,
            SENSOR_DE_ESTACIONAMENTO,
            COMPUTAR_DE_BORDO,
            AIRBAG,
            AR_QUENTE,
            RODAS_LIGA_LEVE,
            AIRBAG_DUPLO,
            VOLANTE_REG_ALTURA,
            FAROL_DE_MILHA,
            BANCO_REGULA_ALTURA,
            MP3_CD_PLAYER,
            VIDROS_ELETRICOS,
            TRAVAS_ELETRICAS,
            DESEMBACADOR_TRASEIRO,
            DIR_HIDRAULICA,
            RETROVISOR_ELETRICO,
            LIMPADOR_TRASEIRO,
            ENCOSTO_CABECA_TRASEIRO,
            DIR_ELETRICA,
            RADIO,
            KIT_MULTIMIDIA,
            CONTROLE_TRACAO,
            CONTROLE_AUTOMATICO_VEL,
            GPS,
            CD_PLAYER,
            FAROL_NEBLINA,
            RETROVISOR_FOTOCROMICO,
            SENSOR_DE_CHUVA,
            TRACAO_QUATRO_POR_QUATRO,
            PILOTO_AUTOMATICO,
            PROTETOR_CACAMBA,
            CAPOTA_MARITIMA,
            DVD_PLAYER,
            FAROL_DE_XENONIO,
            BANCO_COM_AQUECIMENTO,
            RADIO_TOCAFITA,
            DISQUETEIRA,
            ESCAPAMENTO_ESPORTIVO,
            FREIO_ABS,
            DATA_CARGA
        )
        VALUES (
            %(AD_ID)s,
            %(INFORMACOES_ADICIONAIS)s,
            %(CORPO_VEICULO)s,
            %(ANO_FABRICACAO)s,
            %(CIDADE)s,
            %(COR)s,
            %(DATA_ATUALIZACAO_AUTOLINE)s,
            %(DATA_CRIACAO_AD)s,
            %(QNTD_PORTAS)s,
            %(EMAIL)s,
            %(MOTOR)s,
            %(COMBUSTIVEL)s,
            %(BLINDADO)s,
            %(COLECIONADOR)s,
            %(ADAPTADO_DEFICIENCIA)s,
            %(FINANCIAVEL)s,
            %(FINANCIADO)s,
            %(GARANTIA_DE_FABRICA)s,
            %(NOVO)s,
            %(DONO_UNICO)s,
            %(QUITADO)s,
            %(REGISTRAMENTO_PAGO)s,
            %(VENDEDOR_PJ)s,
            %(NAO_ACEITA_TROCA)s,
            %(IMPOSTOS_PAGOS)s,
            %(ZEROKM)s,
            %(KILOMETRAGEM)s,
            %(LINK_AD)s,
            %(FABRICANTE)s,
            %(CELULAR)s,
            %(MODELO)s,
            %(ANO_MODELO)s,
            %(BAIRRO)s,
            %(TELEFONE)s,
            %(PRECO)s,
            %(PRECO_FIPE)s,
            %(DATA_DE_REGISTRO)s,
            %(PLACA)s,
            %(COR_SECUNDARIA)s,
            %(TIPO_VEICULO)s,
            %(ENDERECO)s,
            %(COMPLEMENTO_ENDERECO)s,
            %(DOCUMENTO_VENDEDOR)s,
            %(NOME_VENDEDOR)s,
            %(UF)s,
            %(ESTADO)s,
            %(TRANSMISSAO)s,
            %(TIPO_VENDEDOR)s,
            %(DATA_ATT_AD)s,
            %(VERSAO)s,
            %(WHATSAPP)s,
            %(AR_CONDICIONADO)s,
            %(TETO_SOLAR)s,
            %(BANCO_DE_COURO)s,
            %(ALARME)s,
            %(FREIO_ABS)s,
            %(SENSOR_DE_ESTACIONAMENTO)s,
            %(COMPUTAR_DE_BORDO)s,
            %(AIRBAG)s,
            %(AR_QUENTE)s,
            %(RODAS_LIGA_LEVE)s,
            %(AIRBAG_DUPLO)s,
            %(VOLANTE_REG_ALTURA)s,
            %(FAROL_DE_MILHA)s,
            %(BANCO_REGULA_ALTURA)s,
            %(MP3_CD_PLAYER)s,
            %(VIDROS_ELETRICOS)s,
            %(TRAVAS_ELETRICAS)s,
            %(DESEMBACADOR_TRASEIRO)s,
            %(DIR_HIDRAULICA)s,
            %(RETROVISOR_ELETRICO)s,
            %(LIMPADOR_TRASEIRO)s,
            %(ENCOSTO_CABECA_TRASEIRO)s,
            %(DIR_ELETRICA)s,
            %(RADIO)s,
            %(KIT_MULTIMIDIA)s,
            %(CONTROLE_TRACAO)s,
            %(CONTROLE_AUTOMATICO_VEL)s,
            %(GPS)s,
            %(CD_PLAYER)s,
            %(FAROL_NEBLINA)s,
            %(RETROVISOR_FOTOCROMICO)s,
            %(SENSOR_DE_CHUVA)s,
            %(TRACAO_QUATRO_POR_QUATRO)s,
            %(PILOTO_AUTOMATICO)s,
            %(PROTETOR_CACAMBA)s,
            %(CAPOTA_MARITIMA)s,
            %(DVD_PLAYER)s,
            %(FAROL_DE_XENONIO)s,
            %(BANCO_COM_AQUECIMENTO)s,
            %(RADIO_TOCAFITA)s,
            %(DISQUETEIRA)s,
            %(ESCAPAMENTO_ESPORTIVO)s,
            %(FREIO_ABS)s,
            %(DATA_CARGA)s
        ) ON CONFLICT DO NOTHING;
        '''
