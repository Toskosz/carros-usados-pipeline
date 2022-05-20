from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, to_timestamp, udf, translate, upper, when, col, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType, FloatType
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p
import unicodedata
import sys
import numpy as np
from sqlalchemy import column, create_engine, table

class AutolineTransform:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("autoline transformation").getOrCreate()
        self.matching_string, self.replace_string = self.__make_trans()
        
        self.schema = StructType([
            StructField("AD_ID", StringType(), True),
            StructField("INFORMACOES_ADICIONAIS", StringType(), True),
            StructField("CORPO_VEICULO", StringType(), True),
            StructField("ANO_FABRICACAO", StringType(), True),
            StructField("CIDADE", StringType(), True),
            StructField("COR", StringType(), True),
            StructField("QNTD_PORTAS", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("MOTOR", StringType(), True),
            StructField("RECURSOS", StringType(), True),
            StructField("COMBUSTIVEL", StringType(), True),
            StructField("BLINDADO", StringType(), True),
            StructField("COLECIONADOR", StringType(), True),
            StructField("ADAPTADO_DEFICIENCIA", StringType(), True),
            StructField("FINANCIAVEL", StringType(), True),
            StructField("FINANCIADO", StringType(), True),
            StructField("GARANTIA_DE_FABRICA", StringType(), True),
            StructField("DONO_UNICO", StringType(), True),
            StructField("QUITADO", StringType(), True),
            StructField("REGISTRAMENTO_PAGO", StringType(), True),
            StructField("VENDEDOR_PJ", StringType(), True),
            StructField("ACEITA_TROCA", StringType(), True),
            StructField("IMPOSTOS_PAGOS", StringType(), True),
            StructField("KILOMETRAGEM", StringType(), True),
            StructField("LINK_AD", StringType(), True),
            StructField("FABRICANTE", StringType(), True),
            StructField("CELULAR", StringType(), True),
            StructField("MODELO", StringType(), True),
            StructField("ANO_MODELO", StringType(), True),
            StructField("BAIRRO", StringType(), True),
            StructField("TELEFONE", StringType(), True),
            StructField("PRECO", FloatType(), True),
            StructField("PRECO_FIPE", StringType(), True),
            StructField("PLACA", StringType(), True),
            StructField("COR_SECUNDARIA", StringType(), True),
            StructField("TIPO_VEICULO", StringType(), True),
            StructField("ENDERECO", StringType(), True),
            StructField("COMPLEMENTO_ENDERECO", StringType(), True),
            StructField("DOCUMENTO_VENDEDOR", StringType(), True),
            StructField("NOME_VENDEDOR", StringType(), True),
            StructField("UF", StringType(), True),
            StructField("ESTADO", StringType(), True),
            StructField("TRANSMISSAO", StringType(), True),
            StructField("TIPO_VENDEDOR", StringType(), True),
            StructField("VERSAO", StringType(), True),
            StructField("WHATSAPP", StringType(), True)
        ])

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
            "INFORMACOES_ADICIONAIS": [self.__remove_jump_line],    #self.__clean_str_column, 
            "CORPO_VEICULO": [self.__clean_str_column],
            "ANO_FABRICACAO": [self.__to_str],
            "CIDADE": [self.__clean_str_column],
            "COR": [self.__clean_str_column],
            "QNTD_PORTAS": [self.__clean_str_column, self.__to_number],
            "COMBUSTIVEL": [self.__clean_str_column],
            "BLINDADO": [self.__compute_bool],
            "COLECIONADOR": [self.__compute_bool],
            "ADAPTADO_DEFICIENCIA": [self.__compute_bool],
            "FINANCIAVEL": [self.__compute_bool],
            "FINANCIADO": [self.__compute_bool],
            "GARANTIA_DE_FABRICA": [self.__compute_bool],
            "DONO_UNICO": [self.__compute_bool],
            "QUITADO": [self.__compute_bool],
            "REGISTRAMENTO_PAGO": [self.__compute_bool],
            "VENDEDOR_PJ": [self.__compute_bool],
            "ACEITA_TROCA": [self.__compute_inverse_bool],
            "IMPOSTOS_PAGOS": [self.__compute_bool],
            "KILOMETRAGEM": [self.__to_float],
            "FABRICANTE": [self.__clean_str_column],
            "MODELO": [self.__clean_str_column],
            "ANO_MODELO": [self.__to_str],
            "BAIRRO": [self.__clean_str_column],
            "PRECO": [self.__to_float],
            "PRECO_FIPE": [],
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
            "VERSAO": [self.__clean_str_column],
        }

    def run(self, default_dataframe) -> None:
        try:
            data = self.spark.createDataFrame(default_dataframe, schema=self.schema)
            
            print("[LOG] Dataframe criado")

            for original_name, column_name in self.dummy_columns.items():
                data = data.withColumn(column_name, when((col("RECURSOS").contains(original_name)), '1').otherwise('0'))

            # drop atributos and optionals column
            data_to_type_compute = data.drop("RECURSOS")

            print("[LOG] Colunas desnecessárias dropadas")

            # types, string cleaning, computes special columns
            for coluna, lst_f in self.columns_func_assigns.items():
                for f in lst_f:
                    data_to_type_compute = data_to_type_compute.withColumn(coluna, f(col(coluna)))

            # fills na values and creates DATA_CARGA column with datetime of load
            tmp_data = data_to_type_compute.withColumn("DATA_CARGA", current_timestamp())
            data_with_na = tmp_data.withColumn("WEBSITE", lit("AUTOLINE"))

            print("[LOG] Transformações feitas")

            data_to_load = data_with_na.na.fill("INDISPONIVEL").na.fill(False).na.fill(0.0)
            
            # Uses pandas dataframe to make the load because i cant do it with pyspark at the moment
            pandas_dataframe = data_to_load.toPandas()
            print("[LOG] Conversão para pandas DataFrame")
            # pandas_dataframe.to_csv("teste.csv")

            self.__load_data(pandas_dataframe)
            print("[LOG] Carga concluída")

            self.spark.stop()
        except Exception as E:
            print("[ERRO] O seguinte erro interrompeu o processo:")
            self.spark.stop()
            raise(E)

    def __to_number(self, column):
        return when(column.contains("ZERO"), "0").when(column.contains("UM"), "1")\
            .when(column.contains("DOIS"), "2").when(column.contains("TRES"), "3")\
            .when(column.contains("QUATRO"), "4").when(column.contains("CINCO"), "5")\
            .when(column.contains("SEIS"), "6").when(column.contains("SETE"), "7")\
            .when(column.contains("OITO"), "8").when(column.contains("NOVE"), "9").otherwise("0")

    def __to_float(self, column):
        return column.cast(FloatType())

    def __to_str(self, column):
        return column.cast(StringType())

    def __compute_bool(self, column):
        return when(column.contains("VERDADEIRO"), '1').otherwise('0')
        
    def __compute_inverse_bool(self, column):
        return when(column.contains("VERDADEIRO"), '0').otherwise('1')

    def __fix_date_type(self, column):
        column_fixed = regexp_replace(column, "T", " ")
        column_with_nat = to_timestamp(column_fixed, 'yyyy-MM-dd HH:mm:ss')
        column_result = regexp_replace(column_with_nat, "NaT", "NULL")
        return column_result

    def __remove_jump_line(self, column):
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

    def __load_data(self,data):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            curr.execute('truncate table stg.autoline')
            p.execute_batch(curr, self.__get_exchange_insert_query(data,"stg.autoline"), data.values)
            curr.execute(open("src/sql_scripts/autoline_to_star_schema.sql", "r").read())

    def __get_exchange_insert_query(self,df,table) -> str:
        df_columns = list(df)
        columns = ", ".join(df_columns)

        values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        
        return insert_stmt