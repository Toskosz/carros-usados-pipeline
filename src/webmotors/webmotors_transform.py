from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, udf, translate, upper, substring_index, when, col, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType, FloatType, IntegerType
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p
import unicodedata
import sys

class WebmotorsTransform:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("webmotors transformation").getOrCreate()
        self.matching_string, self.replace_string = self.__make_trans()

        self.schema = StructType([
            StructField("AD_ID", StringType(), True),                
            StructField("TITULO", StringType(), True),               
            StructField("FABRICANTE", StringType(), True),           
            StructField("MODELO", StringType(), True),               
            StructField("VERSAO", StringType(), True),               
            StructField("ANO_FABRICACAO", StringType(), True),       
            StructField("ANO_MODELO", FloatType(), True),          
            StructField("KILOMETRAGEM", FloatType(), True),        
            StructField("TRANSMISSAO", StringType(), True),          
            StructField("QNTD_PORTAS", StringType(), True),          
            StructField("CORPO_VEICULO", StringType(), True),        
            StructField("ATRIBUTOS", StringType(), True),            
            StructField("BLINDADO", StringType(), True),             
            StructField("COR", StringType(), True),                  
            StructField("TIPO_VENDEDOR", StringType(), True),        
            StructField("CIDADE_VENDEDOR", StringType(), True),      
            StructField("ESTADO_VENDEDOR", StringType(), True),      
            StructField("TIPO_ANUNCIO", StringType(), True),         
            StructField("ENTREGA_CARRO", StringType(), True),        
            StructField("TROCA_COM_TROCO", StringType(), True),      
            StructField("PRECO", FloatType(), True),               
            StructField("PRECO_DESEJADO", FloatType(), True),      
            StructField("COMENTARIO_DONO", StringType(), True),      
            StructField("PORCENTAGEM_FIPE", StringType(), True),     
            StructField("OPTIONALS", StringType(), True),            
            StructField("COMBUSTIVEL", StringType(), True)
        ])

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

    def run(self, default_dataframe) -> None:
        try:
            data = self.spark.createDataFrame(default_dataframe, schema=self.schema)

            print("[LOG] Dataframe criado")

            for original_name, column_name in self.dummy_columns.items():
                data = data.withColumn(column_name, when((col("ATRIBUTOS").contains(original_name)), '1').when((col("OPTIONALS").contains(original_name)), '1').otherwise('0'))

            # drop atributos and optionals column
            data_with_dummy_columns = data.drop("ATRIBUTOS","OPTIONALS")

            print("[LOG] Colunas desnecessárias dropadas")

            # separation of UF and ESTADO from ESTADO column
            data_with_uf = data_with_dummy_columns.withColumn("UF_VENDEDOR", self.__compute_UF(data_with_dummy_columns.ESTADO_VENDEDOR))
            data_to_type_compute = data_with_uf.withColumn("ESTADO_VENDEDOR", self.__compute_ESTADO(data_with_uf.ESTADO_VENDEDOR))
            
            # types, string cleaning, computes special columns
            for coluna, lst_f in self.columns_func_assigns.items():
                for f in lst_f:
                    data_to_type_compute = data_to_type_compute.withColumn(coluna, f(col(coluna)))

            # creates DATA_CARGA column with datetime of load
            tmp_data = data_to_type_compute.withColumn("DATA_CARGA", current_timestamp())
            data_to_load = tmp_data.withColumn("WEBSITE", lit("WEBMOTORS"))

            print("[LOG] Transformações feitas")

            # Uses pandas dataframe to make the load because i cant do it with pyspark at the moment
            # todo: load with pyspark dataframe
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

    def __to_str(self, column):
        int_column = column.cast(IntegerType())
        return int_column.cast(StringType())

    def __compute_bool(self, column):
        return when(column.contains("true"), '1').otherwise('0')

    def __to_float(self, column):
        return column.cast(FloatType())

    # removes special characters and uppercase it
    def __clean_str_column(self, column):
        normalized_column = translate(regexp_replace(column, "\p{M}", ""), self.matching_string, self.replace_string)
        return upper(normalized_column)

    # computes column BLINDADO
    def __compute_BLINDADO(self, column):
        return when(column.contains("S"), '1').otherwise('0')

    # separates the ESTADO_VENDEDOR into two colums, ESTADO_VENDEDOR and UF_VENDEDOR
    def __compute_UF(self, estados):
        estados_tmp = substring_index(estados, '(', -1) # DF)
        estados_tmp = substring_index(estados_tmp, ')', 1) # DF
        return estados_tmp

    # computes new estado values without uf
    def __compute_ESTADO(self, estados):
        estados_tmp = substring_index(estados, '(', 1)
        return estados_tmp

    def __load_data(self,data):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            curr.execute('truncate table stg.webmotors')
            p.execute_batch(curr, self.__get_exchange_insert_query(data,"stg.webmotors"), data.values)

    def __get_exchange_insert_query(self,df,table) -> str:
        df_columns = list(df)
        columns = ", ".join(df_columns)

        values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        
        return insert_stmt