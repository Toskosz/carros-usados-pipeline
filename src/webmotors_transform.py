from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
import datetime
import ast

class WebmotorsTransform:

    def __init__(self) -> None:
        self.files_path = "raw/webmotors/"
        self.spark = SparkSession.builder.appName("webmotors transformation").getOrCreate()

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

    def run(self) -> None:
        last_file = self.__get_last_file()
        data = self.spark.read.csv(self.files_path + last_file + ".csv", header=True)
        
        # todo: maybe the below process is shitty. investigate later.
        # adds and fills dummy columns to data
        for original_name, column_name in self.dummy_columns:
            data = data.withColumn(column_name, self.__has_att(original_name, data.ATRIBUTOS, data.OPTIONALS))

        # drop atributos and optionals column
        data_with_dummy_columns = data.drop("ATRIBUTOS","OPTIONALS")

        # todo: separation of UF and ESTADO from ESTADO column
        # todo: compute types
        # todo: clean strings
        # todo: compute blindado
        # todo: compute bool
        
    # get most recent csv data file
    def __get_last_file(self):
        # get all csv files
        files = {f.removesuffix(".csv") : datetime.strptime(f.removesuffix(".csv"), '%Y%m%d%H') for f in listdir(self.files_path) if isfile(join(self.files_path, f))}
        # latest file
        return max(files, key=files.get)

    # verifies if dummy column existis in the row atributos and optionals
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