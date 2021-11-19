from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
        # todo: add dummy columns to data
        # todo: fill dummy columns (vehicles_att and optionals)
        rdd_with_va = data.rdd.map(lambda x: self.__get_vehicle_attributes(x))
        df_with_va = rdd_with_va.toDF(['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
            'ACEITA_TROCA','ALIENADO','GARANTIA_DE_FABRICA','IPVA_PAGO','LICENCIADO','REVISOES_PELA_AGENDA_CARRO','REVISOES_PELA_CONCESSIONARIA','UNICO_DONO','BLINDADO','COR','TIPO_VENDEDOR',
            'CIDADE_VENDEDOR','ESTADO_VENDEDOR','UF_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO','PORCENTAGEM_FIPE','AIRBAG',
            'ALARME','AR_CONDICIONADO','AR_QUENTE','BANCO_REGULA_ALTURA','BANCO_COM_AQUECIMENTO','BANCO_DE_COURO','CAPOTA_MARITIMA','MP3_CD_PLAYER','CD_PLAYER','COMPUTAR_DE_BORDO',
            'CONTROLE_AUTOMATICO_VEL','CONTROLE_TRACAO','DESEMBACADOR_TRASEIRO','DIR_HIDRAULICA','DISQUETEIRA','DVD_PLAYER','ENCOSTO_CABECA_TRASEIRO','FAROL_DE_XENONIO','FREIO_ABS',
            'GPS','LIMPADOR_TRASEIRO','PROTETOR_CACAMBA','RADIO','RADIO_TOCAFICA','RETROVISOR_FOTOCROMICO','RETROVISOR_ELETRICO','RODAS_LIGA_LEVE','SENSOR_DE_CHUVA',
            'SENSOR_DE_ESTACIONAMENTO','TETO_SOLAR','TRACAO_QUATRO_POR_QUATRO','TRAVAS_ELETRICAS','VIDROS_ELETRICOS','VOLANTE_REG_ALTURA','COMBUSTIVEL'])

    def __get_last_file(self):
        # get all csv files
        files = {f.removesuffix(".csv") : datetime.strptime(f.removesuffix(".csv"), '%Y%m%d%H') for f in listdir(self.files_path) if isfile(join(self.files_path, f))}
        # latest file
        return max(files, key=files.get)

    def __get_vehicle_attributes(self, row):
        row_val = row.ATRIBUTOS.replace('[', '')
        row_val = row_val.replace(']', '')
        row_dict = ast.literal_eval(row_val)

        for att_dict in row_dict:
            for _, attribute_desc in att_dict.items():
                column_name = self.dummy_columns[attribute_desc]
                # 1 = vehicle has attribute
                row.withColumn(column_name,lit('1')).show()
        
        # todo: verify if "return row" is possible
        # todo: verify if this function follow the functional programming paradigm 
        return row