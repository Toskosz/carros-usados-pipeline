from datetime import datetime
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
import psycopg2.extras as p
import numpy as np

class WebmotorsTransform:

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
            "ANO_FABRICACAO": [],
            "ANO_MODELO": [self.__to_str],
            "KILOMETRAGEM":[self.__to_float],
            "TRANSMISSAO": [self.__clean_str_column],
            "QNTD_PORTAS": [],
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

    def __create_dummy_columns(self, row):
        for original_name, column_name in self.dummy_columns.items():
            if original_name in row["ATRIBUTOS"] or original_name in row['OPTIONALS']:
                row[column_name] = True
            else:
                row[column_name] = False
        return row

    def __properly_fill_na(self, df):
        for col in df:
            datatype = df[col].dtype 
            
            if datatype == int or datatype == float:
                df[col].fillna(0, inplace=True)
            elif datatype == str:
                df[col].fillna("INDISPONIVEL", inplace=True)
            else:
                df[col].fillna(False, inplace=True)
        
        return df

    def run(self, default_dataframe) -> None:
        try:
            df_with_dummys = default_dataframe.apply(self.__create_dummy_columns, axis=1).drop(['ATRIBUTOS', 'OPTIONALS'], 1)

            # separation of UF and ESTADO from ESTADO column
            df_with_dummys['UF_VENDEDOR'] = df_with_dummys["ESTADO_VENDEDOR"].apply(lambda st: st[st.find("(")+1:st.find(")")])
            df_with_dummys['ESTADO_VENDEDOR'] = df_with_dummys["ESTADO_VENDEDOR"].apply(lambda st: st[:st.find("(")])
            
            for coluna, lst_f in self.columns_func_assigns.items():
                for f in lst_f:
                    df_with_dummys[coluna] = f(df_with_dummys[coluna])

            df_with_dummys['DATA_CARGA'] = datetime.now()
            df_with_dummys['WEBSITE'] = "WEBMOTORS"

            data_to_load = self.__properly_fill_na(df_with_dummys)

            print("[LOG] Finished transformations")

            self.__load_data(data_to_load)
            print("[LOG] Finished load to DB")

        except Exception as E:
            raise(E)

    def __to_str(self, column):
        return column.astype(str)

    def __compute_bool(self, column):
        return np.where(column=="true", True, False)

    def __to_float(self, column):
        return column.astype(float)

    def __clean_str_column(self, column):
        return column.str.replace('[^\w\s]', '').upper()
        # return column.map(lambda x: re.sub(r'\W+', '', x)).str.upper()

    def __compute_BLINDADO(self, column):
        return np.where(column=="S", True, False)

    def __load_data(self,data):
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            curr.execute('truncate table stg.webmotors')
            p.execute_batch(curr, self.__get_exchange_insert_query(data,"stg.webmotors"), data.values)
            curr.execute(open("src/sql_scripts/webmotors_to_star_schema.sql", "r").read())

    def __get_exchange_insert_query(self,df,table) -> str:
        df_columns = list(df)
        columns = ", ".join(df_columns)

        values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        
        return insert_stmt