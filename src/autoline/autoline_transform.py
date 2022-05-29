from xml.etree.ElementInclude import include
from util.creds import get_warehouse_creds
from util.warehouse import WarehouseConnection
from datetime import datetime
import psycopg2.extras as p
import numpy as np

class AutolineTransform:

    def __init__(self) -> None:
        self.dummy_columns = {
            'Ar Condicionado':'AR_CONDICIONADO',
            'Teto Solar':'TETO_SOLAR',
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
            'Escapamento Esportivo':'ESCAPAMENTO_ESPORTIVO',
        }

        self.columns_func_assigns = {
            "INFORMACOES_ADICIONAIS": [self.__remove_jump_line],
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
            "PRECO_FIPE": [self.__fix_empty_string, self.__to_float],
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
            "PLACA": [self.__fix_empty_string],
            "MOTOR": [self.__clean_str_column, self.__fix_empty_string]
        }

    def __create_dummy_columns(self, row):
        for original_name, column_name in self.dummy_columns.items():
            if original_name in row["RECURSOS"]:
                row[column_name] = True
            else:
                row[column_name] = False
        return row

    def __properly_fill_na(self, df):

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

    def run(self, default_dataframe) -> None:
        df_with_dummys = default_dataframe.apply(self.__create_dummy_columns, axis=1).drop('RECURSOS', axis=1)

        for coluna, lst_f in self.columns_func_assigns.items():
            for f in lst_f:
                df_with_dummys[coluna] = f(df_with_dummys[coluna])

        df_with_dummys['DATA_CARGA'] = datetime.now()
        df_with_dummys['WEBSITE'] = "AUTOLINE"

        data_to_load = self.__properly_fill_na(df_with_dummys)

        print("[LOG] Finished transformations")

        self.__load_data(data_to_load)
        print("[LOG] Finished load to DB")

    def __to_number(self, column):
        return column.map({'ZERO': '0', 'UM': '1', 'DOIS': '2',
         'TRES': '3', 'QUATRO': '4', 'CINCO': '5', 'SEIS': '6', 
         'SETE': '7', 'OITO': '8', 'NOVE': '9'}, na_action='ignore')
        
    def __to_float(self, column):
        return column.astype(float)

    def __to_str(self, column):
        return column.astype(str)

    def __compute_bool(self, column):
        return np.where(column=='VERDADEIRO', True, False)
        
    def __fix_empty_string(self, column):
        return np.where(column=="", None, column)
        
    def __compute_inverse_bool(self, column):
        return np.where(column=='VERDADEIRO', False, True)
        
    def __remove_jump_line(self, column):
        return column.replace(to_replace=r"\\n", value="", regex=True)

    def __clean_str_column(self, column):
        return column.str.replace('[^\w\s]', '', regex=True).str.upper()
        # return column.map(lambda x: re.sub(r'\W+', '', x)).str.upper()

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