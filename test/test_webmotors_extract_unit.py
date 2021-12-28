import unittest
from src.webmotors_etl import WebmotorsExtract
 
# comando de rodar testes
# python -m unittest discover <test_directory>

class TestWebMotorsExtractor(unittest.TestCase):

    expected_columns = ['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
        'ATRIBUTOS','BLINDADO','COR','TIPO_VENDEDOR','CIDADE_VENDEDOR','ESTADO_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO',
        'PORCENTAGEM_FIPE','OPTIONALS','COMBUSTIVEL']

    def test_single_row_extraction(self):
        batch_size = 1
        extractor  = WebmotorsExtract()
        
        cars_extracted = extractor.run(batch_size)

        # Todas as colunas
        self.assertCountEqual(self.expected_columns, cars_extracted.columns)

        # Numero de linhas
        self.assertEqual(len(cars_extracted.index), batch_size)

        # Coluna AD_ID não pode ter nulos
        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        # não pode ter linhas AD_ID repetidas
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

    def test_single_page_extraction(self):
        batch_size = 24
        extractor  = WebmotorsExtract()
        
        cars_extracted = extractor.run(batch_size)

        # Todas as colunas
        self.assertCountEqual(self.expected_columns, cars_extracted.columns)

        # Numero de linhas
        self.assertEqual(len(cars_extracted.index), batch_size)

        # Coluna AD_ID não pode ter nulos
        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        # não pode ter linhas AD_ID repetidas
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

    def test_multiple_page_extraction(self):
        batch_size = 72
        extractor  = WebmotorsExtract()
        
        cars_extracted = extractor.run(batch_size)

        # Todas as colunas
        self.assertCountEqual(self.expected_columns, cars_extracted.columns)

        # Numero de linhas
        self.assertEqual(len(cars_extracted.index), batch_size)

        # Coluna AD_ID não pode ter nulos
        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        # não pode ter linhas AD_ID repetidas
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

if __name__ == '__main__':
    unittest.main()