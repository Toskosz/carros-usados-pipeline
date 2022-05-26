import unittest
from src.autoline.autoline_extract import AutolineExtract
 
# comando de rodar testes
# python -m unittest discover <test_directory>

class TestWebMotorsExtractor(unittest.TestCase):

    expected_columns = ['AD_ID','INFORMACOES_ADICIONAIS','CORPO_VEICULO','ANO_FABRICACAO','CIDADE','COR','DATA_ATUALIZACAO_AUTOLINE','DATA_CRIACAO_AD',
        'QNTD_PORTAS','EMAIL','MOTOR','RECURSOS','COMBUSTIVEL','BLINDADO','COLECIONADOR','ADAPTADO_DEFICIENCIA','FINANCIAVEL','FINANCIADO','GARANTIA_DE_FABRICA','NOVO','DONO_UNICO',
        'QUITADO','REGISTRAMENTO_PAGO','VENDEDOR_PJ','NAO_ACEITA_TROCA','IMPOSTOS_PAGOS','ZEROKM','KILOMETRAGEM','LINK_AD','FABRICANTE','CELULAR','MODELO','ANO_MODELO','BAIRRO',
        'TELEFONE','PRECO','PRECO_FIPE','DATA_DE_REGISTRO','PLACA','COR_SECUNDARIA','TIPO_VEICULO','ENDERECO','COMPLEMENTO_ENDERECO','DOCUMENTO_VENDEDOR',
        'NOME_VENDEDOR','UF','ESTADO','TRANSMISSAO','TIPO_VENDEDOR','DATA_ATT_AD','VERSAO','WHATSAPP']

    def test_single_row_extraction(self):
        batch_size = 1
        extractor  = AutolineExtract()
        
        cars_extracted = extractor.run(batch_size)

        self.assertCountEqual(self.expected_columns, cars_extracted.columns)
        self.assertEqual(len(cars_extracted.index), batch_size)

        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

    def test_single_page_extraction(self):
        batch_size = 24
        extractor  = AutolineExtract()
        
        cars_extracted = extractor.run(batch_size)

        self.assertCountEqual(self.expected_columns, cars_extracted.columns)

        self.assertEqual(len(cars_extracted.index), batch_size)

        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

    def test_multiple_page_extraction(self):
        batch_size = 72
        extractor  = AutolineExtract()
        
        cars_extracted = extractor.run(batch_size)

        self.assertCountEqual(self.expected_columns, cars_extracted.columns)

        self.assertEqual(len(cars_extracted.index), batch_size)

        df_column_nan = cars_extracted.isnull().sum()
        self.assertEqual(df_column_nan['AD_ID'], 0)
        
        duplicate_rows = cars_extracted[cars_extracted.duplicated(subset='AD_ID',keep=False)]
        self.assertEqual(len(duplicate_rows.index), 0)

if __name__ == '__main__':
    unittest.main()