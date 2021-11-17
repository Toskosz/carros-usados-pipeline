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

if 'VehicleAttributes' in specs.keys():
    atributos = specs['VehicleAttributes']
    for atributo in atributos:
        for _, atributo_desc in atributo.items():
            column_name = self.dummy_columns[atributo_desc]
            tmp_row[column_name] = 1

optionals = tmp_specs['Optionals']
for option in optionals:
    column_name = self.dummy_columns[option['Name']]
    tmp_row[column_name] = 1

tmp_row['UF_VENDEDOR'] = tmp_row['ESTADO_VENDEDOR'][tmp_row['ESTADO_VENDEDOR'].find("(")+1:tmp_row['ESTADO_VENDEDOR'].find(")")]
tmp_row['ESTADO_VENDEDOR'] = tmp_row['ESTADO_VENDEDOR'][:tmp_row['ESTADO_VENDEDOR'].find("(")]


for coluna, lst_f in self.columns_func_assigns.items():
    for f in lst_f:
        resulting_data[coluna] = f(resulting_data[coluna])

# if dummy columns = 0 zero then car doesnt have feature so "0"
resulting_data[self.dummy_columns] = resulting_data[self.dummy_columns].fillna(value=0)


resulting_data = resulting_data.fillna(value="INDISPONIVEL")
del resulting_data['OBSERVACOES']

self.columns_func_assigns = {
    "TITULO": [self.clean_str_column],
    "FABRICANTE": [self.clean_str_column],
    "MODELO": [self.clean_str_column],
    "VERSAO": [self.clean_str_column],
    "ANO_FABRICACAO": [self.to_str],
    "ANO_MODELO": [self.to_str],
    "KILOMETRAGEM":[self.to_float],
    "TRANSMISSAO": [self.clean_str_column],
    "QNTD_PORTAS": [self.to_str],
    "CORPO_VEICULO": [self.clean_str_column],
    "BLINDADO": [self.compute_blindado_webmotors],
    "COR": [self.clean_str_column],
    "TIPO_VENDEDOR": [self.clean_str_column],
    "CIDADE_VENDEDOR": [self.clean_str_column],
    "ESTADO_VENDEDOR": [self.clean_str_column],
    "UF_VENDEDOR": [self.clean_str_column],
    "TIPO_ANUNCIO": [self.clean_str_column],
    "ENTREGA_CARRO": [self.compute_bool],
    "TROCA_COM_TROCO": [self.compute_bool],
    "PRECO": [self.to_float],
    "PORCENTAGEM_FIPE": [self.to_float],
    "COMBUSTIVEL": [self.clean_str_column],
    "COMENTARIO_DONO": [self.clean_str_column]
}

def clean_str_column(column):
    # removes accents
    normalized_column = column.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    # return uppercase
    return normalized_column.str.upper()

def to_upper(column):
    # return uppercase
    return column.str.upper()

def to_str(column):
    return column.astype(str)

def compute_blindado_webmotors(column):
    return np.where(column == 'N', 0, 1)

def compute_bool(column):
    return np.where(column == 'true', 1, 0)

def to_float(column):
    return column.astype(float)

#   Loading part
    
def _get_exchange_insert_query(self) -> str:
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
        RADIO_TOCAFICA,
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
        %(RADIO_TOCAFICA)s,
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

carros_webmotors = pd.DataFrame(columns=['AD_ID','TITULO','FABRICANTE','MODELO','VERSAO','ANO_FABRICACAO','ANO_MODELO','KILOMETRAGEM','TRANSMISSAO','QNTD_PORTAS','CORPO_VEICULO',
    'ACEITA_TROCA','ALIENADO','GARANTIA_DE_FABRICA','IPVA_PAGO','LICENCIADO','REVISOES_PELA_AGENDA_CARRO','REVISOES_PELA_CONCESSIONARIA','UNICO_DONO','BLINDADO','COR','TIPO_VENDEDOR',
    'CIDADE_VENDEDOR','ESTADO_VENDEDOR','UF_VENDEDOR','AD_TYPE','SCORE_VENDEDOR','ENTREGA_CARRO','TROCA_COM_TROCO','PRECO','PRECO_DESEJADO','COMENTARIO_DONO','PORCENTAGEM_FIPE','AIRBAG',
    'ALARME','AR_CONDICIONADO','AR_QUENTE','BANCO_REGULA_ALTURA','BANCO_COM_AQUECIMENTO','BANCO_DE_COURO','CAPOTA_MARITIMA','MP3_CD_PLAYER','CD_PLAYER','COMPUTAR_DE_BORDO',
    'CONTROLE_AUTOMATICO_VEL','CONTROLE_TRACAO','DESEMBACADOR_TRASEIRO','DIR_HIDRAULICA','DISQUETEIRA','DVD_PLAYER','ENCOSTO_CABECA_TRASEIRO','FAROL_DE_XENONIO','FREIO_ABS',
    'GPS','LIMPADOR_TRASEIRO','PROTETOR_CACAMBA','RADIO','RADIO_TOCAFICA','RETROVISOR_FOTOCROMICO','RETROVISOR_ELETRICO','RODAS_LIGA_LEVE','SENSOR_DE_CHUVA',
    'SENSOR_DE_ESTACIONAMENTO','TETO_SOLAR','TRACAO_QUATRO_POR_QUATRO','TRAVAS_ELETRICAS','VIDROS_ELETRICOS','VOLANTE_REG_ALTURA','COMBUSTIVEL'])
