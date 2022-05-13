DROP TABLE IF EXISTS STG.AUTOLINE;
DROP TABLE IF EXISTS STG.WEBMOTORS;
DROP SCHEMA IF EXISTS STG;
CREATE SCHEMA STG;
CREATE TABLE STG.AUTOLINE (
    COD SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    INFORMACOES_ADICIONAIS VARCHAR(3000), /* -> TITULO */
    CORPO_VEICULO VARCHAR(100),
    ANO_FABRICACAO VARCHAR(5),
    CIDADE VARCHAR(100), /* -> CIDADE_VENDEDOR */
    COR VARCHAR(50),
    DATA_CRIACAO_AD DATE,
    QNTD_PORTAS VARCHAR(20),
    EMAIL VARCHAR(100),
    MOTOR VARCHAR(100),
    COMBUSTIVEL VARCHAR(100),
    BLINDADO INT,
    COLECIONADOR INT,
    ADAPTADO_DEFICIENCIA INT,
    FINANCIAVEL INT,
    FINANCIADO INT,
    GARANTIA_DE_FABRICA INT,
    DONO_UNICO INT, /* -> UNICO_DONO */
    QUITADO INT,
    REGISTRAMENTO_PAGO INT,
    VENDEDOR_PJ INT,
    ACEITA_TROCA INT,
    IMPOSTOS_PAGOS INT,
    KILOMETRAGEM NUMERIC,
    LINK_AD VARCHAR(100),
    FABRICANTE VARCHAR(100),
    CELULAR VARCHAR(20),
    MODELO VARCHAR(100),
    ANO_MODELO VARCHAR(5),
    BAIRRO VARCHAR(150),
    TELEFONE VARCHAR(20),
    PRECO NUMERIC,
    PRECO_FIPE NUMERIC,
    PLACA VARCHAR(10),
    COR_SECUNDARIA VARCHAR(50),
    TIPO_VEICULO VARCHAR(100),
    ENDERECO VARCHAR(150),
    COMPLEMENTO_ENDERECO VARCHAR(150),
    DOCUMENTO_VENDEDOR VARCHAR(50),
    NOME_VENDEDOR VARCHAR(100),
    UF VARCHAR(5), /* -> UF_VENDEDOR */
    ESTADO VARCHAR(50), /* -> ESTADO_VENDEDOR */
    TRANSMISSAO VARCHAR(100),
    TIPO_VENDEDOR VARCHAR(100),
    VERSAO VARCHAR(100),
    WHATSAPP VARCHAR(20),
    WEBSITE VARCHAR(100),
    AR_CONDICIONADO INT,
    TETO_SOLAR INT,
    BANCO_DE_COURO INT,
    ALARME INT,
    FREIO_ABS INT,
    SENSOR_DE_ESTACIONAMENTO INT,
    COMPUTAR_DE_BORDO INT,
    AIRBAG INT,
    AR_QUENTE INT,
    RODAS_LIGA_LEVE INT,
    AIRBAG_DUPLO INT,
    VOLANTE_REG_ALTURA INT,
    FAROL_DE_MILHA INT,
    BANCO_REGULA_ALTURA INT,
    MP3_CD_PLAYER INT,
    VIDROS_ELETRICOS INT,
    TRAVAS_ELETRICAS INT,
    DESEMBACADOR_TRASEIRO INT,
    DIR_HIDRAULICA INT,
    RETROVISOR_ELETRICO INT,
    LIMPADOR_TRASEIRO INT,
    ENCOSTO_CABECA_TRASEIRO INT,
    DIR_ELETRICA INT,
    RADIO INT,
    KIT_MULTIMIDIA INT,
    CONTROLE_TRACAO INT,
    CONTROLE_AUTOMATICO_VEL INT,
    GPS INT,
    CD_PLAYER INT,
    FAROL_NEBLINA INT,
    RETROVISOR_FOTOCROMICO INT,
    SENSOR_DE_CHUVA INT,
    TRACAO_QUATRO_POR_QUATRO INT,
    PILOTO_AUTOMATICO INT,
    PROTETOR_CACAMBA INT,
    CAPOTA_MARITIMA INT,
    DVD_PLAYER INT,
    FAROL_DE_XENONIO INT,
    BANCO_COM_AQUECIMENTO INT,
    RADIO_TOCAFITA INT,
    DISQUETEIRA INT,
    ESCAPAMENTO_ESPORTIVO INT,
    DATA_CARGA DATE
);
CREATE TABLE STG.WEBMOTORS (
    COD SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    TITULO VARCHAR(3000),
    FABRICANTE VARCHAR(100),
    MODELO VARCHAR(100),
    VERSAO VARCHAR(150),
    ANO_FABRICACAO VARCHAR(5),
    ANO_MODELO VARCHAR(5),
    KILOMETRAGEM NUMERIC,
    TRANSMISSAO VARCHAR(50),
    QNTD_PORTAS VARCHAR(50),
    CORPO_VEICULO VARCHAR(100),
    ACEITA_TROCA INT,
    ALIENADO INT,
    GARANTIA_DE_FABRICA INT,
    IPVA_PAGO INT,
    LICENCIADO INT,
    REVISOES_PELA_AGENDA_CARRO INT,
    REVISOES_PELA_CONCESSIONARIA INT,
    UNICO_DONO INT,
    BLINDADO INT,
    COR VARCHAR(50),
    TIPO_VENDEDOR VARCHAR(50),
    CIDADE_VENDEDOR VARCHAR(100),
    ESTADO_VENDEDOR VARCHAR(100),
    UF_VENDEDOR VARCHAR(5),
    TIPO_ANUNCIO VARCHAR(100),
    ENTREGA_CARRO INT,
    TROCA_COM_TROCO INT,
    PRECO NUMERIC,
    PRECO_DESEJADO NUMERIC,
    COMENTARIO_DONO VARCHAR(1000),
    WEBSITE VARCHAR(100),
    PORCENTAGEM_FIPE NUMERIC,
    AIRBAG INT,
    ALARME INT,
    AR_CONDICIONADO INT,
    AR_QUENTE INT,
    BANCO_REGULA_ALTURA INT,
    BANCO_COM_AQUECIMENTO INT,
    BANCO_DE_COURO INT,
    CAPOTA_MARITIMA INT,
    MP3_CD_PLAYER INT,
    CD_PLAYER INT,
    COMPUTAR_DE_BORDO INT,
    CONTROLE_AUTOMATICO_VEL INT,
    CONTROLE_TRACAO INT,
    DESEMBACADOR_TRASEIRO INT,
    DIR_HIDRAULICA INT,
    DISQUETEIRA INT,
    DVD_PLAYER INT,
    ENCOSTO_CABECA_TRASEIRO INT,
    FAROL_DE_XENONIO INT,
    FREIO_ABS INT,
    GPS INT,
    LIMPADOR_TRASEIRO INT,
    PROTETOR_CACAMBA INT,
    RADIO INT,
    RADIO_TOCAFITA INT,
    RETROVISOR_FOTOCROMICO INT,
    RETROVISOR_ELETRICO INT,
    RODAS_LIGA_LEVE INT,
    SENSOR_DE_CHUVA INT,
    SENSOR_DE_ESTACIONAMENTO INT,
    TETO_SOLAR INT,
    TRACAO_QUATRO_POR_QUATRO INT,
    TRAVAS_ELETRICAS INT,
    VIDROS_ELETRICOS INT,
    VOLANTE_REG_ALTURA INT,
    COMBUSTIVEL VARCHAR(50),
    DATA_CARGA DATE
);
DROP TABLE IF EXISTS WH.FACT_AD;
DROP TABLE IF EXISTS WH.DIM_AD;
DROP TABLE IF EXISTS WH.DIM_CARS;
DROP TABLE IF EXISTS WH.DIM_RESOURCES;
DROP SCHEMA IF EXISTS WH;
CREATE SCHEMA WH;
CREATE TABLE WH.DIM_RESOURCES(
    RESOURCE_KEY SERIAL PRIMARY KEY,
    FINANCIAVEL INT,
    FINANCIADO INT,
    IMPOSTOS_PAGOS INT,
    REGISTRAMENTO_PAGO INT,
    QUITADO INT,
    ALIENADO INT,
    REVISOES_PELA_AGENDA_CARRO INT,
    REVISOES_PELA_CONCESSIONARIA INT,
    UNICO_DONO INT,
    COLECIONADOR INT,
    ADAPTADO_DEFICIENCIA INT,
    BLINDADO INT,
    LICENCIADO INT,
    IPVA_PAGO INT,
    GARANTIA_DE_FABRICA INT,
    AIRBAG INT,
    AIRBAG_DUPLO INT,
    ALARME INT,
    AR_CONDICIONADO INT,
    AR_QUENTE INT,
    BANCO_REGULA_ALTURA INT,
    BANCO_COM_AQUECIMENTO INT,
    BANCO_DE_COURO INT,
    CAPOTA_MARITIMA INT,
    MP3_CD_PLAYER INT,
    CD_PLAYER INT,
    DISQUETEIRA INT,
    DVD_PLAYER INT,
    COMPUTAR_DE_BORDO INT,
    CONTROLE_AUTOMATICO_VEL INT,
    CONTROLE_TRACAO INT,
    DESEMBACADOR_TRASEIRO INT,
    DIR_HIDRAULICA INT,
    DIR_ELETRICA INT,
    ENCOSTO_CABECA_TRASEIRO INT,
    FAROL_DE_XENONIO INT,
    FAROL_DE_MILHA INT,
    FAROL_NEBLINA INT,
    FREIO_ABS INT,
    GPS INT,
    LIMPADOR_TRASEIRO INT,
    PROTETOR_CACAMBA INT,
    RADIO INT,
    RADIO_TOCAFITA INT,
    KIT_MULTIMIDIA INT,
    RETROVISOR_FOTOCROMICO INT,
    RETROVISOR_ELETRICO INT,
    RODAS_LIGA_LEVE INT,
    SENSOR_DE_CHUVA INT,
    SENSOR_DE_ESTACIONAMENTO INT,
    TETO_SOLAR INT,
    TRACAO_QUATRO_POR_QUATRO INT,
    TRAVAS_ELETRICAS INT,
    VIDROS_ELETRICOS INT,
    VOLANTE_REG_ALTURA INT,
    PILOTO_AUTOMATICO INT,
    ESCAPAMENTO_ESPORTIVO INT
);
CREATE TABLE WH.DIM_CARS (
    CAR_KEY SERIAL PRIMARY KEY,
    FABRICANTE VARCHAR(100),
    TIPO_VEICULO VARCHAR(100),
    MODELO VARCHAR(100),
    VERSAO VARCHAR(150),
    ANO_FABRICACAO VARCHAR(5),
    ANO_MODELO VARCHAR(5),
    TRANSMISSAO VARCHAR(50),
    QNTD_PORTAS VARCHAR(50),
    CORPO_VEICULO VARCHAR(100),
    COR VARCHAR(50),
    COR_SECUNDARIA VARCHAR(50),
    COMBUSTIVEL VARCHAR(50),
    MOTOR VARCHAR(100)
);
CREATE TABLE WH.DIM_AD (
    AD_KEY SERIAL PRIMARY KEY,
    TITULO VARCHAR(3000),
    TIPO_ANUNCIO VARCHAR(100),
    NOME_VENDEDOR VARCHAR(100),
    CIDADE_VENDEDOR VARCHAR(100),
    ESTADO_VENDEDOR VARCHAR(100),
    UF_VENDEDOR VARCHAR(5),
    BAIRRO VARCHAR(150),
    ENDERECO VARCHAR(150),
    COMPLEMENTO_ENDERECO VARCHAR(150),
    DOCUMENTO_VENDEDOR VARCHAR(50),
    EMAIL VARCHAR(100),
    CELULAR VARCHAR(20),
    TELEFONE VARCHAR(20),
    WHATSAPP VARCHAR(20),
    TIPO_VENDEDOR VARCHAR(50),
    VENDEDOR_PJ INT,
    ACEITA_TROCA INT,
    TROCA_COM_TROCO INT,
    ENTREGA_CARRO INT,
    COMENTARIO_DONO VARCHAR(1000),
    PLACA VARCHAR(10),
    WEBSITE VARCHAR(100),
    LINK_AD VARCHAR(100),
    DATA_CRIACAO_AD DATE
);
CREATE TABLE WH.FACT_AD (
    ROW_ID SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    AD_KEY INT,
    CAR_KEY INT,
    RESOURCES_KEY INT,
    KILOMETRAGEM NUMERIC,
    PRECO NUMERIC,
    PRECO_DESEJADO NUMERIC,
    FIPE NUMERIC
);