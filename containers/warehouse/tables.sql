DROP TABLE IF EXISTS STG.AUTOLINE;
DROP TABLE IF EXISTS STG.WEBMOTORS;
DROP SCHEMA IF EXISTS STG;
CREATE SCHEMA STG;
CREATE TABLE STG.AUTOLINE (
    COD SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    INFORMACOES_ADICIONAIS VARCHAR(3000), /* -> TITULO */
    CORPO_VEICULO VARCHAR(100),
    ANO_FABRICACAO VARCHAR(15),
    CIDADE VARCHAR(100), /* -> CIDADE_VENDEDOR */
    COR VARCHAR(50),
    QNTD_PORTAS VARCHAR(20),
    EMAIL VARCHAR(100),
    MOTOR VARCHAR(100),
    COMBUSTIVEL VARCHAR(100),
    BLINDADO BOOLEAN,
    COLECIONADOR BOOLEAN,
    ADAPTADO_DEFICIENCIA BOOLEAN,
    FINANCIAVEL BOOLEAN,
    FINANCIADO BOOLEAN,
    GARANTIA_DE_FABRICA BOOLEAN,
    DONO_UNICO BOOLEAN, /* -> UNICO_DONO */
    QUITADO BOOLEAN,
    REGISTRAMENTO_PAGO BOOLEAN,
    VENDEDOR_PJ BOOLEAN,
    ACEITA_TROCA BOOLEAN,
    IMPOSTOS_PAGOS BOOLEAN,
    KILOMETRAGEM NUMERIC,
    LINK_AD VARCHAR(100),
    FABRICANTE VARCHAR(100),
    CELULAR VARCHAR(20),
    MODELO VARCHAR(100),
    ANO_MODELO VARCHAR(15),
    BAIRRO VARCHAR(150),
    TELEFONE VARCHAR(20),
    PRECO NUMERIC,
    PRECO_FIPE NUMERIC,
    PLACA VARCHAR(15),
    COR_SECUNDARIA VARCHAR(50),
    TIPO_VEICULO VARCHAR(100),
    ENDERECO VARCHAR(150),
    COMPLEMENTO_ENDERECO VARCHAR(150),
    DOCUMENTO_VENDEDOR VARCHAR(50),
    NOME_VENDEDOR VARCHAR(100),
    UF VARCHAR(15), /* -> UF_VENDEDOR */
    ESTADO VARCHAR(50), /* -> ESTADO_VENDEDOR */
    TRANSMISSAO VARCHAR(100),
    TIPO_VENDEDOR VARCHAR(100),
    VERSAO VARCHAR(100),
    WHATSAPP VARCHAR(20),
    WEBSITE VARCHAR(100),
    AR_CONDICIONADO BOOLEAN,
    TETO_SOLAR BOOLEAN,
    BANCO_DE_COURO BOOLEAN,
    ALARME BOOLEAN,
    FREIO_ABS BOOLEAN,
    SENSOR_DE_ESTACIONAMENTO BOOLEAN,
    COMPUTAR_DE_BORDO BOOLEAN,
    AIRBAG BOOLEAN,
    AR_QUENTE BOOLEAN,
    RODAS_LIGA_LEVE BOOLEAN,
    AIRBAG_DUPLO BOOLEAN,
    VOLANTE_REG_ALTURA BOOLEAN,
    FAROL_DE_MILHA BOOLEAN,
    BANCO_REGULA_ALTURA BOOLEAN,
    MP3_CD_PLAYER BOOLEAN,
    VIDROS_ELETRICOS BOOLEAN,
    TRAVAS_ELETRICAS BOOLEAN,
    DESEMBACADOR_TRASEIRO BOOLEAN,
    DIR_HIDRAULICA BOOLEAN,
    RETROVISOR_ELETRICO BOOLEAN,
    LIMPADOR_TRASEIRO BOOLEAN,
    ENCOSTO_CABECA_TRASEIRO BOOLEAN,
    DIR_ELETRICA BOOLEAN,
    RADIO BOOLEAN,
    KIT_MULTIMIDIA BOOLEAN,
    CONTROLE_TRACAO BOOLEAN,
    CONTROLE_AUTOMATICO_VEL BOOLEAN,
    GPS BOOLEAN,
    CD_PLAYER BOOLEAN,
    FAROL_NEBLINA BOOLEAN,
    RETROVISOR_FOTOCROMICO BOOLEAN,
    SENSOR_DE_CHUVA BOOLEAN,
    TRACAO_QUATRO_POR_QUATRO BOOLEAN,
    PILOTO_AUTOMATICO BOOLEAN,
    PROTETOR_CACAMBA BOOLEAN,
    CAPOTA_MARITIMA BOOLEAN,
    DVD_PLAYER BOOLEAN,
    FAROL_DE_XENONIO BOOLEAN,
    BANCO_COM_AQUECIMENTO BOOLEAN,
    RADIO_TOCAFITA BOOLEAN,
    DISQUETEIRA BOOLEAN,
    ESCAPAMENTO_ESPORTIVO BOOLEAN,
    DATA_CARGA TIMESTAMP,
    AD_KEY INT,
    CAR_KEY INT,
    SELLER_KEY INT,
    LOCATION_KEY INT,
    RESOURCE_KEY INT
);
CREATE TABLE STG.WEBMOTORS (
    COD SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    TITULO VARCHAR(3000),
    FABRICANTE VARCHAR(100),
    MODELO VARCHAR(100),
    VERSAO VARCHAR(150),
    ANO_FABRICACAO VARCHAR(15),
    ANO_MODELO VARCHAR(15),
    KILOMETRAGEM NUMERIC,
    TRANSMISSAO VARCHAR(50),
    QNTD_PORTAS VARCHAR(50),
    CORPO_VEICULO VARCHAR(100),
    ACEITA_TROCA BOOLEAN,
    ALIENADO BOOLEAN,
    GARANTIA_DE_FABRICA BOOLEAN,
    IPVA_PAGO BOOLEAN,
    LICENCIADO BOOLEAN,
    REVISOES_PELA_AGENDA_CARRO BOOLEAN,
    REVISOES_PELA_CONCESSIONARIA BOOLEAN,
    UNICO_DONO BOOLEAN,
    BLINDADO BOOLEAN,
    COR VARCHAR(50),
    TIPO_VENDEDOR VARCHAR(50),
    CIDADE_VENDEDOR VARCHAR(100),
    ESTADO_VENDEDOR VARCHAR(100),
    UF_VENDEDOR VARCHAR(15),
    TIPO_ANUNCIO VARCHAR(100),
    ENTREGA_CARRO BOOLEAN,
    TROCA_COM_TROCO BOOLEAN,
    PRECO NUMERIC,
    PRECO_DESEJADO NUMERIC,
    COMENTARIO_DONO VARCHAR(5000),
    WEBSITE VARCHAR(100),
    PORCENTAGEM_FIPE NUMERIC,
    AIRBAG BOOLEAN,
    ALARME BOOLEAN,
    AR_CONDICIONADO BOOLEAN,
    AR_QUENTE BOOLEAN,
    BANCO_REGULA_ALTURA BOOLEAN,
    BANCO_COM_AQUECIMENTO BOOLEAN,
    BANCO_DE_COURO BOOLEAN,
    CAPOTA_MARITIMA BOOLEAN,
    MP3_CD_PLAYER BOOLEAN,
    CD_PLAYER BOOLEAN,
    COMPUTAR_DE_BORDO BOOLEAN,
    CONTROLE_AUTOMATICO_VEL BOOLEAN,
    CONTROLE_TRACAO BOOLEAN,
    DESEMBACADOR_TRASEIRO BOOLEAN,
    DIR_HIDRAULICA BOOLEAN,
    DISQUETEIRA BOOLEAN,
    DVD_PLAYER BOOLEAN,
    ENCOSTO_CABECA_TRASEIRO BOOLEAN,
    FAROL_DE_XENONIO BOOLEAN,
    FREIO_ABS BOOLEAN,
    GPS BOOLEAN,
    LIMPADOR_TRASEIRO BOOLEAN,
    PROTETOR_CACAMBA BOOLEAN,
    RADIO BOOLEAN,
    RADIO_TOCAFITA BOOLEAN,
    RETROVISOR_FOTOCROMICO BOOLEAN,
    RETROVISOR_ELETRICO BOOLEAN,
    RODAS_LIGA_LEVE BOOLEAN,
    SENSOR_DE_CHUVA BOOLEAN,
    SENSOR_DE_ESTACIONAMENTO BOOLEAN,
    TETO_SOLAR BOOLEAN,
    TRACAO_QUATRO_POR_QUATRO BOOLEAN,
    TRAVAS_ELETRICAS BOOLEAN,
    VIDROS_ELETRICOS BOOLEAN,
    VOLANTE_REG_ALTURA BOOLEAN,
    COMBUSTIVEL VARCHAR(50),
    DATA_CARGA TIMESTAMP,
    AD_KEY INT,
    CAR_KEY INT,
    SELLER_KEY INT,
    LOCATION_KEY INT,
    RESOURCE_KEY INT
);
DROP TABLE IF EXISTS WH.FACT_AD;
DROP TABLE IF EXISTS WH.DIM_AD;
DROP TABLE IF EXISTS WH.DIM_CARS;
DROP TABLE IF EXISTS WH.DIM_RESOURCES;
DROP TABLE IF EXISTS WH.DIM_SELLER;
DROP TABLE IF EXISTS WH.DIM_LOCATION;
DROP SCHEMA IF EXISTS WH;
CREATE SCHEMA WH;
CREATE TABLE WH.DIM_RESOURCE(
    RESOURCE_KEY SERIAL PRIMARY KEY,
    FINANCIAVEL BOOLEAN NOT NULL DEFAULT 'false',
    FINANCIADO BOOLEAN NOT NULL DEFAULT 'false',
    IMPOSTOS_PAGOS BOOLEAN NOT NULL DEFAULT 'false',
    REGISTRAMENTO_PAGO BOOLEAN NOT NULL DEFAULT 'false',
    QUITADO BOOLEAN NOT NULL DEFAULT 'false',
    ALIENADO BOOLEAN NOT NULL DEFAULT 'false',
    REVISOES_PELA_AGENDA_CARRO BOOLEAN NOT NULL DEFAULT 'false',
    REVISOES_PELA_CONCESSIONARIA BOOLEAN NOT NULL DEFAULT 'false',
    UNICO_DONO BOOLEAN NOT NULL DEFAULT 'false',
    COLECIONADOR BOOLEAN NOT NULL DEFAULT 'false',
    ADAPTADO_DEFICIENCIA BOOLEAN NOT NULL DEFAULT 'false',
    BLINDADO BOOLEAN NOT NULL DEFAULT 'false',
    LICENCIADO BOOLEAN NOT NULL DEFAULT 'false',
    IPVA_PAGO BOOLEAN NOT NULL DEFAULT 'false',
    GARANTIA_DE_FABRICA BOOLEAN NOT NULL DEFAULT 'false',
    AIRBAG BOOLEAN NOT NULL DEFAULT 'false',
    AIRBAG_DUPLO BOOLEAN NOT NULL DEFAULT 'false',
    ALARME BOOLEAN NOT NULL DEFAULT 'false',
    AR_CONDICIONADO BOOLEAN NOT NULL DEFAULT 'false',
    AR_QUENTE BOOLEAN NOT NULL DEFAULT 'false',
    BANCO_REGULA_ALTURA BOOLEAN NOT NULL DEFAULT 'false',
    BANCO_COM_AQUECIMENTO BOOLEAN NOT NULL DEFAULT 'false',
    BANCO_DE_COURO BOOLEAN NOT NULL DEFAULT 'false',
    CAPOTA_MARITIMA BOOLEAN NOT NULL DEFAULT 'false',
    MP3_CD_PLAYER BOOLEAN NOT NULL DEFAULT 'false',
    CD_PLAYER BOOLEAN NOT NULL DEFAULT 'false',
    DISQUETEIRA BOOLEAN NOT NULL DEFAULT 'false',
    DVD_PLAYER BOOLEAN NOT NULL DEFAULT 'false',
    COMPUTAR_DE_BORDO BOOLEAN NOT NULL DEFAULT 'false',
    CONTROLE_AUTOMATICO_VEL BOOLEAN NOT NULL DEFAULT 'false',
    CONTROLE_TRACAO BOOLEAN NOT NULL DEFAULT 'false',
    DESEMBACADOR_TRASEIRO BOOLEAN NOT NULL DEFAULT 'false',
    DIR_HIDRAULICA BOOLEAN NOT NULL DEFAULT 'false',
    DIR_ELETRICA BOOLEAN NOT NULL DEFAULT 'false',
    ENCOSTO_CABECA_TRASEIRO BOOLEAN NOT NULL DEFAULT 'false',
    FAROL_DE_XENONIO BOOLEAN NOT NULL DEFAULT 'false',
    FAROL_DE_MILHA BOOLEAN NOT NULL DEFAULT 'false',
    FAROL_NEBLINA BOOLEAN NOT NULL DEFAULT 'false',
    FREIO_ABS BOOLEAN NOT NULL DEFAULT 'false',
    GPS BOOLEAN NOT NULL DEFAULT 'false',
    LIMPADOR_TRASEIRO BOOLEAN NOT NULL DEFAULT 'false',
    PROTETOR_CACAMBA BOOLEAN NOT NULL DEFAULT 'false',
    RADIO BOOLEAN NOT NULL DEFAULT 'false',
    RADIO_TOCAFITA BOOLEAN NOT NULL DEFAULT 'false',
    KIT_MULTIMIDIA BOOLEAN NOT NULL DEFAULT 'false',
    RETROVISOR_FOTOCROMICO BOOLEAN NOT NULL DEFAULT 'false',
    RETROVISOR_ELETRICO BOOLEAN NOT NULL DEFAULT 'false',
    RODAS_LIGA_LEVE BOOLEAN NOT NULL DEFAULT 'false',
    SENSOR_DE_CHUVA BOOLEAN NOT NULL DEFAULT 'false',
    SENSOR_DE_ESTACIONAMENTO BOOLEAN NOT NULL DEFAULT 'false',
    TETO_SOLAR BOOLEAN NOT NULL DEFAULT 'false',
    TRACAO_QUATRO_POR_QUATRO BOOLEAN NOT NULL DEFAULT 'false',
    TRAVAS_ELETRICAS BOOLEAN NOT NULL DEFAULT 'false',
    VIDROS_ELETRICOS BOOLEAN NOT NULL DEFAULT 'false',
    VOLANTE_REG_ALTURA BOOLEAN NOT NULL DEFAULT 'false',
    PILOTO_AUTOMATICO BOOLEAN NOT NULL DEFAULT 'false',
    ESCAPAMENTO_ESPORTIVO BOOLEAN NOT NULL DEFAULT 'false'
);

CREATE TABLE WH.DIM_CAR (
    CAR_KEY SERIAL PRIMARY KEY,
    FABRICANTE VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    TIPO_VEICULO VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    MODELO VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    VERSAO VARCHAR(150) NOT NULL DEFAULT 'INDISPONIVEL',
    ANO_FABRICACAO VARCHAR(15) NOT NULL DEFAULT 'INDISPONIVEL',
    ANO_MODELO VARCHAR(15) NOT NULL DEFAULT 'INDISPONIVEL',
    TRANSMISSAO VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    QNTD_PORTAS VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    CORPO_VEICULO VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    COR VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    COR_SECUNDARIA VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    COMBUSTIVEL VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    MOTOR VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    PLACA VARCHAR(15) NOT NULL DEFAULT 'INDISPONIVEL'
);

CREATE TABLE WH.DIM_LOCATION(
    LOCATION_KEY SERIAL PRIMARY KEY,
    CIDADE VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    ESTADO VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    UF VARCHAR(15) NOT NULL DEFAULT 'INDISPONIVEL',
    BAIRRO VARCHAR(150) NOT NULL DEFAULT 'INDISPONIVEL',
    ENDERECO VARCHAR(150) NOT NULL DEFAULT 'INDISPONIVEL',
    COMPLEMENTO_ENDERECO VARCHAR(150) NOT NULL DEFAULT 'INDISPONIVEL'
);

CREATE TABLE WH.DIM_SELLER(
    SELLER_KEY SERIAL PRIMARY KEY,
    NOME_VENDEDOR VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    DOCUMENTO_VENDEDOR VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    EMAIL VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    CELULAR VARCHAR(20) NOT NULL DEFAULT 'INDISPONIVEL',
    TELEFONE VARCHAR(20) NOT NULL DEFAULT 'INDISPONIVEL',
    WHATSAPP VARCHAR(20) NOT NULL DEFAULT 'INDISPONIVEL',
    TIPO_VENDEDOR VARCHAR(50) NOT NULL DEFAULT 'INDISPONIVEL',
    VENDEDOR_PJ BOOLEAN NOT NULL DEFAULT 'false'
);

CREATE TABLE WH.DIM_AD (
    AD_KEY SERIAL PRIMARY KEY,
    AD_ID VARCHAR(20),
    TITULO VARCHAR(3000) NOT NULL DEFAULT 'INDISPONIVEL',
    TIPO_ANUNCIO VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    ACEITA_TROCA BOOLEAN NOT NULL DEFAULT 'false',
    TROCA_COM_TROCO BOOLEAN NOT NULL DEFAULT 'false',
    ENTREGA_CARRO BOOLEAN NOT NULL DEFAULT 'false',
    COMENTARIO_DONO VARCHAR(5000) NOT NULL DEFAULT 'INDISPONIVEL',
    WEBSITE VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL',
    LINK_AD VARCHAR(100) NOT NULL DEFAULT 'INDISPONIVEL'
);

CREATE TABLE WH.FACT_AD (
    ROW_ID SERIAL PRIMARY KEY,
    AD_KEY INT,
    CAR_KEY INT,
    SELLER_KEY INT,
    LOCATION_KEY INT,
    RESOURCE_KEY INT,
    KILOMETRAGEM NUMERIC,
    PRECO NUMERIC,
    PRECO_DESEJADO NUMERIC NOT NULL DEFAULT 0,
    FIPE NUMERIC,
    DATA_CARGA TIMESTAMP
);