/* DIM_AD */

INSERT INTO WH.DIM_AD (
    AD_ID,
    TITULO,
    ACEITA_TROCA,
    WEBSITE,
    LINK_AD
) SELECT DISTINCT
    A.AD_ID,
    A.INFORMACOES_ADICIONAIS,
    A.ACEITA_TROCA,
    A.WEBSITE,
    A.LINK_AD
FROM STG.AUTOLINE AS A;

DELETE  FROM
    WH.DIM_AD a
        USING WH.DIM_AD b
WHERE
    a.AD_KEY > b.AD_KEY AND
    a.AD_ID = b.AD_ID AND
    a.TITULO = b.TITULO AND
    a.TIPO_ANUNCIO = b.TIPO_ANUNCIO AND
    a.ACEITA_TROCA = b.ACEITA_TROCA AND
    a.TROCA_COM_TROCO = b.TROCA_COM_TROCO AND
    a.ENTREGA_CARRO = b.ENTREGA_CARRO AND
    a.COMENTARIO_DONO = b.COMENTARIO_DONO AND
    a.WEBSITE = b.WEBSITE AND
    a.LINK_AD = b.LINK_AD;

UPDATE STG.AUTOLINE
SET AD_KEY = WH.DIM_AD.AD_KEY
FROM WH.DIM_AD
WHERE WH.DIM_AD.AD_ID = STG.AUTOLINE.AD_ID AND
    WH.DIM_AD.TITULO = STG.AUTOLINE.INFORMACOES_ADICIONAIS AND
    WH.DIM_AD.TIPO_ANUNCIO = 'INDISPONIVEL' AND
    WH.DIM_AD.ACEITA_TROCA = STG.AUTOLINE.ACEITA_TROCA AND
    WH.DIM_AD.TROCA_COM_TROCO = 'false' AND
    WH.DIM_AD.ENTREGA_CARRO = 'false' AND
    WH.DIM_AD.COMENTARIO_DONO = 'INDISPONIVEL' AND
    WH.DIM_AD.WEBSITE = STG.AUTOLINE.WEBSITE AND
    WH.DIM_AD.LINK_AD = STG.AUTOLINE.LINK_AD;


/* DIM_CAR */

INSERT INTO WH.DIM_CAR (
    FABRICANTE,
    TIPO_VEICULO,
    MODELO,
    VERSAO,
    ANO_FABRICACAO,
    ANO_MODELO,
    TRANSMISSAO,
    QNTD_PORTAS,
    CORPO_VEICULO,
    COR,
    COR_SECUNDARIA,
    COMBUSTIVEL,
    MOTOR,
    PLACA
) SELECT DISTINCT
    A.FABRICANTE,
    A.TIPO_VEICULO,
    A.MODELO,
    A.VERSAO,
    A.ANO_FABRICACAO,
    A.ANO_MODELO,
    A.TRANSMISSAO,
    A.QNTD_PORTAS,
    A.CORPO_VEICULO,
    A.COR,
    A.COR_SECUNDARIA,
    A.COMBUSTIVEL,
    A.MOTOR,
    A.PLACA
FROM STG.AUTOLINE AS A;

DELETE FROM
    WH.DIM_CAR a
        USING WH.DIM_CAR b
WHERE
    a.CAR_KEY > b.CAR_KEY AND
    a.FABRICANTE = b.FABRICANTE AND
    a.TIPO_VEICULO = b.TIPO_VEICULO AND
    a.MODELO = b.MODELO AND
    a.VERSAO = b.VERSAO AND
    a.ANO_FABRICACAO = b.ANO_FABRICACAO AND
    a.ANO_MODELO = b.ANO_MODELO AND
    a.TRANSMISSAO = b.TRANSMISSAO AND
    a.QNTD_PORTAS = b.QNTD_PORTAS AND
    a.CORPO_VEICULO = b.CORPO_VEICULO AND
    a.COR = b.COR AND
    a.COR_SECUNDARIA = b.COR_SECUNDARIA AND
    a.COMBUSTIVEL = b.COMBUSTIVEL AND
    a.MOTOR = b.MOTOR AND
    a.PLACA = b.PLACA;

UPDATE STG.AUTOLINE
SET CAR_KEY = WH.DIM_CAR.CAR_KEY
FROM WH.DIM_CAR
WHERE WH.DIM_CAR.FABRICANTE = STG.AUTOLINE.FABRICANTE AND
    WH.DIM_CAR.TIPO_VEICULO = STG.AUTOLINE.TIPO_VEICULO AND
    WH.DIM_CAR.MODELO = STG.AUTOLINE.MODELO AND
    WH.DIM_CAR.VERSAO = STG.AUTOLINE.VERSAO AND
    WH.DIM_CAR.ANO_FABRICACAO = STG.AUTOLINE.ANO_FABRICACAO AND
    WH.DIM_CAR.ANO_MODELO = STG.AUTOLINE.ANO_MODELO AND
    WH.DIM_CAR.TRANSMISSAO = STG.AUTOLINE.TRANSMISSAO AND
    WH.DIM_CAR.QNTD_PORTAS = STG.AUTOLINE.QNTD_PORTAS AND
    WH.DIM_CAR.CORPO_VEICULO = STG.AUTOLINE.CORPO_VEICULO AND
    WH.DIM_CAR.COR = STG.AUTOLINE.COR AND
    WH.DIM_CAR.COR_SECUNDARIA = STG.AUTOLINE.COR_SECUNDARIA AND
    WH.DIM_CAR.COMBUSTIVEL = STG.AUTOLINE.COMBUSTIVEL AND
    WH.DIM_CAR.MOTOR = STG.AUTOLINE.MOTOR AND
    WH.DIM_CAR.PLACA = STG.AUTOLINE.PLACA;

/* DIM_LOCATION */

INSERT INTO WH.DIM_LOCATION (
    CIDADE,
    ESTADO,
    UF,
    BAIRRO,
    ENDERECO,
    COMPLEMENTO_ENDERECO
) SELECT DISTINCT
    A.CIDADE,
    A.ESTADO,
    A.UF,
    A.BAIRRO,
    A.ENDERECO,
    A.COMPLEMENTO_ENDERECO
FROM STG.AUTOLINE AS A;

DELETE FROM
    WH.DIM_LOCATION a
        USING WH.DIM_LOCATION b
WHERE
    a.LOCATION_KEY > b.LOCATION_KEY AND
    a.CIDADE = b.CIDADE AND
    a.ESTADO = b.ESTADO AND
    a.UF = b.UF AND
    a.BAIRRO = b.BAIRRO AND
    a.ENDERECO = b.ENDERECO AND
    a.COMPLEMENTO_ENDERECO = b.COMPLEMENTO_ENDERECO;

UPDATE STG.AUTOLINE
SET LOCATION_KEY = WH.DIM_LOCATION.LOCATION_KEY
FROM WH.DIM_LOCATION
WHERE WH.DIM_LOCATION.CIDADE = STG.AUTOLINE.CIDADE AND
    WH.DIM_LOCATION.ESTADO = STG.AUTOLINE.ESTADO AND
    WH.DIM_LOCATION.UF = STG.AUTOLINE.UF AND
    WH.DIM_LOCATION.BAIRRO = STG.AUTOLINE.BAIRRO AND
    WH.DIM_LOCATION.ENDERECO = STG.AUTOLINE.ENDERECO AND
    WH.DIM_LOCATION.COMPLEMENTO_ENDERECO = STG.AUTOLINE.COMPLEMENTO_ENDERECO;

/* DIM_RESOURCE */
INSERT INTO WH.DIM_RESOURCE (
    FINANCIAVEL,
    FINANCIADO,
    IMPOSTOS_PAGOS,
    REGISTRAMENTO_PAGO,
    QUITADO,
    UNICO_DONO,
    COLECIONADOR,
    ADAPTADO_DEFICIENCIA,
    BLINDADO,
    GARANTIA_DE_FABRICA,
    AIRBAG,
    AIRBAG_DUPLO,
    ALARME,
    AR_CONDICIONADO,
    AR_QUENTE,
    BANCO_REGULA_ALTURA,
    BANCO_COM_AQUECIMENTO,
    BANCO_DE_COURO,
    CAPOTA_MARITIMA,
    MP3_CD_PLAYER,
    CD_PLAYER,
    DISQUETEIRA,
    DVD_PLAYER,
    COMPUTAR_DE_BORDO,
    CONTROLE_AUTOMATICO_VEL,
    CONTROLE_TRACAO,
    DESEMBACADOR_TRASEIRO,
    DIR_HIDRAULICA,
    DIR_ELETRICA,
    ENCOSTO_CABECA_TRASEIRO,
    FAROL_DE_XENONIO,
    FAROL_DE_MILHA,
    FAROL_NEBLINA,
    FREIO_ABS,
    GPS,
    LIMPADOR_TRASEIRO,
    PROTETOR_CACAMBA,
    RADIO,
    RADIO_TOCAFITA,
    KIT_MULTIMIDIA,
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
    PILOTO_AUTOMATICO,
    ESCAPAMENTO_ESPORTIVO
) SELECT DISTINCT
    A.FINANCIAVEL,
    A.FINANCIADO,
    A.IMPOSTOS_PAGOS,
    A.REGISTRAMENTO_PAGO,
    A.QUITADO,
    A.DONO_UNICO,
    A.COLECIONADOR,
    A.ADAPTADO_DEFICIENCIA,
    A.BLINDADO,
    A.GARANTIA_DE_FABRICA,
    A.AIRBAG,
    A.AIRBAG_DUPLO,
    A.ALARME,
    A.AR_CONDICIONADO,
    A.AR_QUENTE,
    A.BANCO_REGULA_ALTURA,
    A.BANCO_COM_AQUECIMENTO,
    A.BANCO_DE_COURO,
    A.CAPOTA_MARITIMA,
    A.MP3_CD_PLAYER,
    A.CD_PLAYER,
    A.DISQUETEIRA,
    A.DVD_PLAYER,
    A.COMPUTAR_DE_BORDO,
    A.CONTROLE_AUTOMATICO_VEL,
    A.CONTROLE_TRACAO,
    A.DESEMBACADOR_TRASEIRO,
    A.DIR_HIDRAULICA,
    A.DIR_ELETRICA,
    A.ENCOSTO_CABECA_TRASEIRO,
    A.FAROL_DE_XENONIO,
    A.FAROL_DE_MILHA,
    A.FAROL_NEBLINA,
    A.FREIO_ABS,
    A.GPS,
    A.LIMPADOR_TRASEIRO,
    A.PROTETOR_CACAMBA,
    A.RADIO,
    A.RADIO_TOCAFITA,
    A.KIT_MULTIMIDIA,
    A.RETROVISOR_FOTOCROMICO,
    A.RETROVISOR_ELETRICO,
    A.RODAS_LIGA_LEVE,
    A.SENSOR_DE_CHUVA,
    A.SENSOR_DE_ESTACIONAMENTO,
    A.TETO_SOLAR,
    A.TRACAO_QUATRO_POR_QUATRO,
    A.TRAVAS_ELETRICAS,
    A.VIDROS_ELETRICOS,
    A.VOLANTE_REG_ALTURA,
    A.PILOTO_AUTOMATICO,
    A.ESCAPAMENTO_ESPORTIVO
FROM STG.AUTOLINE AS A;

DELETE FROM
    WH.DIM_RESOURCE a
        USING WH.DIM_RESOURCE b
WHERE
    a.RESOURCE_KEY > b.RESOURCE_KEY AND
    a.FINANCIAVEL = b.FINANCIAVEL AND
    a.FINANCIADO = b.FINANCIADO AND
    a.IMPOSTOS_PAGOS = b.IMPOSTOS_PAGOS AND
    a.REGISTRAMENTO_PAGO = REGISTRAMENTO_PAGO AND
    a.QUITADO = b.QUITADO AND
    a.ALIENADO = b.ALIENADO AND
    a.REVISOES_PELA_AGENDA_CARRO = b.REVISOES_PELA_AGENDA_CARRO AND
    a.REVISOES_PELA_CONCESSIONARIA = b.REVISOES_PELA_CONCESSIONARIA AND
    a.UNICO_DONO = b.UNICO_DONO AND
    a.COLECIONADOR = b.COLECIONADOR AND
    a.ADAPTADO_DEFICIENCIA = b.ADAPTADO_DEFICIENCIA AND
    a.BLINDADO = b.BLINDADO AND
    a.LICENCIADO = b.LICENCIADO AND
    a.IPVA_PAGO = b.IPVA_PAGO AND
    a.GARANTIA_DE_FABRICA = b.GARANTIA_DE_FABRICA AND
    a.AIRBAG = b.AIRBAG AND
    a.AIRBAG_DUPLO = b.AIRBAG_DUPLO AND
    a.ALARME = b.ALARME AND
    a.AR_CONDICIONADO = b.AR_CONDICIONADO AND
    a.AR_QUENTE = b.AR_QUENTE AND
    a.BANCO_REGULA_ALTURA = b.BANCO_REGULA_ALTURA AND
    a.BANCO_COM_AQUECIMENTO = b.BANCO_COM_AQUECIMENTO AND
    a.BANCO_DE_COURO = b.BANCO_DE_COURO AND
    a.CAPOTA_MARITIMA = b.CAPOTA_MARITIMA AND
    a.MP3_CD_PLAYER = b.MP3_CD_PLAYER AND
    a.CD_PLAYER = b.CD_PLAYER AND
    a.DISQUETEIRA = b.DISQUETEIRA AND
    a.DVD_PLAYER = b.DVD_PLAYER AND
    a.COMPUTAR_DE_BORDO = b.COMPUTAR_DE_BORDO AND
    a.CONTROLE_AUTOMATICO_VEL = b.CONTROLE_AUTOMATICO_VEL AND
    a.CONTROLE_TRACAO = b.CONTROLE_TRACAO AND
    a.DESEMBACADOR_TRASEIRO = b.DESEMBACADOR_TRASEIRO AND
    a.DIR_HIDRAULICA = b.DIR_HIDRAULICA AND
    a.DIR_ELETRICA = b.DIR_ELETRICA AND
    a.ENCOSTO_CABECA_TRASEIRO = b.ENCOSTO_CABECA_TRASEIRO AND
    a.FAROL_DE_XENONIO = b.FAROL_DE_XENONIO AND
    a.FAROL_DE_MILHA = b.FAROL_DE_MILHA AND
    a.FAROL_NEBLINA = b.FAROL_NEBLINA AND
    a.FREIO_ABS = b.FREIO_ABS AND
    a.GPS = b.GPS AND
    a.LIMPADOR_TRASEIRO = b.LIMPADOR_TRASEIRO AND
    a.PROTETOR_CACAMBA = b.PROTETOR_CACAMBA AND
    a.RADIO = b.RADIO AND
    a.RADIO_TOCAFITA = b.RADIO_TOCAFITA AND
    a.KIT_MULTIMIDIA = b.KIT_MULTIMIDIA AND
    a.RETROVISOR_FOTOCROMICO = b.RETROVISOR_FOTOCROMICO AND
    a.RETROVISOR_ELETRICO = b.RETROVISOR_ELETRICO AND
    a.RODAS_LIGA_LEVE = b.RODAS_LIGA_LEVE AND
    a.SENSOR_DE_CHUVA = b.SENSOR_DE_CHUVA AND
    a.SENSOR_DE_ESTACIONAMENTO = b.SENSOR_DE_ESTACIONAMENTO AND
    a.TETO_SOLAR = b.TETO_SOLAR AND
    a.TRACAO_QUATRO_POR_QUATRO = b.TRACAO_QUATRO_POR_QUATRO AND
    a.TRAVAS_ELETRICAS = b.TRAVAS_ELETRICAS AND
    a.VIDROS_ELETRICOS = b.VIDROS_ELETRICOS AND
    a.VOLANTE_REG_ALTURA = b.VOLANTE_REG_ALTURA AND
    a.PILOTO_AUTOMATICO = b.PILOTO_AUTOMATICO AND
    a.ESCAPAMENTO_ESPORTIVO = b.ESCAPAMENTO_ESPORTIVO;

UPDATE STG.AUTOLINE
SET RESOURCE_KEY = WH.DIM_RESOURCE.RESOURCE_KEY
FROM WH.DIM_RESOURCE
WHERE WH.DIM_RESOURCE.FINANCIAVEL = STG.AUTOLINE.FINANCIAVEL AND
    WH.DIM_RESOURCE.FINANCIADO = STG.AUTOLINE.FINANCIADO AND
    WH.DIM_RESOURCE.IMPOSTOS_PAGOS = STG.AUTOLINE.IMPOSTOS_PAGOS AND
    WH.DIM_RESOURCE.REGISTRAMENTO_PAGO = STG.AUTOLINE.REGISTRAMENTO_PAGO AND
    WH.DIM_RESOURCE.QUITADO = STG.AUTOLINE.QUITADO AND
    WH.DIM_RESOURCE.ALIENADO = 'false' AND
    WH.DIM_RESOURCE.REVISOES_PELA_AGENDA_CARRO = 'false' AND
    WH.DIM_RESOURCE.REVISOES_PELA_CONCESSIONARIA = 'false' AND
    WH.DIM_RESOURCE.UNICO_DONO = STG.AUTOLINE.DONO_UNICO AND
    WH.DIM_RESOURCE.COLECIONADOR = STG.AUTOLINE.COLECIONADOR AND
    WH.DIM_RESOURCE.ADAPTADO_DEFICIENCIA = STG.AUTOLINE.ADAPTADO_DEFICIENCIA AND
    WH.DIM_RESOURCE.BLINDADO = STG.AUTOLINE.BLINDADO AND
    WH.DIM_RESOURCE.LICENCIADO = 'false' AND
    WH.DIM_RESOURCE.IPVA_PAGO = 'false'  AND
    WH.DIM_RESOURCE.GARANTIA_DE_FABRICA = STG.AUTOLINE.GARANTIA_DE_FABRICA AND
    WH.DIM_RESOURCE.AIRBAG = STG.AUTOLINE.AIRBAG AND
    WH.DIM_RESOURCE.AIRBAG_DUPLO = STG.AUTOLINE.AIRBAG_DUPLO AND
    WH.DIM_RESOURCE.ALARME = STG.AUTOLINE.ALARME AND
    WH.DIM_RESOURCE.AR_CONDICIONADO = STG.AUTOLINE.AR_CONDICIONADO AND
    WH.DIM_RESOURCE.AR_QUENTE = STG.AUTOLINE.AR_QUENTE AND
    WH.DIM_RESOURCE.BANCO_REGULA_ALTURA = STG.AUTOLINE.BANCO_REGULA_ALTURA AND
    WH.DIM_RESOURCE.BANCO_COM_AQUECIMENTO = STG.AUTOLINE.BANCO_COM_AQUECIMENTO AND
    WH.DIM_RESOURCE.BANCO_DE_COURO = STG.AUTOLINE.BANCO_DE_COURO AND
    WH.DIM_RESOURCE.CAPOTA_MARITIMA = STG.AUTOLINE.CAPOTA_MARITIMA AND
    WH.DIM_RESOURCE.MP3_CD_PLAYER = STG.AUTOLINE.MP3_CD_PLAYER AND
    WH.DIM_RESOURCE.CD_PLAYER = STG.AUTOLINE.CD_PLAYER AND
    WH.DIM_RESOURCE.DISQUETEIRA = STG.AUTOLINE.DISQUETEIRA AND
    WH.DIM_RESOURCE.DVD_PLAYER = STG.AUTOLINE.DVD_PLAYER AND
    WH.DIM_RESOURCE.COMPUTAR_DE_BORDO = STG.AUTOLINE.COMPUTAR_DE_BORDO AND
    WH.DIM_RESOURCE.CONTROLE_AUTOMATICO_VEL = STG.AUTOLINE.CONTROLE_AUTOMATICO_VEL AND
    WH.DIM_RESOURCE.CONTROLE_TRACAO = STG.AUTOLINE.CONTROLE_TRACAO AND
    WH.DIM_RESOURCE.DESEMBACADOR_TRASEIRO = STG.AUTOLINE.DESEMBACADOR_TRASEIRO AND
    WH.DIM_RESOURCE.DIR_HIDRAULICA = STG.AUTOLINE.DIR_HIDRAULICA AND
    WH.DIM_RESOURCE.DIR_ELETRICA = STG.AUTOLINE.DIR_ELETRICA AND
    WH.DIM_RESOURCE.ENCOSTO_CABECA_TRASEIRO = STG.AUTOLINE.ENCOSTO_CABECA_TRASEIRO AND
    WH.DIM_RESOURCE.FAROL_DE_XENONIO = STG.AUTOLINE.FAROL_DE_XENONIO AND
    WH.DIM_RESOURCE.FAROL_DE_MILHA = STG.AUTOLINE.FAROL_DE_MILHA AND
    WH.DIM_RESOURCE.FAROL_NEBLINA = STG.AUTOLINE.FAROL_NEBLINA AND
    WH.DIM_RESOURCE.FREIO_ABS = STG.AUTOLINE.FREIO_ABS AND
    WH.DIM_RESOURCE.GPS = STG.AUTOLINE.GPS AND
    WH.DIM_RESOURCE.LIMPADOR_TRASEIRO = STG.AUTOLINE.LIMPADOR_TRASEIRO AND
    WH.DIM_RESOURCE.PROTETOR_CACAMBA = STG.AUTOLINE.PROTETOR_CACAMBA AND
    WH.DIM_RESOURCE.RADIO = STG.AUTOLINE.RADIO AND
    WH.DIM_RESOURCE.RADIO_TOCAFITA = STG.AUTOLINE.RADIO_TOCAFITA AND
    WH.DIM_RESOURCE.KIT_MULTIMIDIA = STG.AUTOLINE.KIT_MULTIMIDIA AND
    WH.DIM_RESOURCE.RETROVISOR_FOTOCROMICO = STG.AUTOLINE.RETROVISOR_FOTOCROMICO AND
    WH.DIM_RESOURCE.RETROVISOR_ELETRICO = STG.AUTOLINE.RETROVISOR_ELETRICO AND
    WH.DIM_RESOURCE.RODAS_LIGA_LEVE = STG.AUTOLINE.RODAS_LIGA_LEVE AND
    WH.DIM_RESOURCE.SENSOR_DE_CHUVA = STG.AUTOLINE.SENSOR_DE_CHUVA AND
    WH.DIM_RESOURCE.SENSOR_DE_ESTACIONAMENTO = STG.AUTOLINE.SENSOR_DE_ESTACIONAMENTO AND
    WH.DIM_RESOURCE.TETO_SOLAR = STG.AUTOLINE.TETO_SOLAR AND
    WH.DIM_RESOURCE.TRACAO_QUATRO_POR_QUATRO = STG.AUTOLINE.TRACAO_QUATRO_POR_QUATRO AND
    WH.DIM_RESOURCE.TRAVAS_ELETRICAS = STG.AUTOLINE.TRAVAS_ELETRICAS AND
    WH.DIM_RESOURCE.VIDROS_ELETRICOS = STG.AUTOLINE.VIDROS_ELETRICOS AND
    WH.DIM_RESOURCE.VOLANTE_REG_ALTURA = STG.AUTOLINE.VOLANTE_REG_ALTURA AND
    WH.DIM_RESOURCE.PILOTO_AUTOMATICO= STG.AUTOLINE.PILOTO_AUTOMATICO AND
    WH.DIM_RESOURCE.ESCAPAMENTO_ESPORTIVO = STG.AUTOLINE.ESCAPAMENTO_ESPORTIVO;

/* DIM SELLER */
INSERT INTO WH.DIM_SELLER (
    NOME_VENDEDOR,
    DOCUMENTO_VENDEDOR,
    EMAIL,
    CELULAR,
    TELEFONE,
    WHATSAPP,
    TIPO_VENDEDOR,
    VENDEDOR_PJ
) SELECT DISTINCT
    A.NOME_VENDEDOR,
    A.DOCUMENTO_VENDEDOR,
    A.EMAIL,
    A.CELULAR,
    A.TELEFONE,
    A.WHATSAPP,
    A.TIPO_VENDEDOR,
    A.VENDEDOR_PJ
FROM STG.AUTOLINE AS A;

DELETE FROM
    WH.DIM_SELLER a
        USING WH.DIM_SELLER b
WHERE
    a.SELLER_KEY > b.SELLER_KEY AND
    a.NOME_VENDEDOR = b.NOME_VENDEDOR AND
    a.DOCUMENTO_VENDEDOR = b.DOCUMENTO_VENDEDOR AND
    a.EMAIL = b.EMAIL AND
    a.CELULAR = b.CELULAR AND
    a.TELEFONE = b.TELEFONE AND
    a.WHATSAPP = b.WHATSAPP AND
    a.TIPO_VENDEDOR = b.TIPO_VENDEDOR AND
    a.VENDEDOR_PJ = b.VENDEDOR_PJ;

UPDATE STG.AUTOLINE
SET SELLER_KEY = WH.DIM_SELLER.SELLER_KEY
FROM WH.DIM_SELLER
WHERE WH.DIM_SELLER.NOME_VENDEDOR = STG.AUTOLINE.NOME_VENDEDOR AND
    WH.DIM_SELLER.DOCUMENTO_VENDEDOR = STG.AUTOLINE.DOCUMENTO_VENDEDOR AND
    WH.DIM_SELLER.EMAIL = STG.AUTOLINE.EMAIL AND
    WH.DIM_SELLER.CELULAR = STG.AUTOLINE.CELULAR AND
    WH.DIM_SELLER.TELEFONE = STG.AUTOLINE.TELEFONE AND
    WH.DIM_SELLER.WHATSAPP = STG.AUTOLINE.WHATSAPP AND
    WH.DIM_SELLER.TIPO_VENDEDOR = STG.AUTOLINE.TIPO_VENDEDOR AND
    WH.DIM_SELLER.VENDEDOR_PJ = STG.AUTOLINE.VENDEDOR_PJ;

/* FACT_AD */

INSERT INTO WH.FACT_AD (
    AD_KEY,
    CAR_KEY,
    SELLER_KEY,
    LOCATION_KEY,
    RESOURCE_KEY,
    KILOMETRAGEM,
    PRECO,
    FIPE
) SELECT
    A.AD_KEY,
    A.CAR_KEY,
    A.SELLER_KEY,
    A.LOCATION_KEY,
    A.RESOURCE_KEY,
    A.KILOMETRAGEM,
    A.PRECO,
    A.PRECO_FIPE
FROM STG.AUTOLINE AS A;