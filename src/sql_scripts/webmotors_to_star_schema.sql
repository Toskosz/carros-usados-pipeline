INSERT INTO WH.DIM_AD (
    AD_ID,
    TITULO,
    TIPO_ANUNCIO,
    ACEITA_TROCA,
    TROCA_COM_TROCO,
    ENTREGA_CARRO,
    COMENTARIO_DONO,
    WEBSITE
) SELECT DISTINCT
    A.AD_ID,
    A.TITULO,
    A.TIPO_ANUNCIO,
    A.ACEITA_TROCA,
    A.TROCA_COM_TROCO,
    A.ENTREGA_CARRO,
    A.COMENTARIO_DONO,
    A.WEBSITE
FROM STG.WEBMOTORS AS A;

UPDATE STG.WEBMOTORS
SET AD_KEY = WH.DIM_AD.AD_KEY
FROM WH.DIM_AD
WHERE WH.DIM_AD.AD_ID = STG.WEBMOTORS.AD_ID AND
    WH.DIM_AD.TITULO = STG.WEBMOTORS.TITULO AND
    WH.DIM_AD.TIPO_ANUNCIO = STG.WEBMOTORS.TIPO_ANUNCIO AND
    WH.DIM_AD.ACEITA_TROCA = STG.WEBMOTORS.ACEITA_TROCA AND
    WH.DIM_AD.TROCA_COM_TROCO = STG.WEBMOTORS.TROCA_COM_TROCO AND
    WH.DIM_AD.ENTREGA_CARRO = STG.WEBMOTORS.ENTREGA_CARRO AND
    WH.DIM_AD.COMENTARIO_DONO = STG.WEBMOTORS.COMENTARIO_DONO  AND
    WH.DIM_AD.WEBSITE = STG.WEBMOTORS.WEBSITE;

INSERT INTO WH.DIM_CAR (
    FABRICANTE,
    MODELO,
    VERSAO,
    ANO_FABRICACAO,
    ANO_MODELO,
    TRANSMISSAO,
    QNTD_PORTAS,
    CORPO_VEICULO,
    COR,
    COMBUSTIVEL
) SELECT DISTINCT
    A.FABRICANTE,
    A.MODELO,
    A.VERSAO,
    A.ANO_FABRICACAO,
    A.ANO_MODELO,
    A.TRANSMISSAO,
    A.QNTD_PORTAS,
    A.CORPO_VEICULO,
    A.COR,
    A.COMBUSTIVEL
FROM STG.WEBMOTORS AS A;

UPDATE STG.WEBMOTORS
SET CAR_KEY = WH.DIM_CAR.CAR_KEY
FROM WH.DIM_CAR
WHERE WH.DIM_CAR.FABRICANTE = STG.WEBMOTORS.FABRICANTE AND
    WH.DIM_CAR.TIPO_VEICULO = 'INDISPONIVEL' AND
    WH.DIM_CAR.MODELO = STG.WEBMOTORS.MODELO AND
    WH.DIM_CAR.VERSAO = STG.WEBMOTORS.VERSAO AND
    WH.DIM_CAR.ANO_FABRICACAO = STG.WEBMOTORS.ANO_FABRICACAO AND
    WH.DIM_CAR.ANO_MODELO = STG.WEBMOTORS.ANO_MODELO AND
    WH.DIM_CAR.TRANSMISSAO = STG.WEBMOTORS.TRANSMISSAO AND
    WH.DIM_CAR.QNTD_PORTAS = STG.WEBMOTORS.QNTD_PORTAS AND
    WH.DIM_CAR.CORPO_VEICULO = STG.WEBMOTORS.CORPO_VEICULO AND
    WH.DIM_CAR.COR = STG.WEBMOTORS.COR AND
    WH.DIM_CAR.COR_SECUNDARIA = 'INDISPONIVEL' AND
    WH.DIM_CAR.COMBUSTIVEL = STG.WEBMOTORS.COMBUSTIVEL AND
    WH.DIM_CAR.MOTOR = 'INDISPONIVEL' AND
    WH.DIM_CAR.PLACA = 'INDISPONIVEL';

INSERT INTO WH.DIM_LOCATION (
    CIDADE,
    ESTADO,
    UF
) SELECT DISTINCT
    A.CIDADE_VENDEDOR,
    A.ESTADO_VENDEDOR,
    A.UF_VENDEDOR
FROM STG.WEBMOTORS AS A;

UPDATE STG.WEBMOTORS
SET LOCATION_KEY = WH.DIM_LOCATION.LOCATION_KEY
FROM WH.DIM_LOCATION
WHERE WH.DIM_LOCATION.CIDADE = STG.WEBMOTORS.CIDADE_VENDEDOR AND
    WH.DIM_LOCATION.ESTADO = STG.WEBMOTORS.ESTADO_VENDEDOR AND
    WH.DIM_LOCATION.UF = STG.WEBMOTORS.UF_VENDEDOR AND
    WH.DIM_LOCATION.BAIRRO = 'INDISPONIVEL' AND
    WH.DIM_LOCATION.ENDERECO = 'INDISPONIVEL' AND
    WH.DIM_LOCATION.COMPLEMENTO_ENDERECO = 'INDISPONIVEL';

INSERT INTO WH.DIM_RESOURCE (
    ALIENADO,
    REVISOES_PELA_AGENDA_CARRO,
    REVISOES_PELA_CONCESSIONARIA,
    UNICO_DONO,
    BLINDADO,
    LICENCIADO,
    IPVA_PAGO,
    GARANTIA_DE_FABRICA,
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
    DISQUETEIRA,
    DVD_PLAYER,
    COMPUTAR_DE_BORDO,
    CONTROLE_AUTOMATICO_VEL,
    CONTROLE_TRACAO,
    DESEMBACADOR_TRASEIRO,
    DIR_HIDRAULICA,
    ENCOSTO_CABECA_TRASEIRO,
    FAROL_DE_XENONIO,
    FREIO_ABS,
    GPS,
    LIMPADOR_TRASEIRO,
    PROTETOR_CACAMBA,
    RADIO,
    RADIO_TOCAFITA,
    RETROVISOR_FOTOCROMICO,
    RETROVISOR_ELETRICO,
    RODAS_LIGA_LEVE,
    SENSOR_DE_CHUVA,
    SENSOR_DE_ESTACIONAMENTO,
    TETO_SOLAR,
    TRACAO_QUATRO_POR_QUATRO,
    TRAVAS_ELETRICAS,
    VIDROS_ELETRICOS,
    VOLANTE_REG_ALTURA
) SELECT DISTINCT
    A.ALIENADO,
    A.REVISOES_PELA_AGENDA_CARRO,
    A.REVISOES_PELA_CONCESSIONARIA,
    A.UNICO_DONO,
    A.BLINDADO,
    A.LICENCIADO,
    A.IPVA_PAGO,
    A.GARANTIA_DE_FABRICA,
    A.AIRBAG,
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
    A.ENCOSTO_CABECA_TRASEIRO,
    A.FAROL_DE_XENONIO,
    A.FREIO_ABS,
    A.GPS,
    A.LIMPADOR_TRASEIRO,
    A.PROTETOR_CACAMBA,
    A.RADIO,
    A.RADIO_TOCAFITA,
    A.RETROVISOR_FOTOCROMICO,
    A.RETROVISOR_ELETRICO,
    A.RODAS_LIGA_LEVE,
    A.SENSOR_DE_CHUVA,
    A.SENSOR_DE_ESTACIONAMENTO,
    A.TETO_SOLAR,
    A.TRACAO_QUATRO_POR_QUATRO,
    A.TRAVAS_ELETRICAS,
    A.VIDROS_ELETRICOS,
    A.VOLANTE_REG_ALTURA
FROM STG.WEBMOTORS AS A;

UPDATE STG.WEBMOTORS
SET RESOURCE_KEY = WH.DIM_RESOURCE.RESOURCE_KEY
FROM WH.DIM_RESOURCE
WHERE WH.DIM_RESOURCE.FINANCIAVEL = 'false' AND
    WH.DIM_RESOURCE.FINANCIADO = 'false' AND
    WH.DIM_RESOURCE.IMPOSTOS_PAGOS = 'false' AND
    WH.DIM_RESOURCE.REGISTRAMENTO_PAGO = 'false' AND
    WH.DIM_RESOURCE.QUITADO = 'false' AND
    WH.DIM_RESOURCE.ALIENADO = STG.WEBMOTORS.ALIENADO AND
    WH.DIM_RESOURCE.REVISOES_PELA_AGENDA_CARRO = STG.WEBMOTORS.REVISOES_PELA_AGENDA_CARRO AND
    WH.DIM_RESOURCE.REVISOES_PELA_CONCESSIONARIA = STG.WEBMOTORS.REVISOES_PELA_CONCESSIONARIA AND
    WH.DIM_RESOURCE.UNICO_DONO = STG.WEBMOTORS.UNICO_DONO AND
    WH.DIM_RESOURCE.COLECIONADOR = 'false'AND
    WH.DIM_RESOURCE.ADAPTADO_DEFICIENCIA = 'false' AND
    WH.DIM_RESOURCE.BLINDADO = STG.WEBMOTORS.BLINDADO AND
    WH.DIM_RESOURCE.LICENCIADO = STG.WEBMOTORS.LICENCIADO AND
    WH.DIM_RESOURCE.IPVA_PAGO = STG.WEBMOTORS.IPVA_PAGO  AND
    WH.DIM_RESOURCE.GARANTIA_DE_FABRICA = STG.WEBMOTORS.GARANTIA_DE_FABRICA AND
    WH.DIM_RESOURCE.AIRBAG = STG.WEBMOTORS.AIRBAG AND
    WH.DIM_RESOURCE.AIRBAG_DUPLO = 'false' AND
    WH.DIM_RESOURCE.ALARME = STG.WEBMOTORS.ALARME AND
    WH.DIM_RESOURCE.AR_CONDICIONADO = STG.WEBMOTORS.AR_CONDICIONADO AND
    WH.DIM_RESOURCE.AR_QUENTE = STG.WEBMOTORS.AR_QUENTE AND
    WH.DIM_RESOURCE.BANCO_REGULA_ALTURA = STG.WEBMOTORS.BANCO_REGULA_ALTURA AND
    WH.DIM_RESOURCE.BANCO_COM_AQUECIMENTO = STG.WEBMOTORS.BANCO_COM_AQUECIMENTO AND
    WH.DIM_RESOURCE.BANCO_DE_COURO = STG.WEBMOTORS.BANCO_DE_COURO AND
    WH.DIM_RESOURCE.CAPOTA_MARITIMA = STG.WEBMOTORS.CAPOTA_MARITIMA AND
    WH.DIM_RESOURCE.MP3_CD_PLAYER = STG.WEBMOTORS.MP3_CD_PLAYER AND
    WH.DIM_RESOURCE.CD_PLAYER = STG.WEBMOTORS.CD_PLAYER AND
    WH.DIM_RESOURCE.DISQUETEIRA = STG.WEBMOTORS.DISQUETEIRA AND
    WH.DIM_RESOURCE.DVD_PLAYER = STG.WEBMOTORS.DVD_PLAYER AND
    WH.DIM_RESOURCE.COMPUTAR_DE_BORDO = STG.WEBMOTORS.COMPUTAR_DE_BORDO AND
    WH.DIM_RESOURCE.CONTROLE_AUTOMATICO_VEL = STG.WEBMOTORS.CONTROLE_AUTOMATICO_VEL AND
    WH.DIM_RESOURCE.CONTROLE_TRACAO = STG.WEBMOTORS.CONTROLE_TRACAO AND
    WH.DIM_RESOURCE.DESEMBACADOR_TRASEIRO = STG.WEBMOTORS.DESEMBACADOR_TRASEIRO AND
    WH.DIM_RESOURCE.DIR_HIDRAULICA = STG.WEBMOTORS.DIR_HIDRAULICA AND
    WH.DIM_RESOURCE.DIR_ELETRICA = 'false'AND
    WH.DIM_RESOURCE.ENCOSTO_CABECA_TRASEIRO = STG.WEBMOTORS.ENCOSTO_CABECA_TRASEIRO AND
    WH.DIM_RESOURCE.FAROL_DE_XENONIO = STG.WEBMOTORS.FAROL_DE_XENONIO AND
    WH.DIM_RESOURCE.FAROL_DE_MILHA = 'false' AND
    WH.DIM_RESOURCE.FAROL_NEBLINA = 'false' AND
    WH.DIM_RESOURCE.FREIO_ABS = STG.WEBMOTORS.FREIO_ABS AND
    WH.DIM_RESOURCE.GPS = STG.WEBMOTORS.GPS AND
    WH.DIM_RESOURCE.LIMPADOR_TRASEIRO = STG.WEBMOTORS.LIMPADOR_TRASEIRO AND
    WH.DIM_RESOURCE.PROTETOR_CACAMBA = STG.WEBMOTORS.PROTETOR_CACAMBA AND
    WH.DIM_RESOURCE.RADIO = STG.WEBMOTORS.RADIO AND
    WH.DIM_RESOURCE.RADIO_TOCAFITA = STG.WEBMOTORS.RADIO_TOCAFITA AND
    WH.DIM_RESOURCE.KIT_MULTIMIDIA = 'false' AND
    WH.DIM_RESOURCE.RETROVISOR_FOTOCROMICO = STG.WEBMOTORS.RETROVISOR_FOTOCROMICO AND
    WH.DIM_RESOURCE.RETROVISOR_ELETRICO = STG.WEBMOTORS.RETROVISOR_ELETRICO AND
    WH.DIM_RESOURCE.RODAS_LIGA_LEVE = STG.WEBMOTORS.RODAS_LIGA_LEVE AND
    WH.DIM_RESOURCE.SENSOR_DE_CHUVA = STG.WEBMOTORS.SENSOR_DE_CHUVA AND
    WH.DIM_RESOURCE.SENSOR_DE_ESTACIONAMENTO = STG.WEBMOTORS.SENSOR_DE_ESTACIONAMENTO AND
    WH.DIM_RESOURCE.TETO_SOLAR = STG.WEBMOTORS.TETO_SOLAR AND
    WH.DIM_RESOURCE.TRACAO_QUATRO_POR_QUATRO = STG.WEBMOTORS.TRACAO_QUATRO_POR_QUATRO AND
    WH.DIM_RESOURCE.TRAVAS_ELETRICAS = STG.WEBMOTORS.TRAVAS_ELETRICAS AND
    WH.DIM_RESOURCE.VIDROS_ELETRICOS = STG.WEBMOTORS.VIDROS_ELETRICOS AND
    WH.DIM_RESOURCE.VOLANTE_REG_ALTURA = STG.WEBMOTORS.VOLANTE_REG_ALTURA AND
    WH.DIM_RESOURCE.PILOTO_AUTOMATICO = 'false' AND
    WH.DIM_RESOURCE.ESCAPAMENTO_ESPORTIVO = 'false';

INSERT INTO WH.DIM_SELLER (
    TIPO_VENDEDOR
) SELECT DISTINCT
    A.TIPO_VENDEDOR
FROM STG.WEBMOTORS AS A;

UPDATE STG.WEBMOTORS
SET SELLER_KEY = WH.DIM_SELLER.SELLER_KEY
FROM WH.DIM_SELLER
WHERE WH.DIM_SELLER.NOME_VENDEDOR = 'false'  AND
    WH.DIM_SELLER.DOCUMENTO_VENDEDOR = 'false'  AND
    WH.DIM_SELLER.EMAIL = 'false'  AND
    WH.DIM_SELLER.CELULAR = 'false'  AND
    WH.DIM_SELLER.TELEFONE = 'false'  AND
    WH.DIM_SELLER.WHATSAPP = 'false'  AND
    WH.DIM_SELLER.TIPO_VENDEDOR = STG.WEBMOTORS.TIPO_VENDEDOR AND
    WH.DIM_SELLER.VENDEDOR_PJ = 'false';

INSERT INTO WH.FACT_AD (
    AD_KEY,
    CAR_KEY,
    SELLER_KEY,
    LOCATION_KEY,
    RESOURCE_KEY,
    KILOMETRAGEM,
    PRECO,
    PRECO_DESEJADO,
    FIPE
) SELECT
    A.AD_KEY,
    A.CAR_KEY,
    A.SELLER_KEY,
    A.LOCATION_KEY,
    A.RESOURCE_KEY,
    A.KILOMETRAGEM,
    A.PRECO,
    A.PRECO_DESEJADO,
    A.PORCENTAGEM_FIPE
FROM STG.WEBMOTORS AS A;