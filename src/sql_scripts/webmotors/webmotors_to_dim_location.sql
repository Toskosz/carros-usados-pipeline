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
SET STG.WEBMOTORS.LOCATION_KEY = WH.DIM_LOCATION.LOCATION_KEY
FROM STG.WEBMOTORS
INNER JOIN WH.DIM_LOCATION ON 
WH.DIM_LOCATION.CIDADE = STG.WEBMOTORS.CIDADE_VENDEDOR AND
WH.DIM_LOCATION.ESTADO = STG.WEBMOTORS.ESTADO_VENDEDOR AND
WH.DIM_LOCATION.UF = STG.WEBMOTORS.UF_VENDEDOR AND
WH.DIM_LOCATION.BAIRRO = 'INDISPONIVEL' AND
WH.DIM_LOCATION.ENDERECO = 'INDISPONIVEL' AND
WH.DIM_LOCATION.COMPLEMENTO_ENDERECO = 'INDISPONIVEL';