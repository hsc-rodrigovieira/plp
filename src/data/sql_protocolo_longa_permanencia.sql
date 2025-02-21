-- #######################################################################
-- # CONSULTA LISTA DE PACIENTES 
-- #######################################################################

-- CONTABILIZA AS OPERAÇÕES PENDENTES
SELECT
  tp_operacao
  ,Count(cd_paciente) qtd
FROM
( -- RECUPERA OS PACIENTES INTERNADOS DENTRO DO PERÍODO DE RISCO-PROTOCOLO OU COM ALTA NO DIA ANTERIOR
  SELECT
    a.cd_paciente
    ,CASE
      WHEN cd_categoria IS NULL AND dt_alta IS NULL AND Round(SYSDATE - hr_atendimento) >= :dias_risco THEN 'I'
      WHEN cd_categoria = 46 AND Round(SYSDATE - hr_atendimento) >= :dias_protocolo THEN 'U'
      WHEN cd_categoria IS NOT NULL AND dt_alta IS NOT NULL THEN 'D'
      WHEN cd_categoria NOT IN (46,47) AND dt_alta IS NULL THEN 'A'
      ELSE 'NA'
    END tp_operacao
  FROM atendime a
    LEFT JOIN categoria_paciente cp on a.cd_paciente = cp.cd_paciente
  WHERE 1=1
    AND tp_atendimento = 'I'
    AND (dt_alta IS NULL OR Trunc(dt_alta) = Trunc(SYSDATE-1))
    AND Round(SYSDATE - hr_atendimento) >= :dias_risco
) GROUP BY tp_operacao
/
-- #######################################################################
-- # REMOVE MARCAÇÃO NA ALTA
-- #######################################################################

-- REGISTRA REMOÇÃO DA CATEGORIA DOS PACIENTES COM ALTA NO DIA ANTERIOR
INSERT ALL
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, cd_categoria, hr_alta    , 'DBAMV'   , 'EXC'      , cd_paciente)
SELECT a.cd_paciente, cp.cd_categoria, a.hr_alta
FROM atendime a
  INNER JOIN categoria_paciente cp ON a.cd_paciente = cp.cd_paciente AND cp.cd_categoria IN (46,47)
WHERE 1=1
  AND a.tp_atendimento = 'I'
  AND Trunc(dt_alta) = Trunc(SYSDATE)-1
;  
-- EXCLUI OS PACIENTES COM ALTA NO DIA ANTERIOR
DELETE
FROM categoria_paciente cp
WHERE cd_categoria IN (46,47)
  AND EXISTS (SELECT 1
              FROM atendime a
              WHERE 1=1
                AND tp_atendimento = 'I'
                AND Trunc(dt_alta) = Trunc(SYSDATE)-1
                AND a.cd_paciente = cp.cd_paciente)
/
-- #######################################################################
-- # INSERE MARCAÇÃO
-- #######################################################################
INSERT ALL
INTO categoria_paciente (cd_categoria_paciente         , cd_categoria, cd_paciente)
                 VALUES (seq_categoria_paciente.NEXTVAL, cd_categoria, cd_paciente)
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, cd_categoria, SYSDATE    , 'DBAMV'   , 'INC'      , cd_paciente)
SELECT
  cd_paciente
  ,CASE
    WHEN Round(SYSDATE - hr_atendimento) BETWEEN :dias_risco AND (:dias_protocolo-1) THEN 46
    WHEN Round(SYSDATE - hr_atendimento) >= :dias_protocolo THEN 47
  END cd_categoria
FROM atendime a  
WHERE 1=1
  AND a.tp_atendimento = 'I'
  AND a.dt_alta IS NULL
  AND Round(SYSDATE - a.hr_atendimento) >= :dias_risco
  AND NOT EXISTS (SELECT 1
                  FROM categoria_paciente cp 
                  WHERE 1=1                    
                    AND a.cd_paciente = cp.cd_paciente)
/
-- #######################################################################
-- # ALTERA DE RISCO PARA PERMANÊNCIA
-- #######################################################################

-- REGISTRA REMOÇÃO DA CATEGORIA DOS PACIENTES COM REGISTRO DE RISCO
INSERT ALL
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, 46          , SYSDATE    , 'DBAMV'   , 'EXC'      , cd_paciente)
SELECT a.cd_paciente
FROM atendime a
WHERE 1=1
  AND a.tp_atendimento = 'I'
  AND a.dt_alta IS NULL
  AND Round(SYSDATE - hr_atendimento) >= :dias_protocolo
  AND EXISTS (SELECT 1
              FROM categoria_paciente cp
              WHERE 1=1
                AND cp.cd_categoria = 46
                AND a.cd_paciente = cp.cd_paciente)
;
-- EXCLUI OS PACIENTES COM REGISTRO DE RISCO
DELETE
FROM categoria_paciente cp
WHERE cd_categoria = 46
  AND EXISTS (SELECT 1
              FROM atendime a
              WHERE 1=1
                AND tp_atendimento = 'I'
                AND Round(SYSDATE - hr_atendimento) >= :dias_protocolo
                AND a.cd_paciente = cp.cd_paciente)
;
-- INSERE MARCAÇÃO
INSERT ALL
INTO categoria_paciente (cd_categoria_paciente         , cd_categoria, cd_paciente)
                 VALUES (seq_categoria_paciente.NEXTVAL, 47          , cd_paciente)
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, 47          , SYSDATE    , 'DBAMV'   , 'INC'      , cd_paciente)
SELECT a.cd_paciente
FROM atendime a
WHERE 1=1
  AND a.tp_atendimento = 'I'
  AND a.dt_alta IS NULL
  AND Round(SYSDATE - hr_atendimento) >= :dias_protocolo
  AND NOT EXISTS (SELECT 1
                  FROM categoria_paciente cp
                  WHERE 1=1
                    AND cp.cd_categoria = 47
                    AND a.cd_paciente = cp.cd_paciente)
/
-- #######################################################################
-- # ALTERA CATEGORIA ANTIGA EM ABERTO
-- #######################################################################

-- REGISTRA REMOÇÃO DA CATEGORIA DOS PACIENTES COM REGISTRO DE RISCO
INSERT ALL
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, cd_categoria, SYSDATE    , 'DBAMV'   , 'EXC'      , cd_paciente)
SELECT cp.cd_paciente, cp.cd_categoria
FROM categoria_paciente cp
WHERE 1=1
  AND cp.cd_categoria NOT IN (46,47)
  AND EXISTS (SELECT 1
              FROM atendime a
              WHERE 1=1
                AND a.tp_atendimento = 'I'
                AND a.dt_alta IS NULL
                AND Round(SYSDATE - a.hr_atendimento) >= :dias_risco
                AND cp.cd_paciente = a.cd_paciente)
;
-- EXCLUI OS PACIENTES COM REGISTRO DE RISCO
DELETE
FROM categoria_paciente cp
WHERE 1=1
  AND cp.cd_categoria NOT IN (46,47)
  AND EXISTS (SELECT 1
              FROM atendime a
              WHERE 1=1
                AND tp_atendimento = 'I'
                AND a.dt_alta IS NULL
                AND Round(SYSDATE - hr_atendimento) >= :dias_risco
                AND a.cd_paciente = cp.cd_paciente)
;
-- INSERE MARCAÇÃO
INSERT ALL
INTO categoria_paciente (cd_categoria_paciente         , cd_categoria, cd_paciente)
                 VALUES (seq_categoria_paciente.NEXTVAL, cd_categoria, cd_paciente)
INTO log_categoria (cd_log_categoria         , cd_categoria, dh_operacao, cd_usuario, tp_operacao, cd_paciente)
            VALUES (seq_log_categoria.NEXTVAL, cd_categoria, SYSDATE    , 'DBAMV'   , 'INC'      , cd_paciente)
SELECT
  a.cd_paciente
  ,CASE
    WHEN Round(SYSDATE - a.hr_atendimento) BETWEEN :dias_risco AND (:dias_protocolo-1) THEN 46
    WHEN Round(SYSDATE - a.hr_atendimento) >= :dias_protocolo THEN 47
  END cd_categoria
FROM atendime a
WHERE 1=1
  AND a.tp_atendimento = 'I'
  AND a.dt_alta IS NULL
  AND Round(SYSDATE - hr_atendimento) >= :dias_risco
  AND NOT EXISTS (SELECT 1
                  FROM categoria_paciente cp
                  WHERE 1=1
                    AND cp.cd_categoria IN (46,47)
                    AND a.cd_paciente = cp.cd_paciente)