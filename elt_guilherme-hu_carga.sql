-- -----------------------------------------------------------------------------
-- ARQUIVO : etl_guilherme_hu_carga.sql
-- ESCOPO  : Camada de Carga (L) do Processo ETL
-- FLUXO   : Staging Conformado (stg_conf_*) ➔ Data Warehouse (dw)
-- ID DA FROTA DE ORIGEM : 'guilherme-hu'
-- -----------------------------------------------------------------------------

-- =============================================================================
-- 1) FUNÇÕES E PROCEDURES GLOBAIS DO DW (Utilitários Compartilhados)
-- =============================================================================

-- Função auxiliar: lookup de sk_tempo por DATE
DROP FUNCTION IF EXISTS dw.fn_sk_tempo;
DELIMITER //
CREATE FUNCTION dw.fn_sk_tempo(p_data DATE)
RETURNS INT
READS SQL DATA
BEGIN
    DECLARE v_sk INT;
    SELECT sk_tempo INTO v_sk
    FROM   dw.dim_tempo
    WHERE  data = p_data
    LIMIT  1;
    RETURN v_sk;
END//
DELIMITER ;

-- Garante que a data exista na dim_tempo; se não, insere dinamicamente
DROP PROCEDURE IF EXISTS dw.sp_garante_dim_tempo;
DELIMITER //
CREATE PROCEDURE dw.sp_garante_dim_tempo(IN p_data DATE)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dw.dim_tempo WHERE data = p_data) THEN
        INSERT INTO dw.dim_tempo (
            sk_tempo, data, ano, trimestre, mes,
            semana_ano, dia_semana, nome_mes, nome_dia
        )
        VALUES (
            CAST(DATE_FORMAT(p_data, '%Y%m%d') AS SIGNED),
            p_data,
            YEAR(p_data),
            QUARTER(p_data),
            MONTH(p_data),
            WEEK(p_data, 3),
            WEEKDAY(p_data) + 1,
            DATE_FORMAT(p_data, '%M'),
            DATE_FORMAT(p_data, '%W')
        )
        ON DUPLICATE KEY UPDATE sk_tempo = sk_tempo;
    END IF;
END//
DELIMITER ;

-- =============================================================================
-- 2) PROCEDURES DE CARGA — DIMENSÕES
-- =============================================================================

-- 2.1) Endereço (Desnormaliza cidades/estados de pátios e clientes)
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_dim_endereco;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_dim_endereco()
BEGIN
    INSERT IGNORE INTO dw.dim_endereco (cidade, estado, pais)
    SELECT DISTINCT end_cidade, end_uf, end_pais
    FROM staging.stg_conf_patio
    WHERE nk_frota_origem = 'guilherme-hu' AND end_cidade IS NOT NULL;

    INSERT IGNORE INTO dw.dim_endereco (cidade, estado, pais)
    SELECT DISTINCT end_cidade, end_uf, end_pais
    FROM staging.stg_conf_cliente
    WHERE nk_frota_origem = 'guilherme-hu' AND end_cidade IS NOT NULL;
END;

-- 2.2) Pátio
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_dim_patio;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_dim_patio()
BEGIN
    INSERT INTO dw.dim_patio (
        nk_frota_origem, nk_id_patio, nome_patio, capacidade_vagas_patio, sk_endereco
    )
    SELECT
        p.nk_frota_origem, p.nk_id_patio, p.nome_patio, p.capacidade_vagas, e.sk_endereco
    FROM staging.stg_conf_patio p
    LEFT JOIN dw.dim_endereco e
        ON e.cidade = p.end_cidade AND e.estado = p.end_uf AND e.pais = p.end_pais
    WHERE p.nk_frota_origem = 'guilherme-hu'
    ON DUPLICATE KEY UPDATE
        nome_patio = VALUES(nome_patio),
        capacidade_vagas_patio = VALUES(capacidade_vagas_patio),
        sk_endereco = VALUES(sk_endereco);
END;

-- 2.3) Grupo
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_dim_grupo;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_dim_grupo()
BEGIN
    INSERT INTO dw.dim_grupo (
        nk_frota_origem, nk_id_grupo, nome_grupo, valor_diaria
    )
    SELECT nk_frota_origem, nk_id_grupo, nome_grupo, valor_diaria
    FROM staging.stg_conf_grupo
    WHERE nk_frota_origem = 'guilherme-hu'
    ON DUPLICATE KEY UPDATE
        nome_grupo = VALUES(nome_grupo),
        valor_diaria = VALUES(valor_diaria);
END;

-- 2.4) Veículo
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_dim_veiculo;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_dim_veiculo()
BEGIN
    INSERT INTO dw.dim_veiculo (
        nk_frota_origem, nk_id_veiculo, placa, marca, modelo, mecanizacao, tem_ar_condicionado
    )
    SELECT nk_frota_origem, nk_id_veiculo, placa, marca, modelo, mecanizacao, tem_ar_condicionado
    FROM staging.stg_conf_veiculo
    WHERE nk_frota_origem = 'guilherme-hu'
    ON DUPLICATE KEY UPDATE
        placa = VALUES(placa), marca = VALUES(marca), modelo = VALUES(modelo),
        mecanizacao = VALUES(mecanizacao), tem_ar_condicionado = VALUES(tem_ar_condicionado);
END;

-- 2.5) Cliente
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_dim_cliente;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_dim_cliente()
BEGIN
    INSERT INTO dw.dim_cliente (
        nk_frota_origem, nk_id_cliente, tipo_cliente, nome, sk_endereco
    )
    SELECT c.nk_frota_origem, c.nk_id_cliente, c.tipo_cliente, c.nome, e.sk_endereco
    FROM staging.stg_conf_cliente c
    LEFT JOIN dw.dim_endereco e
        ON e.cidade = c.end_cidade AND e.estado = c.end_uf AND e.pais = c.end_pais
    WHERE c.nk_frota_origem = 'guilherme-hu'
    ON DUPLICATE KEY UPDATE
        tipo_cliente = VALUES(tipo_cliente), nome = VALUES(nome), sk_endereco = VALUES(sk_endereco);
END;

-- =============================================================================
-- 3) PROCEDURES DE CARGA — FATOS
-- =============================================================================

-- 3.1) Fato: Inventário de Pátio
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_fato_inventario_patio;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_fato_inventario_patio()
BEGIN
    DECLARE v_data DATE;
    DECLARE v_done INT DEFAULT 0;
    DECLARE cur_datas CURSOR FOR SELECT DISTINCT data_snapshot FROM staging.stg_conf_snapshot_patio WHERE nk_frota_origem = 'guilherme-hu';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    -- Garante datas em dim_tempo
    OPEN cur_datas;
    loop_datas: LOOP
        FETCH cur_datas INTO v_data;
        IF v_done THEN LEAVE loop_datas; END IF;
        CALL dw.sp_garante_dim_tempo(v_data);
    END LOOP;
    CLOSE cur_datas;

    -- Registra rejeitos (SKs não resolvidas)
    INSERT INTO staging.stg_rejeitos_etl (tabela_origem, nk_frota_origem, nk_id_registro, motivo_rejeito)
    SELECT 'stg_conf_snapshot_patio', s.nk_frota_origem, s.nk_id_veiculo, 'SK ausente em Dim_Tempo, Dim_Patio, Dim_Veiculo ou Dim_Grupo'
    FROM staging.stg_conf_snapshot_patio s
    LEFT JOIN dw.dim_patio dp ON dp.nk_frota_origem = s.nk_frota_origem AND dp.nk_id_patio = s.nk_id_patio
    LEFT JOIN dw.dim_veiculo dv ON dv.nk_frota_origem = s.nk_frota_origem AND dv.nk_id_veiculo = s.nk_id_veiculo
    LEFT JOIN dw.dim_grupo dg ON dg.nk_frota_origem = s.nk_frota_origem AND dg.nk_id_grupo = s.nk_id_grupo
    WHERE s.nk_frota_origem = 'guilherme-hu' AND (dw.fn_sk_tempo(s.data_snapshot) IS NULL OR dp.sk_patio IS NULL OR dv.sk_veiculo IS NULL OR dg.sk_grupo IS NULL);

    -- Carga
    INSERT INTO dw.fato_inventario_patio (sk_tempo_referencia, sk_patio, sk_veiculo, sk_grupo, qtde_veiculos_presentes)
    SELECT dw.fn_sk_tempo(s.data_snapshot), dp.sk_patio, dv.sk_veiculo, dg.sk_grupo, 1
    FROM staging.stg_conf_snapshot_patio s
    JOIN dw.dim_patio dp ON dp.nk_frota_origem = s.nk_frota_origem AND dp.nk_id_patio = s.nk_id_patio
    JOIN dw.dim_veiculo dv ON dv.nk_frota_origem = s.nk_frota_origem AND dv.nk_id_veiculo = s.nk_id_veiculo
    JOIN dw.dim_grupo dg ON dg.nk_frota_origem = s.nk_frota_origem AND dg.nk_id_grupo = s.nk_id_grupo
    WHERE s.nk_frota_origem = 'guilherme-hu' AND dw.fn_sk_tempo(s.data_snapshot) IS NOT NULL
    ON DUPLICATE KEY UPDATE sk_grupo = VALUES(sk_grupo), qtde_veiculos_presentes = VALUES(qtde_veiculos_presentes);
END;

-- 3.2) Fato: Locação
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_fato_locacao;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_fato_locacao()
BEGIN
    DECLARE v_data DATE;
    DECLARE v_done INT DEFAULT 0;
    DECLARE cur_datas CURSOR FOR
        SELECT DISTINCT dt FROM (
            SELECT data_retirada AS dt FROM staging.stg_conf_locacao WHERE nk_frota_origem = 'guilherme-hu' AND data_retirada IS NOT NULL
            UNION ALL SELECT data_prev_devolucao FROM staging.stg_conf_locacao WHERE nk_frota_origem = 'guilherme-hu' AND data_prev_devolucao IS NOT NULL
            UNION ALL SELECT data_real_devolucao FROM staging.stg_conf_locacao WHERE nk_frota_origem = 'guilherme-hu' AND data_real_devolucao IS NOT NULL
        ) all_dates;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur_datas; 
    loop_datas: LOOP
        FETCH cur_datas INTO v_data;
        IF v_done THEN LEAVE loop_datas; END IF;
        CALL dw.sp_garante_dim_tempo(v_data);
    END LOOP;
    CLOSE cur_datas;

    -- Rejeitos
    INSERT INTO staging.stg_rejeitos_etl (tabela_origem, nk_frota_origem, nk_id_registro, motivo_rejeito)
    SELECT 'stg_conf_locacao', l.nk_frota_origem, l.nk_id_locacao, 'Problema na resolução de Surrogate Keys (Dimensões incompletas)'
    FROM staging.stg_conf_locacao l
    LEFT JOIN dw.dim_cliente dc ON dc.nk_frota_origem = l.nk_frota_origem AND dc.nk_id_cliente = l.nk_id_cliente
    LEFT JOIN dw.dim_veiculo dv ON dv.nk_frota_origem = l.nk_frota_origem AND dv.nk_id_veiculo = l.nk_id_veiculo
    LEFT JOIN dw.dim_grupo dg ON dg.nk_frota_origem = l.nk_frota_origem AND dg.nk_id_grupo = l.nk_id_grupo
    LEFT JOIN dw.dim_patio dp_ret ON dp_ret.nk_frota_origem = l.nk_frota_origem AND dp_ret.nk_id_patio = l.nk_id_patio_retirada
    WHERE l.nk_frota_origem = 'guilherme-hu' AND (dw.fn_sk_tempo(l.data_retirada) IS NULL OR dc.sk_cliente IS NULL OR dv.sk_veiculo IS NULL OR dg.sk_grupo IS NULL OR dp_ret.sk_patio IS NULL);

    -- Carga
    INSERT INTO dw.fato_locacao (nk_frota_origem, nk_id_locacao, sk_tempo_retirada, sk_tempo_prev_devolucao, sk_tempo_real_devolucao, sk_cliente, sk_veiculo, sk_grupo, sk_patio_retirada, sk_patio_devolucao_real, valor_final, qtde_locacoes)
    SELECT l.nk_frota_origem, l.nk_id_locacao, dw.fn_sk_tempo(l.data_retirada), dw.fn_sk_tempo(l.data_prev_devolucao), dw.fn_sk_tempo(l.data_real_devolucao), dc.sk_cliente, dv.sk_veiculo, dg.sk_grupo, dp_ret.sk_patio, dp_dev.sk_patio, l.valor_final, 1
    FROM staging.stg_conf_locacao l
    JOIN dw.dim_cliente dc ON dc.nk_frota_origem = l.nk_frota_origem AND dc.nk_id_cliente = l.nk_id_cliente
    JOIN dw.dim_veiculo dv ON dv.nk_frota_origem = l.nk_frota_origem AND dv.nk_id_veiculo = l.nk_id_veiculo
    JOIN dw.dim_grupo dg ON dg.nk_frota_origem = l.nk_frota_origem AND dg.nk_id_grupo = l.nk_id_grupo
    JOIN dw.dim_patio dp_ret ON dp_ret.nk_frota_origem = l.nk_frota_origem AND dp_ret.nk_id_patio = l.nk_id_patio_retirada
    LEFT JOIN dw.dim_patio dp_dev ON dp_dev.nk_frota_origem = l.nk_frota_origem AND dp_dev.nk_id_patio = l.nk_id_patio_devolucao
    WHERE l.nk_frota_origem = 'guilherme-hu' AND dw.fn_sk_tempo(l.data_retirada) IS NOT NULL
    ON DUPLICATE KEY UPDATE sk_tempo_real_devolucao = VALUES(sk_tempo_real_devolucao), sk_patio_devolucao_real = VALUES(sk_patio_devolucao_real), valor_final = VALUES(valor_final);
END;

-- 3.3) Fato: Reserva
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_fato_reserva;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_fato_reserva()
BEGIN
    DECLARE v_data DATE;
    DECLARE v_done INT DEFAULT 0;
    DECLARE cur_datas CURSOR FOR
        SELECT DISTINCT dt FROM (
            SELECT data_reserva AS dt FROM staging.stg_conf_reserva WHERE nk_frota_origem = 'guilherme-hu' AND data_reserva IS NOT NULL
            UNION ALL SELECT data_retirada_prevista FROM staging.stg_conf_reserva WHERE nk_frota_origem = 'guilherme-hu' AND data_retirada_prevista IS NOT NULL
            UNION ALL SELECT data_devolucao_prevista FROM staging.stg_conf_reserva WHERE nk_frota_origem = 'guilherme-hu' AND data_devolucao_prevista IS NOT NULL
        ) all_dates;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur_datas;
    loop_datas: LOOP
        FETCH cur_datas INTO v_data;
        IF v_done THEN LEAVE loop_datas; END IF;
        CALL dw.sp_garante_dim_tempo(v_data);
    END LOOP;
    CLOSE cur_datas;

    -- Rejeitos
    INSERT INTO staging.stg_rejeitos_etl (tabela_origem, nk_frota_origem, nk_id_registro, motivo_rejeito)
    SELECT 'stg_conf_reserva', r.nk_frota_origem, r.nk_id_reserva, 'Problema na resolução de Surrogate Keys (Dimensões incompletas)'
    FROM staging.stg_conf_reserva r
    LEFT JOIN dw.dim_cliente dc ON dc.nk_frota_origem = r.nk_frota_origem AND dc.nk_id_cliente = r.nk_id_cliente
    LEFT JOIN dw.dim_grupo dg ON dg.nk_frota_origem = r.nk_frota_origem AND dg.nk_id_grupo = r.nk_id_grupo
    LEFT JOIN dw.dim_patio dp_ret ON dp_ret.nk_frota_origem = r.nk_frota_origem AND dp_ret.nk_id_patio = r.nk_id_patio_retirada
    LEFT JOIN dw.dim_patio dp_fim ON dp_fim.nk_frota_origem = r.nk_frota_origem AND dp_fim.nk_id_patio = r.nk_id_patio_fim
    WHERE r.nk_frota_origem = 'guilherme-hu' AND (dw.fn_sk_tempo(r.data_reserva) IS NULL OR dc.sk_cliente IS NULL OR dg.sk_grupo IS NULL OR dp_ret.sk_patio IS NULL OR dp_fim.sk_patio IS NULL);

    -- Carga
    INSERT INTO dw.fato_reserva (nk_frota_origem, nk_id_reserva, sk_tempo_reserva, sk_tempo_prev_retirada, sk_tempo_prev_devolucao, sk_cliente, sk_grupo, sk_patio_retirada, sk_patio_fim, duracao_prevista_dias, valor_previsto_reserva, dd_status_reserva, qtde_reservas)
    SELECT r.nk_frota_origem, r.nk_id_reserva, dw.fn_sk_tempo(r.data_reserva), dw.fn_sk_tempo(r.data_retirada_prevista), dw.fn_sk_tempo(r.data_devolucao_prevista), dc.sk_cliente, dg.sk_grupo, dp_ret.sk_patio, dp_fim.sk_patio, r.duracao_prevista_dias, r.valor_previsto_reserva, r.status_reserva, 1
    FROM staging.stg_conf_reserva r
    JOIN dw.dim_cliente dc ON dc.nk_frota_origem = r.nk_frota_origem AND dc.nk_id_cliente = r.nk_id_cliente
    JOIN dw.dim_grupo dg ON dg.nk_frota_origem = r.nk_frota_origem AND dg.nk_id_grupo = r.nk_id_grupo
    JOIN dw.dim_patio dp_ret ON dp_ret.nk_frota_origem = r.nk_frota_origem AND dp_ret.nk_id_patio = r.nk_id_patio_retirada
    JOIN dw.dim_patio dp_fim ON dp_fim.nk_frota_origem = r.nk_frota_origem AND dp_fim.nk_id_patio = r.nk_id_patio_fim
    WHERE r.nk_frota_origem = 'guilherme-hu' AND dw.fn_sk_tempo(r.data_reserva) IS NOT NULL
    ON DUPLICATE KEY UPDATE dd_status_reserva = VALUES(dd_status_reserva), valor_previsto_reserva = VALUES(valor_previsto_reserva), duracao_prevista_dias = VALUES(duracao_prevista_dias);
END;

-- =============================================================================
-- 4) PROCEDURE MAIN DE CARGA
-- =============================================================================
DROP PROCEDURE IF EXISTS dw.sp_guilherme_hu_carga_completa;
CREATE PROCEDURE dw.sp_guilherme_hu_carga_completa()
BEGIN
    -- Dimensões carregadas primeiro para integridade referencial
    CALL dw.sp_guilherme_hu_carga_dim_endereco();
    CALL dw.sp_guilherme_hu_carga_dim_patio();
    CALL dw.sp_guilherme_hu_carga_dim_grupo();
    CALL dw.sp_guilherme_hu_carga_dim_veiculo();
    CALL dw.sp_guilherme_hu_carga_dim_cliente();

    -- Fatos na sequência
    CALL dw.sp_guilherme_hu_carga_fato_inventario_patio();
    CALL dw.sp_guilherme_hu_carga_fato_locacao();
    CALL dw.sp_guilherme_hu_carga_fato_reserva();
END;

-- =============================================================================
-- 5) EXEMPLO DE EXECUÇÃO
-- =============================================================================
/*
  -- Carga full (Primeira vez para a frota guilherme-hu):
  CALL staging.sp_guilherme_hu_extracao_completa(TRUE);
  CALL staging.sp_guilherme_hu_transformacao_completa();
  CALL dw.sp_guilherme_hu_carga_completa();

  -- Se for usar de forma orquestrada/agendada, apenas chamar:
  CALL dw.sp_guilherme_hu_carga_completa();
*/
