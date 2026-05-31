-- -----------------------------------------------------------------------------
-- ARQUIVO : etl_guilherme-hu_extracao.sql
-- ESCOPO  : Camada de Extração (E) do Processo ETL
-- FLUXO   : Banco Operacional (OLTP: locadora) ➔ Área de Preparação (Staging)
-- -----------------------------------------------------------------------------
-- ID DA FROTA DE ORIGEM : 'guilherme-hu'
-- SCHEMA FONTE          : locadora
-- SCHEMA DESTINO        : staging (compartilhado para conformidade do DW)
-- -----------------------------------------------------------------------------
-- ARQUITETURA E ESTRATÉGIA DE CAPTURA:
--   • Abordagem Híbrida: Implementação orientada a eventos (Event-Driven) via 
--     Triggers para capturar modificações em tempo real, combinada com Stored 
--     Procedures para cargas completas (Batch) ou recuperações controladas.
--   • Rastreabilidade: Uso obrigatório do metadado 'dt_extracao' (timestamp local) 
--     e da chave natural 'nk_frota_origem' para garantir a auditoria e o isolamento 
--     dos dados de origem no Data Warehouse.
--
-- DIRETRIZES DE MAPEAMENTO (OLTP ➔ STAGING):
--   • Localização : Vagas e Pátios traduzidos na capacidade física dos pontos de venda.
--   • Veículos    : Fusão de dados de frota com especificações de engenharia (câmbio e ar).
--   • Clientes    : Unificação cadastral desnormalizando dados geográficos e documentos.
-- -----------------------------------------------------------------------------

-- =============================================================================
-- SCRIPT DE EXTRAÇÃO (ETL - EXTRACT) - ORIGEM: GUILHERME-HU
-- Mapeamento do Modelo Físico (OLTP) para a Staging Area
-- =============================================================================

-- Marca o momento desta extração para uso nos metadados
SET @extracao_ts = NOW();

-- Criação do schema de staging, caso não exista
CREATE SCHEMA IF NOT EXISTS staging;

-- =============================================================================
-- 1) STAGING: Criação das tabelas
-- =============================================================================

-- 1.1) stg_guilherme_hu_patio
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_patio (
    nk_frota_origem       VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_patio           INT          NOT NULL,
    nome_patio            VARCHAR(150),
    capacidade_vagas      INT,
    end_cidade            VARCHAR(100),
    end_uf                CHAR(2),
    end_logradouro        VARCHAR(150),
    dt_extracao           DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_patio)
);

-- 1.2) stg_guilherme_hu_grupo
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_grupo (
    nk_frota_origem       VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_grupo           INT          NOT NULL,
    nome_grupo            VARCHAR(100),
    codigo_grupo          VARCHAR(10),
    classe_luxo           VARCHAR(30),
    valor_diaria          DECIMAL(12,2),
    dt_extracao           DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_grupo)
);

-- 1.3) stg_guilherme_hu_veiculo
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_veiculo (
    nk_frota_origem       VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_veiculo         INT          NOT NULL,
    nk_id_grupo           INT,
    nk_id_patio_origem    INT,
    placa                 VARCHAR(10),
    marca                 VARCHAR(60),
    modelo                VARCHAR(60),
    versao                VARCHAR(60),
    mecanizacao           VARCHAR(20),
    tem_ar_condicionado   TINYINT(1),
    ano_fabricacao        INT,
    situacao              VARCHAR(20),
    dt_extracao           DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_veiculo)
);

-- 1.4) stg_guilherme_hu_cliente
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_cliente (
    nk_frota_origem       VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_cliente         INT          NOT NULL,
    tipo_cliente          VARCHAR(2),
    nome                  VARCHAR(200),
    email                 VARCHAR(150),
    cidade_origem         VARCHAR(100),
    end_uf                CHAR(2),
    end_cidade            VARCHAR(100),
    cpf                   VARCHAR(11),
    cnpj                  VARCHAR(14),
    nome_fantasia         VARCHAR(150),
    dt_extracao           DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_cliente)
);

-- 1.5) stg_guilherme_hu_reserva
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_reserva (
    nk_frota_origem           VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_reserva             INT          NOT NULL,
    nk_id_cliente             INT,
    nk_id_grupo               INT,
    nk_id_patio_retirada      INT,
    nk_id_patio_fim           INT,
    data_reserva              DATE,
    data_retirada_prevista    DATE,
    data_devolucao_prevista   DATE,
    duracao_prevista_dias     INT,
    valor_previsto_reserva    DECIMAL(12,2),
    status_reserva            VARCHAR(30),
    dt_extracao               DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_reserva)
);

-- 1.6) stg_guilherme_hu_locacao
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_locacao (
    nk_frota_origem           VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_locacao             INT          NOT NULL,
    nk_id_cliente             INT,
    nk_id_veiculo             INT,
    nk_id_grupo               INT,
    nk_id_patio_retirada      INT,
    nk_id_patio_devolucao     INT,
    data_retirada             DATE,
    data_prev_devolucao       DATE,
    data_real_devolucao       DATE,
    valor_diaria_aplicada     DECIMAL(12,2),
    valor_final               DECIMAL(14,2),
    dt_extracao               DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_locacao)
);

-- 1.7) stg_guilherme_hu_snapshot_patio
CREATE TABLE IF NOT EXISTS staging.stg_guilherme_hu_snapshot_patio (
    nk_frota_origem       VARCHAR(30)  NOT NULL DEFAULT 'guilherme-hu',
    nk_id_patio           INT          NOT NULL,
    nk_id_veiculo         INT          NOT NULL,
    nk_id_grupo           INT,
    data_snapshot         DATE         NOT NULL,
    dt_extracao           DATETIME     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nk_frota_origem, nk_id_patio, nk_id_veiculo, data_snapshot)
);

-- Função auxiliar para corte incremental
DROP FUNCTION IF EXISTS staging.fn_guilherme_hu_corte_incremental;
CREATE FUNCTION staging.fn_guilherme_hu_corte_incremental()
RETURNS DATETIME
READS SQL DATA
BEGIN
    RETURN COALESCE(@ultima_extracao, '1900-01-01 00:00:00');
END;

-- =============================================================================
-- 2) PROCEDURES DE EXTRAÇÃO
-- =============================================================================

-- 2.1) sp_guilherme_hu_extrai_patio
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_patio;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_patio()
BEGIN
    TRUNCATE TABLE staging.stg_guilherme_hu_patio;
    INSERT INTO staging.stg_guilherme_hu_patio (
        nk_frota_origem, nk_id_patio, nome_patio, capacidade_vagas, end_cidade, end_uf, end_logradouro, dt_extracao
    )
    SELECT
        'guilherme-hu', p.Id_patio, p.Nome_patio, 
        (SELECT COUNT(*) FROM locadora.Vaga v WHERE v.Id_patio = p.Id_patio) AS capacidade_vagas,
        e.Cidade, e.UF, e.Rua_Avenida, NOW()
    FROM locadora.Patio p
    JOIN locadora.Endereco e ON p.Id_endereco = e.Id_endereco;
END;

-- 2.2) sp_guilherme_hu_extrai_grupo
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_grupo;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_grupo()
BEGIN
    TRUNCATE TABLE staging.stg_guilherme_hu_grupo;
    INSERT INTO staging.stg_guilherme_hu_grupo (
        nk_frota_origem, nk_id_grupo, nome_grupo, codigo_grupo, classe_luxo, valor_diaria, dt_extracao
    )
    SELECT
        'guilherme-hu', g.Id_grupo, g.Nome, SUBSTRING(g.Nome, 1, 3), 'N/A', g.Diaria_grupo, NOW()
    FROM locadora.Grupo g;
END;

-- 2.3) sp_guilherme_hu_extrai_veiculo
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_veiculo;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_veiculo()
BEGIN
    TRUNCATE TABLE staging.stg_guilherme_hu_veiculo;
    INSERT INTO staging.stg_guilherme_hu_veiculo (
        nk_frota_origem, nk_id_veiculo, nk_id_grupo, nk_id_patio_origem, placa, marca, modelo, versao, mecanizacao, tem_ar_condicionado, ano_fabricacao, situacao, dt_extracao
    )
    SELECT
        'guilherme-hu', v.Id_veiculo, v.Id_grupo, 
        (SELECT Id_patio FROM locadora.Vaga vg WHERE vg.Id_veiculo = v.Id_veiculo LIMIT 1),
        v.Placa, v.Marca, v.Modelo, v.Versao,
        IF(ec.Direcao_automatica = 1, 'AUTOMATICO', 'MANUAL'),
        ec.Ar_condicionado, CAST(v.Ano AS UNSIGNED), 'ATIVO', NOW()
    FROM locadora.Veiculo v
    JOIN locadora.Especificacoes_const ec ON v.Id_spec_const = ec.Id_spec_const;
END;

-- 2.4) sp_guilherme_hu_extrai_cliente
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_cliente;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_cliente()
BEGIN
    -- O OLTP não tem PJ/PF explícito, assumimos tudo PF conforme o design da origem e usamos UPSERT
    INSERT INTO staging.stg_guilherme_hu_cliente (
        nk_frota_origem, nk_id_cliente, tipo_cliente, nome, email, cidade_origem, end_uf, end_cidade, cpf, cnpj, nome_fantasia, dt_extracao
    )
    SELECT
        'guilherme-hu', c.Id_cliente, 'PF', c.Nome_completo, c.Email, e.Cidade, e.UF, e.Cidade, dc.CPF, NULL, NULL, NOW()
    FROM locadora.Cliente c
    JOIN locadora.Endereco e ON c.Id_endereco = e.Id_endereco
    JOIN locadora.Documento_cliente dc ON c.Id_documento = dc.Id_documento
    ON DUPLICATE KEY UPDATE
        nome = VALUES(nome), email = VALUES(email), dt_extracao = VALUES(dt_extracao);
END;

-- 2.5) sp_guilherme_hu_extrai_reserva
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_reserva;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_reserva()
BEGIN
    INSERT INTO staging.stg_guilherme_hu_reserva (
        nk_frota_origem, nk_id_reserva, nk_id_cliente, nk_id_grupo, nk_id_patio_retirada, nk_id_patio_fim, data_reserva, data_retirada_prevista, data_devolucao_prevista, duracao_prevista_dias, valor_previsto_reserva, status_reserva, dt_extracao
    )
    SELECT
        'guilherme-hu', r.Id_reserva, r.Id_cliente, r.Id_grupo, r.Id_patio_origem, r.Id_patio_fim,
        r.Data_reserva, DATE(r.Data_inicio_combinada), DATE(r.Data_fim_combinada),
        DATEDIFF(DATE(r.Data_fim_combinada), DATE(r.Data_inicio_combinada)),
        r.Preco_final,
        CASE r.Estado_reserva
            WHEN 0 THEN 'ATIVA'
            WHEN 1 THEN 'CANCELADA'
            WHEN 2 THEN 'CONVERTIDA'
            ELSE 'ATIVA'
        END, NOW()
    FROM locadora.Reserva r
    ON DUPLICATE KEY UPDATE
        status_reserva = VALUES(status_reserva), valor_previsto_reserva = VALUES(valor_previsto_reserva), dt_extracao = VALUES(dt_extracao);
END;

-- 2.6) sp_guilherme_hu_extrai_locacao
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_locacao;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_locacao()
BEGIN
    INSERT INTO staging.stg_guilherme_hu_locacao (
        nk_frota_origem, nk_id_locacao, nk_id_cliente, nk_id_veiculo, nk_id_grupo, nk_id_patio_retirada, nk_id_patio_devolucao, data_retirada, data_prev_devolucao, data_real_devolucao, valor_diaria_aplicada, valor_final, dt_extracao
    )
    SELECT
        'guilherme-hu', l.Id_locacao, r.Id_cliente, l.Id_veiculo, r.Id_grupo, l.Id_patio,
        (SELECT p.Id_patio FROM locadora.Devolucao d JOIN locadora.Vaga vg ON d.Id_vaga = vg.Id_vaga JOIN locadora.Patio p ON vg.Id_patio = p.Id_patio WHERE d.Id_locacao = l.Id_locacao LIMIT 1),
        DATE(l.Data_locacao), DATE(r.Data_fim_combinada),
        (SELECT DATE(d.Data_devolucao) FROM locadora.Devolucao d WHERE d.Id_locacao = l.Id_locacao LIMIT 1),
        g.Diaria_grupo, r.Preco_final, NOW()
    FROM locadora.Locacao l
    JOIN locadora.Reserva r ON l.Id_reserva = r.Id_reserva
    JOIN locadora.Grupo g ON r.Id_grupo = g.Id_grupo
    ON DUPLICATE KEY UPDATE
        nk_id_patio_devolucao = VALUES(nk_id_patio_devolucao), data_real_devolucao = VALUES(data_real_devolucao), valor_final = VALUES(valor_final), dt_extracao = VALUES(dt_extracao);
END;

-- 2.7) sp_guilherme_hu_extrai_snapshot_patio
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extrai_snapshot_patio;
CREATE PROCEDURE staging.sp_guilherme_hu_extrai_snapshot_patio(IN p_data_snapshot DATE)
BEGIN
    DECLARE v_data DATE;
    SET v_data = COALESCE(p_data_snapshot, DATE_SUB(CURDATE(), INTERVAL 1 DAY));

    DELETE FROM staging.stg_guilherme_hu_snapshot_patio WHERE data_snapshot = v_data AND nk_frota_origem = 'guilherme-hu';

    INSERT INTO staging.stg_guilherme_hu_snapshot_patio (
        nk_frota_origem, nk_id_patio, nk_id_veiculo, nk_id_grupo, data_snapshot, dt_extracao
    )
    SELECT 'guilherme-hu', vg.Id_patio, v.Id_veiculo, v.Id_grupo, v_data, NOW()
    FROM locadora.Veiculo v
    JOIN locadora.Vaga vg ON v.Id_veiculo = vg.Id_veiculo;
END;

-- =============================================================================
-- 3) PROCEDURE MAIN DE EXTRAÇÃO
-- =============================================================================
-- Chama todas as extrações na ordem correta
DROP PROCEDURE IF EXISTS staging.sp_guilherme_hu_extracao_completa;
CREATE PROCEDURE staging.sp_guilherme_hu_extracao_completa(IN p_full_load TINYINT(1))
BEGIN
    IF p_full_load THEN SET @ultima_extracao = '1900-01-01 00:00:00'; END IF;
    
    -- Extração de Dimensões
    CALL staging.sp_guilherme_hu_extrai_patio();
    CALL staging.sp_guilherme_hu_extrai_grupo();
    CALL staging.sp_guilherme_hu_extrai_veiculo();
    CALL staging.sp_guilherme_hu_extrai_cliente();
    
    -- Extração de Fatos e Snapshots
    CALL staging.sp_guilherme_hu_extrai_reserva();
    CALL staging.sp_guilherme_hu_extrai_locacao();
    CALL staging.sp_guilherme_hu_extrai_snapshot_patio(NULL);
END;

-- =============================================================================
-- 4) TRIGGERS DE EXTRAÇÃO (Event-Driven)
-- =============================================================================
-- Mantém a staging area atualizada em tempo real com eventos do banco locadora

DELIMITER //

-- 4.1) Pátio
DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_patio_ai//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_patio_ai AFTER INSERT ON locadora.Patio FOR EACH ROW
BEGIN
    INSERT INTO staging.stg_guilherme_hu_patio (nk_frota_origem, nk_id_patio, nome_patio, end_cidade, dt_extracao)
    VALUES ('guilherme-hu', NEW.Id_patio, NEW.Nome_patio, (SELECT Cidade FROM locadora.Endereco WHERE Id_endereco = NEW.Id_endereco), NOW())
    ON DUPLICATE KEY UPDATE nome_patio = VALUES(nome_patio), end_cidade = VALUES(end_cidade), dt_extracao = NOW();
END//

-- 4.2) Cliente
DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_cliente_ai//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_cliente_ai AFTER INSERT ON locadora.Cliente FOR EACH ROW
BEGIN
    INSERT INTO staging.stg_guilherme_hu_cliente (nk_frota_origem, nk_id_cliente, tipo_cliente, nome, email, end_uf, end_cidade, dt_extracao)
    VALUES ('guilherme-hu', NEW.Id_cliente, 'PF', NEW.Nome_completo, NEW.Email, (SELECT UF FROM locadora.Endereco WHERE Id_endereco = NEW.Id_endereco), (SELECT Cidade FROM locadora.Endereco WHERE Id_endereco = NEW.Id_endereco), NOW())
    ON DUPLICATE KEY UPDATE nome = VALUES(nome), email = VALUES(email), dt_extracao = NOW();
END//

-- 4.3) Reserva
DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_reserva_ai//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_reserva_ai AFTER INSERT ON locadora.Reserva FOR EACH ROW
BEGIN
    INSERT INTO staging.stg_guilherme_hu_reserva (nk_frota_origem, nk_id_reserva, nk_id_cliente, nk_id_grupo, nk_id_patio_retirada, nk_id_patio_fim, data_reserva, data_retirada_prevista, data_devolucao_prevista, duracao_prevista_dias, valor_previsto_reserva, status_reserva, dt_extracao)
    VALUES ('guilherme-hu', NEW.Id_reserva, NEW.Id_cliente, NEW.Id_grupo, NEW.Id_patio_origem, NEW.Id_patio_fim, NEW.Data_reserva, DATE(NEW.Data_inicio_combinada), DATE(NEW.Data_fim_combinada), DATEDIFF(DATE(NEW.Data_fim_combinada), DATE(NEW.Data_inicio_combinada)), NEW.Preco_final, CASE NEW.Estado_reserva WHEN 0 THEN 'ATIVA' WHEN 1 THEN 'CANCELADA' WHEN 2 THEN 'CONVERTIDA' ELSE 'ATIVA' END, NOW())
    ON DUPLICATE KEY UPDATE status_reserva = VALUES(status_reserva), valor_previsto_reserva = VALUES(valor_previsto_reserva), dt_extracao = NOW();
END//

DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_reserva_au//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_reserva_au AFTER UPDATE ON locadora.Reserva FOR EACH ROW
BEGIN
    UPDATE staging.stg_guilherme_hu_reserva
    SET status_reserva = CASE NEW.Estado_reserva WHEN 0 THEN 'ATIVA' WHEN 1 THEN 'CANCELADA' WHEN 2 THEN 'CONVERTIDA' ELSE 'ATIVA' END, valor_previsto_reserva = NEW.Preco_final, dt_extracao = NOW()
    WHERE nk_id_reserva = NEW.Id_reserva AND nk_frota_origem = 'guilherme-hu';
END//

-- 4.4) Locacao
DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_locacao_ai//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_locacao_ai AFTER INSERT ON locadora.Locacao FOR EACH ROW
BEGIN
    INSERT INTO staging.stg_guilherme_hu_locacao (nk_frota_origem, nk_id_locacao, nk_id_cliente, nk_id_veiculo, nk_id_grupo, nk_id_patio_retirada, data_retirada, dt_extracao)
    VALUES ('guilherme-hu', NEW.Id_locacao, (SELECT Id_cliente FROM locadora.Reserva WHERE Id_reserva = NEW.Id_reserva), NEW.Id_veiculo, (SELECT Id_grupo FROM locadora.Reserva WHERE Id_reserva = NEW.Id_reserva), NEW.Id_patio, DATE(NEW.Data_locacao), NOW())
    ON DUPLICATE KEY UPDATE dt_extracao = NOW();
END//

-- 4.5) Devolução (Atualiza a Locação e Finaliza o Ciclo)
DROP TRIGGER IF EXISTS locadora.trg_extrai_guilherme_hu_devolucao_ai//
CREATE TRIGGER locadora.trg_extrai_guilherme_hu_devolucao_ai AFTER INSERT ON locadora.Devolucao FOR EACH ROW
BEGIN
    UPDATE staging.stg_guilherme_hu_locacao
    SET data_real_devolucao = DATE(NEW.Data_devolucao), nk_id_patio_devolucao = (SELECT p.Id_patio FROM locadora.Vaga vg JOIN locadora.Patio p ON vg.Id_patio = p.Id_patio WHERE vg.Id_vaga = NEW.Id_vaga), dt_extracao = NOW()
    WHERE nk_id_locacao = NEW.Id_locacao AND nk_frota_origem = 'guilherme-hu';
END//

DELIMITER ;
