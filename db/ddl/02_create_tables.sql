SET search_path TO sicred;

CREATE TABLE IF NOT EXISTS emissor (
    id_emissor      SERIAL          PRIMARY KEY,
    nome_emissor    VARCHAR(100)    NOT NULL,
    cnpj_emissor    VARCHAR(20)     NOT NULL UNIQUE,
    setor           VARCHAR(50),
    rating          VARCHAR(10),
    criado_em       TIMESTAMP       NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE emissor IS 
    'Entidades emissoras de ativos financeiros.';

CREATE TABLE IF NOT EXISTS ativo (
    id_ativo        SERIAL          PRIMARY KEY,
    codigo_ativo    VARCHAR(20)     NOT NULL UNIQUE,
    nome_ativo      VARCHAR(100)    NOT NULL,
    classe_ativo    VARCHAR(50)     NOT NULL,
    indexador       VARCHAR(30),
    data_vencimento DATE,
    id_emissor      INT             NOT NULL,
    criado_em       TIMESTAMP       NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_ativo_emissor 
        FOREIGN KEY (id_emissor) 
        REFERENCES emissor(id_emissor) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE
);

COMMENT ON TABLE ativo IS 
    'Instrumentos financeiros que compõem as carteiras dos fundos.';

CREATE TABLE IF NOT EXISTS fundo (
    id_fundo        SERIAL          PRIMARY KEY,
    cnpj_fundo      VARCHAR(20)     NOT NULL UNIQUE,
    nome_fundo      VARCHAR(100)    NOT NULL,
    tipo_fundo      VARCHAR(50)     NOT NULL,
    data_inicio     DATE            NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'ATIVO',
    criado_em       TIMESTAMP       NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_fundo_status 
        CHECK (status IN ('ATIVO', 'ENCERRADO', 'SUSPENSO', 'EM_LIQUIDACAO'))
);

COMMENT ON TABLE fundo IS 
    'Fundos de investimento administrados pela Sicred Asset Management.';

CREATE TABLE IF NOT EXISTS posicao_carteira (
    id_posicao       SERIAL          PRIMARY KEY,
    data_posicao     DATE            NOT NULL,
    id_fundo         INT             NOT NULL,
    id_ativo         INT             NOT NULL,
    quantidade       DECIMAL(18,6)   NOT NULL CHECK (quantidade >= 0),
    preco_unitario   DECIMAL(18,6)   NOT NULL CHECK (preco_unitario >= 0),
    valor_financeiro DECIMAL(18,2)   NOT NULL,
    percentual_pl    DECIMAL(9,6)    NOT NULL CHECK (percentual_pl >= 0),
    criado_em        TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_posicao_data_fundo_ativo 
        UNIQUE (data_posicao, id_fundo, id_ativo),
    CONSTRAINT fk_posicao_fundo 
        FOREIGN KEY (id_fundo) 
        REFERENCES fundo(id_fundo) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,
    CONSTRAINT fk_posicao_ativo 
        FOREIGN KEY (id_ativo) 
        REFERENCES ativo(id_ativo) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE
);

COMMENT ON TABLE posicao_carteira IS 
    'Snapshot diário da posição de cada ativo em cada fundo.';

CREATE TABLE IF NOT EXISTS operacao (
    id_operacao     SERIAL          PRIMARY KEY,
    data_operacao   DATE            NOT NULL,
    id_fundo        INT             NOT NULL,
    id_ativo        INT             NOT NULL,
    tipo_operacao   VARCHAR(20)     NOT NULL,
    quantidade      DECIMAL(18,6)   NOT NULL CHECK (quantidade > 0),
    preco_operacao  DECIMAL(18,6)   NOT NULL CHECK (preco_operacao > 0),
    valor_operacao  DECIMAL(18,2)   NOT NULL,
    criado_em       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_operacao_tipo 
        CHECK (tipo_operacao IN ('COMPRA', 'VENDA', 'AMORTIZACAO', 'VENCIMENTO', 'TRANSFERENCIA')),
    CONSTRAINT fk_operacao_fundo 
        FOREIGN KEY (id_fundo) 
        REFERENCES fundo(id_fundo) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,
    CONSTRAINT fk_operacao_ativo 
        FOREIGN KEY (id_ativo) 
        REFERENCES ativo(id_ativo) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE
);

COMMENT ON TABLE operacao IS 
    'Registro de cada compra ou venda realizada pelos fundos.';

CREATE TABLE IF NOT EXISTS preco_mercado (
    id_preco        SERIAL          PRIMARY KEY,
    data_referencia DATE            NOT NULL,
    id_ativo        INT             NOT NULL,
    preco_mercado   DECIMAL(18,6)   NOT NULL CHECK (preco_mercado >= 0),
    fonte_preco     VARCHAR(50)     NOT NULL,
    criado_em       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_preco_data_ativo_fonte 
        UNIQUE (data_referencia, id_ativo, fonte_preco),
    CONSTRAINT fk_preco_ativo 
        FOREIGN KEY (id_ativo) 
        REFERENCES ativo(id_ativo) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE
);

COMMENT ON TABLE preco_mercado IS 
    'Série histórica de preços de mercado dos ativos.';