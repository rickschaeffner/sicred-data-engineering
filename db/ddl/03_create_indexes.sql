SET search_path TO sicred;

CREATE INDEX IF NOT EXISTS idx_posicao_data_posicao 
    ON posicao_carteira(data_posicao);

CREATE INDEX IF NOT EXISTS idx_posicao_id_fundo 
    ON posicao_carteira(id_fundo);

CREATE INDEX IF NOT EXISTS idx_posicao_id_ativo 
    ON posicao_carteira(id_ativo);

CREATE INDEX IF NOT EXISTS idx_posicao_data_fundo 
    ON posicao_carteira(data_posicao, id_fundo);

CREATE INDEX IF NOT EXISTS idx_preco_data_referencia 
    ON preco_mercado(data_referencia);

CREATE INDEX IF NOT EXISTS idx_preco_id_ativo 
    ON preco_mercado(id_ativo);

CREATE INDEX IF NOT EXISTS idx_preco_data_ativo 
    ON preco_mercado(data_referencia, id_ativo);

CREATE INDEX IF NOT EXISTS idx_operacao_data 
    ON operacao(data_operacao);

CREATE INDEX IF NOT EXISTS idx_operacao_id_fundo 
    ON operacao(id_fundo);

CREATE INDEX IF NOT EXISTS idx_operacao_tipo 
    ON operacao(tipo_operacao);

CREATE INDEX IF NOT EXISTS idx_ativo_id_emissor 
    ON ativo(id_emissor);

CREATE INDEX IF NOT EXISTS idx_ativo_classe 
    ON ativo(classe_ativo);

CREATE INDEX IF NOT EXISTS idx_ativo_indexador 
    ON ativo(indexador);