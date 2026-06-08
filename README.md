# Sicred Data Engineering — ETL Pipeline

## Resumo
Pipeline ETL batch para consolidação de dados de posições de carteiras de fundos de investimento. Extrai dados do PostgreSQL, processa via Apache Spark e gera um arquivo CSV com visão analítica unificada.

## Tecnologias
- Python 3.8+
- Apache Spark 3.5.1 (PySpark)
- PostgreSQL 15
- Docker + Docker Compose
- pytest

## Arquitetura

- PostgreSQL → PySpark ETL → carteira_flat.csv

## Estrutura

sicred-data-engineering/
├── db/
│   ├── ddl/   — criação de tabelas e índices
│   └── dml/   — dados de exemplo
├── docker/    — Dockerfiles
├── spark/     — código PySpark (ETL)
├── tests/     — testes unitários
├── docker-compose.yml
└── requirements.txt

## Como executar

### Pré-requisitos
- Docker Desktop instalado e rodando

### Subir o banco
```bash
docker compose up postgres -d
```

### Executar o ETL
```bash
docker compose up etl
```

O CSV será gerado na pasta `output/`.

### Executar os testes
```bash
pip install -e ".[test]"
pytest tests/unit/ -v -m unit
```

## Modelo de Dados
- **FUNDO** — fundos de investimento
- **ATIVO** — instrumentos financeiros
- **EMISSOR** — emissores dos ativos
- **POSICAO_CARTEIRA** — posições diárias (tabela fato)
- **PRECO_MERCADO** — preços históricos
- **OPERACAO** — compras e vendas

## Saída — carteira_flat.csv
Arquivo CSV com visão consolidada contendo 19 colunas:
`data_posicao`, `cnpj_fundo`, `nome_fundo`, `tipo_fundo`, `codigo_ativo`, `nome_ativo`, `classe_ativo`, `indexador`, `data_vencimento`, `nome_emissor`, `cnpj_emissor`, `setor_emissor`, `rating_emissor`, `quantidade`, `preco_unitario`, `preco_mercado`, `valor_financeiro`, `percentual_pl`, `fonte_preco`

## Decisões técnicas
- **LEFT JOIN** em vez de INNER JOIN para não perder posições sem preço de mercado
- **DecimalType** em vez de DoubleType para precisão exata em valores financeiros
- **Window Function** para deduplicação de preços por fonte prioritária (ANBIMA > B3)
- **Schemas explícitos**

