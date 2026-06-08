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

## Por que escolhi este design?

### PostgreSQL
Escolhi PostgreSQL por ser um banco relacional robusto, open source e com excelente suporte a tipos financeiros como `DECIMAL(18,6)`, que garante precisão aritmética exata para valores monetários. SQLite seria insuficiente para simular um ambiente de produção e MySQL não tem suporte nativo tão maduro a constraints complexas.

### Apache Spark
O case exigia um framework de processamento distribuído. Spark foi a escolha natural por ser o padrão de mercado para ETL em ambientes de Data Lake, ter integração nativa com JDBC para leitura de bancos relacionais, suportar PySpark (Python), e ter documentação e comunidade muito mais maduras que Flink ou Storm para este tipo de pipeline batch.

### LEFT JOIN em vez de INNER JOIN
Todos os JOINs do pipeline usam LEFT JOIN. Em Asset Management, perder uma posição silenciosamente é crítico — o PL reportado ficaria menor que o real e a reconciliação com a custódia falharia. Com LEFT JOIN, posições sem preço de mercado aparecem no output com valor `N/A`, tornando o problema visível e auditável.

### DecimalType em vez de DoubleType no Spark
Valores financeiros exigem precisão decimal exata. O tipo `Double` usa ponto flutuante binário e introduz erros de arredondamento — `0.1 + 0.2 = 0.30000000000000004`. O `DecimalType(18,6)` do Spark garante precisão exata, essencial para cálculo de PL e valor financeiro.

### Docker
Docker garante que o ambiente de execução seja idêntico em qualquer máquina. Sem Docker, seria preciso instalar PostgreSQL, Java, Spark e configurar variáveis de ambiente manualmente, gerando risco de "funciona na minha máquina". Com `docker compose up etl` o pipeline executa do zero em qualquer ambiente.

### Estrutura de pastas
A separação em `db/`, `spark/`, `tests/` e `docker/` segue o princípio de separação de responsabilidades. Cada pasta tem uma função clara e pode ser evoluída independentemente. Scripts SQL ficam em `db/` para serem versionados e executados em qualquer ambiente, não apenas via Docker.

---

## O que eu faria com mais tempo?

### Processamento incremental (delta load)
Atualmente o pipeline faz full load , lê todas as posições a cada execução. Em produção com anos de histórico isso seria inviável. Implementaria processamento incremental com parâmetros `--data-inicio` e `--data-fim` para processar apenas o delta diário.

### Orquestração com Apache Airflow
O pipeline hoje é executado manualmente via `docker compose up etl`. Em produção, precisaria ser agendado para rodar automaticamente após o fechamento do mercado (após as 18h). Implementaria uma DAG no Airflow com dependências, retentativas automáticas e alertas por e-mail em caso de falha.

### Camada de qualidade de dados com Great Expectations
Adicionaria validações automáticas antes do LOAD: verificar que a soma de `percentual_pl` por fundo está próxima de 100%, que não existem preços negativos, que datas de vencimento são futuras. Falhas gerariam alertas antes de o arquivo corrompido chegar aos consumidores.

### Migração para Delta Lake
Substituiria o CSV por Delta Lake (formato colunar com suporte a ACID, time travel e schema evolution). Isso permitiria consultas históricas, rollback de dados incorretos e evolução do schema sem reprocessamento completo.

### Testes de integração automatizados
Os testes unitários existentes cobrem a lógica de transformação com dados em memória. Adicionaria testes de integração que sobem o PostgreSQL via Docker, inserem dados reais e verificam o CSV gerado — cobertura end-to-end do pipeline.

### API REST para consulta da carteira_flat
Exporia os dados via FastAPI para que as áreas de gestão, risco e compliance pudessem consultar as posições sem precisar abrir o CSV — com filtros por fundo, data e classe de ativo.

---
