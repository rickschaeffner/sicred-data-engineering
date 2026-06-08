# Análise Exploratória das Carteiras — Documentação

**Arquivo:** `analise_carteiras.ipynb`  
**Autor:** Henrique Schaeffner  
**Fonte de dados:** Pipeline ETL PySpark → `carteira_flat.csv`

---

## Contexto de Negócio
Exemplo utilizado com dados ficticios:

A Sicred Asset Management administra 4 fundos de investimento com perfis distintos de risco e retorno. Antes da implementação do pipeline ETL, as informações de posição, preço, emissor e classificação dos ativos estavam fragmentadas em sistemas diferentes — tornando impossível uma visão consolidada e analítica das carteiras.

Este notebook utiliza os dados consolidados pelo pipeline ETL para responder às perguntas mais críticas do negócio de gestão de recursos:

- Qual é a composição de cada fundo?
- Como os preços dos ativos evoluíram no período?
- Qual é a exposição por classe de ativo e indexador?
- Quais ativos concentram o maior valor financeiro?
- Como o patrimônio líquido está distribuído entre os emissores?

---

## Estrutura do Notebook

### Seção 1 — Importações e Configurações
Carrega as bibliotecas necessárias e define o estilo visual padronizado para todos os gráficos. A padronização de cores por classe de ativo e rating de crédito garante consistência visual em todas as análises.

### Seção 2 — Carregamento dos Dados
Localiza automaticamente o arquivo CSV mais recente gerado pelo pipeline ETL na pasta `output/`. Realiza a conversão explícita de tipos numéricos para garantir precisão nos cálculos financeiros.

### Seção 3 — Visão Geral
Apresenta um resumo quantitativo dos dados carregados antes de qualquer análise visual, incluindo contagem de fundos, ativos e emissores, além do patrimônio líquido total consolidado e estatísticas descritivas das colunas numéricas.

### Seção 4 — Análise 1: Composição das Carteiras por Fundo
Dois gráficos complementares respondem à pergunta: *"Como cada fundo aloca seu patrimônio?"*

**Gráfico de pizza (donut):** Mostra a composição individual de cada fundo em percentual do PL. O formato donut foi escolhido por facilitar a leitura dos percentuais sem poluição visual.

**Gráfico de barras empilhadas horizontais:** Permite comparação direta entre fundos. A linha vertical vermelha em 100% serve como referência visual para identificar fundos com alocação completa.

**Por que esta análise importa:** A composição da carteira determina o perfil de risco do fundo. Um fundo de Renda Fixa com alta concentração em títulos públicos tem perfil conservador. Um fundo de Ações com concentração em poucos ativos tem risco de concentração elevado.

### Seção 5 — Análise 2: Evolução de Preços dos Ativos
Como a base de dados ficticios possui apenas uma data de referência (31/05/2024), foi gerada uma série histórica simulada de 30 dias úteis com parâmetros realistas:

- **Renda Fixa:** volatilidade diária de 0,8% — compatível com títulos públicos indexados
- **Renda Variável:** volatilidade diária de 2,2% — compatível com ações do Ibovespa

Os gráficos apresentam o retorno acumulado normalizado (base 100 no início do período), separados por classe de ativo para facilitar a comparação entre papéis de mesmo perfil de risco.

**Por que esta análise importa:** A volatilidade histórica dos preços é um dos principais insumos para cálculo de VaR (Value at Risk) e stress testing — ferramentas fundamentais nas áreas de risco de fundos de investimento.

### Seção 6 — Análise 3: Exposição por Classe de Ativo e Indexador
Três visualizações respondem à pergunta: *"Onde está o dinheiro e a qual risco de mercado ele está exposto?"*

**Barras horizontais por classe:** Compara o volume financeiro em Renda Fixa versus Renda Variável com percentual sobre o total.

**Barras horizontais por indexador:** Mostra a exposição a CDI, IPCA, SELIC e Prefixado — fundamental para análise de risco de taxa de juros. Ativos de Renda Variável aparecem como `N/A` pois não têm indexador.

**Heatmap fundo × classe:** Permite identificar em qual fundo cada classe de ativo está mais concentrada, revelando sobreposições e oportunidades de diversificação.

**Por que esta análise importa:** A exposição por indexador determina como a carteira reage a movimentos de taxa de juros (Selic), inflação (IPCA) e câmbio. É uma das análises mais requisitadas pelas áreas de risco e compliance.

### Seção 7 — Análise 4: Ranking de Ativos por Valor Financeiro
Apresenta todos os ativos em ordem decrescente de valor financeiro total, consolidando posições do mesmo ativo em múltiplos fundos. O número de fundos que possuem cada ativo é exibido sobre cada barra.

A coloração distingue Renda Fixa (azul) de Renda Variável (vermelho), permitindo identificar visualmente se os maiores ativos por valor são de natureza conservadora ou arrojada.

**Por que esta análise importa:** Concentração excessiva em poucos ativos eleva o risco idiossincrático da carteira. Reguladores como a CVM estabelecem limites de concentração por ativo para fundos de varejo.

### Seção 8 — Análise 5: Distribuição do PL por Emissor
Duas visualizações respondem à pergunta: *"Qual é o risco de crédito da carteira?"*

**Barras por emissor com rating:** Cada barra representa o valor financeiro total alocado em ativos de um emissor, com coloração baseada no rating de crédito (verde = AAA, laranja = A+, vermelho = BBB+). Isso permite identificar visualmente onde está concentrado o risco de crédito.

**Curva de concentração:** Inspirada na curva de Lorenz, mostra o percentual acumulado do PL à medida que os emissores são adicionados em ordem decrescente de exposição. A linha de referência em 80% permite aplicar o princípio de Pareto: identificar quais emissores respondem pela maior parte do risco.

**Por que esta análise importa:** O risco de crédito por emissor é monitorado diariamente pelas áreas de risco. Um emissor que entra em default pode comprometer uma parcela significativa do PL do fundo.

### Seção 9 — Dashboard Executivo Final
Consolida as principais métricas em um único painel visual com 6 quadrantes:

- **3 cards de KPI:** PL total, número de fundos, número de ativos
- **PL por fundo:** visão rápida de tamanho relativo de cada fundo
- **Renda Fixa vs Variável:** alocação macro da carteira
- **Top 5 ativos:** maior concentração por ativo

O dashboard foi desenhado para ser a primeira tela apresentada em reuniões de comitê de investimentos — informação densa com leitura imediata.

---

## Decisões Técnicas

### Por que pandas e não PySpark para as análises?
O arquivo `carteira_flat.csv` tem volume pequeno (18 registros no POC). Para análise exploratória local com visualizações, o pandas é mais adequado: integra nativamente com matplotlib e seaborn, e não exige inicialização de cluster Spark. O PySpark é usado no pipeline de produção onde o volume de dados justifica processamento distribuído.

### Por que matplotlib e seaborn e não plotly?
Matplotlib e seaborn geram gráficos estáticos de alta qualidade, adequados para relatórios em PDF e apresentações. Plotly geraria gráficos interativos, úteis em dashboards web mas desnecessários para análise exploratória em notebook.

### Por que simular a série histórica de preços?
A base de dados do POC possui apenas uma data de referência. Em produção, a tabela `preco_mercado` acumularia histórico diário e a série seria real. A simulação foi feita com parâmetros estatisticamente realistas (volatilidade por classe de ativo) para demonstrar o tipo de análise que seria feita com dados reais.

### Por que separar Renda Fixa de Renda Variável na análise de preços?
As duas classes têm dinâmicas de precificação completamente diferentes. Renda Fixa tem volatilidade baixa e trajetória suave (marcação pela curva). Renda Variável tem volatilidade alta e movimentos bruscos (marcação a mercado via bolsa). Misturá-las num mesmo gráfico tornaria a visualização ilegível.

---

## Como Executar

### Pré-requisitos
O pipeline ETL deve ter sido executado antes para gerar o arquivo CSV:

```bash
docker compose up etl
```

### Instalação das dependências do notebook

```bash
pip install jupyter matplotlib seaborn pandas numpy
```

### Execução

```bash
cd notebooks
jupyter notebook
```

Abra o arquivo `analise_carteiras.ipynb` e execute célula por célula com **Shift+Enter**, ou execute todas de uma vez pelo menu **Kernel → Restart & Run All**.

### Saída esperada
Após execução completa, os seguintes gráficos são gerados e exibidos inline no notebook:

- Gráfico de pizza — composição por fundo
- Gráfico de barras empilhadas — composição comparativa
- Gráfico de retorno acumulado — Renda Fixa
- Gráfico de retorno acumulado — Renda Variável
- Gráfico de exposição por classe de ativo
- Gráfico de exposição por indexador
- Heatmap fundo × classe de ativo
- Ranking de ativos por valor financeiro
- Gráfico de PL por emissor com rating
- Curva de concentração por emissor
- Dashboard executivo final

---

## Limitações e Próximos Passos

**Limitações do POC:**
- Apenas uma data de referência disponível (31/05/2024)
- 18 posições no total — volume insuficiente para análises estatísticas robustas
- Série histórica de preços simulada, não real

**Com dados reais de produção seria possível:**
- Calcular VaR histórico (Value at Risk) com janela de 252 dias úteis
- Analisar correlação entre ativos para otimização de carteira (Markowitz)
- Monitorar drawdown máximo por fundo
- Calcular tracking error em relação ao benchmark (CDI, IBOV)
- Gerar relatório automático em PDF com todas as análises
- Criar alertas automáticos para concentração acima dos limites regulatórios