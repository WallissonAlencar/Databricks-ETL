# Databricks-ETL
Extração de dados do uber eats

Este projeto tem como objetivo automatizar o processo de ingestão, transformação e análise de dados do Uber Eats utilizando o Databricks como plataforma principal de dados.
A arquitetura segue o padrão ELT (Extract, Load, Transform), aproveitando o poder do Delta Lake, PySpark e notebooks Databricks para garantir escalabilidade e performance.

elt-uber-eats-analytics-semana/
│
├── notebooks/
│   ├── 01_extract_data.py
│   ├── 02_load_bronze_delta.py
│   ├── 03_transform_silver_gold.py
│   └── 04_kpi_analysis.py
│
├── requirements.txt
├── README.md
└── utils/
    └── helpers.py



        Etapas Principais do ELT

1. Extract:

Coleta dados do Uber Eats (API ou datasets CSV).
Armazena no Databricks FileStore ou DBFS.

2.Load:

Cria tabelas Delta (camada Bronze).
Implementa controle de versão e schema evolution.

3.Transform:

Aplica regras de negócio (normalização, joins, KPIs).
Gera tabelas Silver e Gold para consumo analítico.

4. Análise:

Cálculo de métricas: tempo médio de entrega, valor médio por pedido, top restaurantes, desempenho por região etc.
Conexão com Power BI para dashboards dinâmicos.

Autor

Wallisson Alencar
📧 wallisson.alencar@gmail.com
💼 Pós-graduação em Engenharia de Dados & IA
📍 Focado em projetos de dados, automação e analytics.
