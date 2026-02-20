# databricks-lakeflow-streamlab

Lakeflow StreamLab demonstrates an end-to-end streaming data pipeline using Databricks Lakeflow (Delta Live Tables), focusing on schema evolution, data quality, and real-time analytics.

┌────────────────────┐
│ Streaming Generator│
│ (PySpark)          │
│ - dados válidos    │
│ - dados inválidos  │
│ - schema changes   │
└─────────┬──────────┘
          ↓
┌────────────────────┐
│ BRONZE (DLT)       │
│ - ingestão raw     │
│ - schema flexível  │
└─────────┬──────────┘
          ↓
┌────────────────────┐
│ SILVER (DLT)       │
│ - validação        │
│ - expectations     │
│ - limpeza          │
└─────────┬──────────┘
          ↓
┌────────────────────┐
│ GOLD (DLT)         │
│ - agregações       │
│ - métricas         │
└─────────┬──────────┘
          ↓
┌────────────────────┐
│ Databricks SQL     │
│ - queries          │
│ - dashboards       │
└────────────────────┘


# Data Pipeline: E-commerce Medallion Architecture (DLT & Streaming)

Este projeto implementa um pipeline de dados ponta a ponta utilizando **Delta Live Tables (DLT)** no Databricks. A arquitetura segue o padrão **Medallion (Bronze, Silver)**, processando dados de e-commerce em tempo real via **Streaming** e tratando mudanças de estado através de **CDC (Change Data Capture)**.

## 🚀 Tecnologias Utilizadas

* **Databricks Delta Live Tables (DLT)**: Para orquestração e governança do pipeline.
* **Spark Structured Streaming**: Para processamento de dados em tempo real.
* **Unity Catalog**: Gerenciamento de governança, esquemas e permissões.
* **Python & SQL**: Linguagens utilizadas no desenvolvimento das transformações.

---

## 🏗️ Arquitetura do Pipeline

O pipeline está dividido em camadas para garantir a qualidade e a confiabilidade dos dados:

1.  **Bronze**: Ingestão de dados brutos (*raw*) mantendo a fidelidade à fonte original.
2.  **CDC**: Captura de mudanças para entidades dinâmicas (Clientes, Produtos, Vendedores).
3.  **Silver**: Limpeza, deduplicação e enriquecimento dos dados.

### Fluxo Visual do Pipeline
> [!TIP]
> Abaixo estão as representações visuais do fluxo de dados e da linhagem das tabelas.

![Pipeline de Dados - Visão Geral](caminho_da_sua_imagem_1.png)
*Legenda: Fluxo de ingestão da camada Bronze para a Silver.*

![Linhagem DLT](caminho_da_sua_imagem_2.png)
*Legenda: Grafo de dependências gerado pelo Delta Live Tables.*

---

## 📂 Estrutura de Arquivos

```text
src/
├── pipelines/
│   ├── bronze/          # Scripts de ingestão inicial (Raw)
│   ├── cdc/             # Lógica de Change Data Capture
│   └── silver/          # Transformações, joins e limpeza
├── setup/               # Scripts de infraestrutura (Schemas, UC, Volumes)
└── utils/               # Gerador de dados sintéticos para teste
