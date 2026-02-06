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
