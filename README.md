# databricks-lakeflow-streamlab

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
