-- Para o Catálogo
GRANT USE CATALOG ON CATALOG olist_lakehouse TO `account users`;

-- Para os Esquemas
GRANT USE SCHEMA ON SCHEMA olist_lakehouse.bronze TO `account users`;
GRANT USE SCHEMA ON SCHEMA olist_lakehouse.silver TO `account users`;
GRANT USE SCHEMA ON SCHEMA olist_lakehouse.gold TO `account users`;
GRANT USE SCHEMA ON SCHEMA olist_lakehouse.cdc TO `account users`;

-- Opcional: Se você quer que eles consigam ler as tabelas dentro dos esquemas
GRANT SELECT ON SCHEMA olist_lakehouse.bronze TO `account users`;
GRANT SELECT ON SCHEMA olist_lakehouse.silver TO `account users`;
GRANT SELECT ON SCHEMA olist_lakehouse.gold TO `account users`;
GRANT SELECT ON SCHEMA olist_lakehouse.cdc TO `account users`;