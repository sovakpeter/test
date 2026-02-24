-- schema.list_tables
SELECT table_name, table_type
FROM system.information_schema.tables
WHERE table_catalog = :catalog
  AND table_schema = :schema_name
ORDER BY table_name
