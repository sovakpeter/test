-- schema.table_columns
SELECT
  column_name,
  full_data_type as data_type,
  (is_nullable = 'YES') as is_nullable,
  ordinal_position
FROM system.information_schema.columns
WHERE table_catalog = :catalog
  AND table_schema = :schema_name
  AND table_name = :table_name
ORDER BY ordinal_position
