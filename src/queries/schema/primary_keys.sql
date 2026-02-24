-- schema.primary_keys
SELECT kcu.column_name
FROM system.information_schema.table_constraints tc
JOIN system.information_schema.key_column_usage kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
 AND tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_catalog = :catalog
  AND tc.table_schema = :schema_name
  AND tc.table_name = :table_name
ORDER BY kcu.ordinal_position
