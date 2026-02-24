-- schema.list_schemas
SELECT schema_name
FROM system.information_schema.schemata
WHERE catalog_name = :catalog
ORDER BY schema_name
