-- analytics.get_table_sample
-- table_name is substituted as an identifier (validated + quoted)
SELECT * FROM :table_name LIMIT :limit
