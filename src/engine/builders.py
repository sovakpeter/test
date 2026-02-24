# src/engine/builders.py
"""
SQLGlot AST builders - turn validated intents into (sql, params) tuples.

Key design decisions:
1. Uses identity mapping for columns (physical == logical) since UI sends 
   physical column names directly.
2. Uses exp.Placeholder for bind parameters, then normalizes to %(param)s.
3. Returns (sql, params) tuples for the Databricks SQL connector.
"""

from __future__ import annotations

import re
from typing import Any

from sqlglot import expressions as exp

from src.engine.config import DEFAULT_DIALECT, get_default_limit, get_global_max_limit
from src.engine.models import (
    SelectIntent,
    InsertIntent,
    UpdateIntent,
    DeleteIntent,
    FilterClause,
    AggregateColumn,
)


# ═══════════════════════════════════════════════════════════════════════════
# Placeholder Normalization
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_placeholders(sql: str) -> str:
    """
    Convert :param_name placeholders to %(param_name)s format.
    
    SQLGlot renders placeholders as :name, but Databricks connector expects %(name)s.
    """
    # Match :identifier patterns (colon followed by word chars)
    # Be careful not to match :: (double colon) which is a cast operator
    return re.sub(r'(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)', r'%(\1)s', sql)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _make_placeholder(param_name: str) -> exp.Placeholder:
    """Create a named placeholder for bind parameters."""
    return exp.Placeholder(this=param_name)


def _make_column(col_name: str) -> exp.Column:
    """Create a column expression with proper quoting."""
    return exp.Column(this=exp.to_identifier(col_name, quoted=True))


def _build_filter_expr(
    filter_clause: FilterClause,
    params: dict[str, Any],
    param_prefix: str,
    idx: int,
) -> exp.Expression:
    """
    Convert a single filter clause into a sqlglot expression with bind parameters.
    
    Updates params dict with the parameter values.
    """
    col_expr = _make_column(filter_clause.column)
    op = filter_clause.op.upper()
    value = filter_clause.value

    if op == "IS NULL":
        return exp.Is(this=col_expr, expression=exp.Null())
    
    if op == "IS NOT NULL":
        return exp.Not(this=exp.Is(this=col_expr, expression=exp.Null()))

    if op in ("IN", "NOT IN"):
        # Multiple placeholders for IN clause
        if not isinstance(value, (list, tuple)):
            value = [value]
        in_placeholders = []
        for j, val in enumerate(value):
            param_name = f"{param_prefix}_{filter_clause.column}_{idx}_{j}"
            params[param_name] = val
            in_placeholders.append(_make_placeholder(param_name))
        
        in_expr = exp.In(this=col_expr, expressions=in_placeholders)
        return exp.Not(this=in_expr) if op == "NOT IN" else in_expr

    if op == "BETWEEN":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError("BETWEEN requires a list/tuple of [low, high]")
        param_low = f"{param_prefix}_{filter_clause.column}_{idx}_low"
        param_high = f"{param_prefix}_{filter_clause.column}_{idx}_high"
        params[param_low] = value[0]
        params[param_high] = value[1]
        return exp.Between(
            this=col_expr,
            low=_make_placeholder(param_low),
            high=_make_placeholder(param_high),
        )

    if op in ("LIKE", "NOT LIKE"):
        param_name = f"{param_prefix}_{filter_clause.column}_{idx}"
        params[param_name] = value
        like_expr = exp.Like(this=col_expr, expression=_make_placeholder(param_name))
        return exp.Not(this=like_expr) if op == "NOT LIKE" else like_expr

    # Standard comparison operators: =, !=, <>, <, <=, >, >=
    param_name = f"{param_prefix}_{filter_clause.column}_{idx}"
    params[param_name] = value
    placeholder = _make_placeholder(param_name)

    op_map = {
        "=": exp.EQ,
        "!=": exp.NEQ,
        "<>": exp.NEQ,
        "<": exp.LT,
        "<=": exp.LTE,
        ">": exp.GT,
        ">=": exp.GTE,
    }
    
    expr_class = op_map.get(op)
    if expr_class is None:
        raise ValueError(f"Unsupported operator: {op}")
    
    return expr_class(this=col_expr, expression=placeholder)


def _combine_and(exprs: list[exp.Expression]) -> exp.Expression:
    """AND-combine a list of expressions."""
    if not exprs:
        raise ValueError("Cannot combine empty expression list")
    result = exprs[0]
    for extra in exprs[1:]:
        result = exp.And(this=result, expression=extra)
    return result


def _combine_or(exprs: list[exp.Expression]) -> exp.Expression:
    """OR-combine a list of expressions."""
    if not exprs:
        raise ValueError("Cannot combine empty expression list")
    result = exprs[0]
    for extra in exprs[1:]:
        result = exp.Or(this=result, expression=extra)
    return result


AGGREGATE_FUNCTIONS: dict[str, type[exp.Expression]] = {
    "COUNT": exp.Count,
    "SUM": exp.Sum,
    "AVG": exp.Avg,
    "MIN": exp.Min,
    "MAX": exp.Max,
}


def _build_aggregate_expr(agg: AggregateColumn) -> exp.Expression:
    """Build a SQLGlot aggregate function expression, optionally aliased."""
    func_class = AGGREGATE_FUNCTIONS.get(agg.function.upper())
    if func_class is None:
        raise ValueError(f"Unsupported aggregate function: {agg.function}")

    if agg.column == "*":
        arg = exp.Star()
    else:
        arg = _make_column(agg.column)

    func_expr = func_class(this=arg)

    if agg.alias:
        return exp.Alias(this=func_expr, alias=exp.to_identifier(agg.alias, quoted=True))
    return func_expr


# ═══════════════════════════════════════════════════════════════════════════
# SELECT builder
# ═══════════════════════════════════════════════════════════════════════════

def build_select(
    intent: SelectIntent,
    dialect: str = DEFAULT_DIALECT,
) -> tuple[str, dict[str, Any]]:
    """
    Build a SELECT statement from a validated SelectIntent.
    
    Returns (sql_string, params_dict) tuple.
    """
    params: dict[str, Any] = {}

    # --- SELECT columns ---
    if intent.is_wildcard:
        query = exp.Select().select("*")
    else:
        select_cols = [_make_column(c) for c in intent.columns]
        query = exp.Select().select(*select_cols)

    # --- Aggregate functions (added after regular columns) ---
    if intent.aggregations:
        for agg in intent.aggregations:
            query = query.select(_build_aggregate_expr(agg))

    # --- FROM ---
    # Use the table name directly (identity mapping)
    query = query.from_(intent.table)

    # --- WHERE ---
    if intent.filters:
        where_parts = []
        for i, f in enumerate(intent.filters):
            where_parts.append(_build_filter_expr(f, params, "p", i))
        query = query.where(_combine_and(where_parts))

    # --- GROUP BY ---
    if intent.group_by:
        group_cols = [_make_column(c) for c in intent.group_by]
        query = query.group_by(*group_cols)

    # --- HAVING ---
    if intent.having:
        having_parts = []
        for i, h in enumerate(intent.having):
            having_parts.append(_build_filter_expr(h, params, "h", i))
        query = query.having(_combine_and(having_parts))

    # --- ORDER BY ---
    if intent.order_by:
        for ob in intent.order_by:
            col_expr = _make_column(ob.column)
            ordered = exp.Ordered(
                this=col_expr,
                desc=(ob.direction.upper() == "DESC"),
            )
            query = query.order_by(ordered)

    # --- LIMIT ---
    limit = intent.limit
    if limit is None:
        limit = get_default_limit()
    limit = min(limit, get_global_max_limit())
    query = query.limit(limit)

    # --- OFFSET ---
    if intent.offset is not None:
        query = query.offset(intent.offset)

    # Render and normalize placeholders
    sql = query.sql(dialect=dialect)
    sql = _normalize_placeholders(sql)
    
    return sql, params


# ═══════════════════════════════════════════════════════════════════════════
# INSERT builder
# ═══════════════════════════════════════════════════════════════════════════

def build_insert(
    intent: InsertIntent,
    dialect: str = DEFAULT_DIALECT,
) -> tuple[str, dict[str, Any]]:
    """
    Build an INSERT INTO ... VALUES (...) statement.
    
    Returns (sql_string, params_dict) tuple.
    """
    params: dict[str, Any] = {}
    columns = []
    placeholders = []

    for col, val in intent.values.items():
        columns.append(exp.to_identifier(col, quoted=True))
        param_name = f"v_{col}"
        params[param_name] = val
        placeholders.append(_make_placeholder(param_name))

    # Build: INSERT INTO <table> (<cols>) VALUES (<vals>)
    insert_expr = exp.Insert(
        this=exp.Schema(
            this=exp.to_table(intent.table),
            expressions=columns,
        ),
        expression=exp.Values(
            expressions=[exp.Tuple(expressions=placeholders)],
        ),
    )

    sql = insert_expr.sql(dialect=dialect)
    sql = _normalize_placeholders(sql)
    
    return sql, params


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE builder
# ═══════════════════════════════════════════════════════════════════════════

def build_update(
    intent: UpdateIntent,
    dialect: str = DEFAULT_DIALECT,
) -> tuple[str, dict[str, Any]]:
    """
    Build an UPDATE or MERGE INTO statement.
    
    - strategy="UPDATE" → plain UPDATE ... SET ... WHERE pk = ...
    - strategy="MERGE" → MERGE INTO ... USING ... ON pk = ... 
                         WHEN MATCHED THEN UPDATE SET ...
                         WHEN NOT MATCHED THEN INSERT ...
    
    Returns (sql_string, params_dict) tuple.
    """
    if intent.strategy == "UPDATE":
        return _build_plain_update(intent, dialect)
    else:
        return _build_merge_update(intent, dialect)


def _build_plain_update(
    intent: UpdateIntent,
    dialect: str,
) -> tuple[str, dict[str, Any]]:
    """Plain UPDATE ... SET ... WHERE ..."""
    params: dict[str, Any] = {}

    # SET clause
    set_exprs = []
    for col, val in intent.updates.items():
        param_name = f"s_{col}"
        params[param_name] = val
        eq_expr = exp.EQ(
            this=_make_column(col),
            expression=_make_placeholder(param_name),
        )
        set_exprs.append(eq_expr)

    # WHERE on PKs
    pk_exprs = []
    for pk_col, pk_val in intent.pk_values.items():
        param_name = f"pk_{pk_col}"
        params[param_name] = pk_val
        pk_exprs.append(
            exp.EQ(
                this=_make_column(pk_col),
                expression=_make_placeholder(param_name),
            )
        )

    # OCC: old value conditions (optimistic concurrency guard)
    old_exprs = []
    for col, val in intent.old_values.items():
        param_name = f"old_{col}"
        params[param_name] = val
        old_exprs.append(
            exp.EQ(
                this=_make_column(col),
                expression=_make_placeholder(param_name),
            )
        )

    all_where = pk_exprs + old_exprs
    update_expr = exp.Update(
        this=exp.to_table(intent.table),
        expressions=set_exprs,
        where=exp.Where(this=_combine_and(all_where)),
    )

    sql = update_expr.sql(dialect=dialect)
    sql = _normalize_placeholders(sql)
    
    return sql, params


def _build_merge_update(
    intent: UpdateIntent,
    dialect: str,
) -> tuple[str, dict[str, Any]]:
    """
    MERGE INTO <table> AS t
    USING (SELECT :pk AS `pk`, :col AS `col`, ...) AS s
    ON t.`pk` = s.`pk`
    WHEN MATCHED THEN UPDATE SET t.`col` = s.`col`, ...
    WHEN NOT MATCHED THEN INSERT (`pk`, `col`) VALUES (s.`pk`, s.`col`)

    Built entirely via SQLGlot AST nodes targeting Spark dialect.
    """
    params: dict[str, Any] = {}

    # Collect all columns: PKs + update values
    all_cols: dict[str, Any] = {}
    for col, val in intent.pk_values.items():
        all_cols[col] = val
    for col, val in intent.updates.items():
        all_cols[col] = val

    # --- Target table with alias ---
    target = exp.to_table(intent.table)
    target.set("alias", exp.TableAlias(this=exp.to_identifier("t")))

    # --- Source: SELECT :m_col AS `col`, ... (subquery aliased as s) ---
    source_aliases = []
    for col, val in all_cols.items():
        param_name = f"m_{col}"
        params[param_name] = val
        source_aliases.append(
            exp.Alias(
                this=_make_placeholder(param_name),
                alias=exp.to_identifier(col, quoted=True),
            )
        )
    source_select = exp.Select().select(*source_aliases)
    source = exp.Subquery(
        this=source_select,
        alias=exp.TableAlias(this=exp.to_identifier("s")),
    )

    # --- ON condition: t.`pk` = s.`pk` AND ... ---
    on_parts = []
    for pk_col in intent.pk_values.keys():
        on_parts.append(
            exp.EQ(
                this=exp.Column(
                    this=exp.to_identifier(pk_col, quoted=True),
                    table=exp.to_identifier("t"),
                ),
                expression=exp.Column(
                    this=exp.to_identifier(pk_col, quoted=True),
                    table=exp.to_identifier("s"),
                ),
            )
        )
    on_condition = _combine_and(on_parts)

    # --- WHEN MATCHED THEN UPDATE SET t.`col` = s.`col`, ... ---
    update_set = []
    for col in intent.updates.keys():
        update_set.append(
            exp.EQ(
                this=exp.Column(
                    this=exp.to_identifier(col, quoted=True),
                    table=exp.to_identifier("t"),
                ),
                expression=exp.Column(
                    this=exp.to_identifier(col, quoted=True),
                    table=exp.to_identifier("s"),
                ),
            )
        )
    when_matched = exp.When(
        matched=True,
        then=exp.Update(expressions=update_set),
    )

    # --- WHEN NOT MATCHED THEN INSERT (`pk`, `col`) VALUES (s.`pk`, s.`col`) ---
    insert_cols = exp.Tuple(
        expressions=[
            exp.Column(this=exp.to_identifier(c, quoted=True))
            for c in all_cols.keys()
        ]
    )
    insert_vals = exp.Tuple(
        expressions=[
            exp.Column(
                this=exp.to_identifier(c, quoted=True),
                table=exp.to_identifier("s"),
            )
            for c in all_cols.keys()
        ]
    )
    when_not_matched = exp.When(
        matched=False,
        then=exp.Insert(this=insert_cols, expression=insert_vals),
    )

    # --- Assemble MERGE ---
    merge_expr = exp.Merge(
        this=target,
        using=source,
        on=on_condition,
        whens=exp.Whens(expressions=[when_matched, when_not_matched]),
    )

    sql = merge_expr.sql(dialect=dialect)
    sql = _normalize_placeholders(sql)

    return sql, params


# ═══════════════════════════════════════════════════════════════════════════
# DELETE builder
# ═══════════════════════════════════════════════════════════════════════════

def build_delete(
    intent: DeleteIntent,
    dialect: str = DEFAULT_DIALECT,
) -> tuple[str, dict[str, Any]]:
    """
    Build a DELETE FROM ... WHERE pk = ... statement.
    
    For batch deletes (multiple PK dicts), uses OR to combine.
    For compound PKs, each PK set is AND-combined.
    
    Returns (sql_string, params_dict) tuple.
    """
    params: dict[str, Any] = {}
    
    # pk_values is guaranteed to be list[dict] after model validation
    pk_sets = intent.pk_values  # type: ignore[union-attr]

    row_conditions = []
    for i, pk_dict in enumerate(pk_sets):
        pk_exprs = []
        for col, val in pk_dict.items():
            param_name = f"d_{col}_{i}"
            params[param_name] = val
            pk_exprs.append(
                exp.EQ(
                    this=_make_column(col),
                    expression=_make_placeholder(param_name),
                )
            )
        
        if len(pk_exprs) == 1:
            row_conditions.append(pk_exprs[0])
        else:
            # Compound PK: wrap in parens for clarity
            row_conditions.append(exp.Paren(this=_combine_and(pk_exprs)))

    # Combine multiple rows with OR
    if len(row_conditions) == 1:
        where_expr = row_conditions[0]
    else:
        where_expr = _combine_or(row_conditions)

    delete_expr = exp.Delete(
        this=exp.to_table(intent.table),
        where=exp.Where(this=where_expr),
    )

    sql = delete_expr.sql(dialect=dialect)
    sql = _normalize_placeholders(sql)
    
    return sql, params
