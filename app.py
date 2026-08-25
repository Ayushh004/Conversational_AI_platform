
import asyncio
import base64
import datetime
import decimal
import io
import json
import os
import re
import time
import uuid
import pandas as pd
import pyodbc
import requests
import tempfile
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from groq import Groq
from dotenv import load_dotenv
from aiohttp import web

# Microsoft 365 Agents SDK imports
from microsoft_agents.hosting.aiohttp import (
    CloudAdapter,
    start_agent_process,
    jwt_authorization_middleware,
)
from microsoft_agents.hosting.core import (
    AgentApplication,
    TurnState,
    TurnContext,
    MemoryStorage,
    Authorization,
)
from microsoft_agents.authentication.msal import MsalConnectionManager
from microsoft_agents.activity import Activity, load_configuration_from_env

load_dotenv()

# ── Env vars ──────────────────────────────────────────────────────────────────
FABRIC_WORKSPACE_ID     = os.environ.get("FABRIC_WORKSPACE_ID", "")
FABRIC_NOTEBOOK_ID      = os.environ.get("FABRIC_NOTEBOOK_ID", "")
FABRIC_TENANT_ID        = os.environ.get("FABRIC_TENANT_ID", "")
FABRIC_CLIENT_ID        = os.environ.get("FABRIC_CLIENT_ID", "")
FABRIC_CLIENT_SECRET    = os.environ.get("FABRIC_CLIENT_SECRET", "")
FABRIC_LAKEHOUSE_ID     = os.environ.get("FABRIC_LAKEHOUSE_ID", "")
FABRIC_USER_EMAIL       = os.environ.get("FABRIC_USER_EMAIL", "")
FABRIC_USER_PASSWORD    = os.environ.get("FABRIC_USER_PASSWORD", "")
GROQ_API_KEY          = os.environ.get("GROQ_API_KEY", "")
FABRIC_UPLOAD_FOLDER       = "Files/uploads"
UPLOAD_TTL_SECONDS         = 24 * 3600   # 24 hours — uploaded tables auto-deleted after this
FABRIC_UPLOAD_NOTEBOOK_ID  = "46657499-1393-49b8-9363-b97db862879c"
NGROK_DOMAIN               = "https://inapplicable-nonpestilential-brinley.ngrok-free.dev"

SQL_SERVER   = "rcmgynklls5uvnpwnp7i3pdp3m-m7vuuz4ofziejmgmkmzzixgije.datawarehouse.fabric.microsoft.com"
SQL_DATABASE = "EcommereAnalytics"

GROQ_MODEL = "llama-3.3-70b-versatile"

agents_sdk_config  = load_configuration_from_env(os.environ)
STORAGE            = MemoryStorage()
CONNECTION_MANAGER = MsalConnectionManager(**agents_sdk_config)
ADAPTER            = CloudAdapter(connection_manager=CONNECTION_MANAGER)
AUTHORIZATION      = Authorization(STORAGE, CONNECTION_MANAGER)

AGENT_APP = AgentApplication[TurnState](
    storage=STORAGE,
    connection_manager=CONNECTION_MANAGER,
    authorization=AUTHORIZATION,
)

groq_client    = Groq(
    api_key=GROQ_API_KEY,
)


class GroqRateLimitError(Exception):
    """Raised when Groq returns a 429 (rate / token-per-day limit reached).
    Carries an optional retry-after hint parsed from the message."""
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def _groq_chat(**kwargs):
    """Single choke-point for all Groq chat completions. Converts a 429 into a
    typed GroqRateLimitError so the user-facing layer can show a clear
    'over capacity, try again shortly' message instead of a misleading
    'no data found' card."""
    try:
        return groq_client.chat.completions.create(**kwargs)
    except Exception as e:
        msg = str(e)
        if "429" in msg or "rate limit" in msg.lower() or "rate_limit" in msg.lower():
            retry = None
            m = re.search(r"try again in ([0-9hms\.\s]+)", msg)
            if m:
                retry = m.group(1).strip()
            raise GroqRateLimitError(msg, retry_after=retry) from e
        raise


_token_cache        = {"token": None, "expires_at": 0}
_onelake_token_cache = {"token": None, "expires_at": 0}
_session_store = {}


# ── Dynamic schema fetching ───────────────────────────────────────────────────
#
# Schema is fetched live from the SQL Analytics endpoint via INFORMATION_SCHEMA.
# Result is cached for 6 hours so we don't hit the endpoint on every message.
# Falls back to a hardcoded schema if the fetch fails (e.g. on startup timeout).
#
# Hardcoded relationships are kept static — they never change and cannot be
# inferred from INFORMATION_SCHEMA without extra metadata queries.
#
_SCHEMA_CACHE     = {"schema": None, "invalid_combos": None, "fetched_at": 0}
_TABLE_SCHEMA_MAP = {}   # table_name -> actual schema (e.g. "dbo", "default", etc.)
_SCHEMA_TTL   = 6 * 3600  # 6 hours

# Fixed relationships — always appended to the dynamic schema
_RELATIONSHIPS = """
════════════════════════════════════════════════════════════
RELATIONSHIPS
════════════════════════════════════════════════════════════
  fact_sales.product_id  -> dim_product.product_id
  fact_sales.customer_id -> dim_customer.customer_id
  fact_sales.vendor_id   -> dim_vendor.vendor_id
  fact_sales.region      -> dim_geography.region
  fact_sales.channel     -> dim_channel.channel
  fact_sales.date        -> dim_date.date
"""

# Table-level purpose hints — used in the dynamic schema output
_TABLE_PURPOSE = {
    "fact_sales":        "row-level sales transactions — use for detail queries or filters not in agg tables",
    "dim_product":       "product attributes: name, category, price",
    "dim_customer":      "customer attributes: name, segment, region, email, lifetime_value",
    "dim_vendor":        "vendor attributes: name, category, country, rating, is_preferred",
    "dim_geography":     "region to region_code mapping",
    "dim_channel":       "sales channel list",
    "dim_date":          "date dimension: year, month, quarter, week, day_of_week, is_weekend",
    "agg_by_category":   "pre-aggregated totals by product category",
    "agg_by_channel":    "pre-aggregated totals by sales channel",
    "agg_by_customer":   "pre-aggregated totals per customer",
    "agg_by_product":    "pre-aggregated totals per product",
    "agg_by_region":     "pre-aggregated totals by region",
    "agg_by_vendor":     "pre-aggregated totals per vendor",
    "agg_daily_trend":   "pre-aggregated daily revenue/profit/units trends",
    "agg_by_year_month": "pre-aggregated monthly and yearly revenue/profit/units totals",
}

# Fallback static schema used if dynamic fetch fails
_FALLBACK_SCHEMA = """
Database: EcommereAnalytics   Schema: dbo   Dialect: T-SQL (SQL Server / Fabric SQL Analytics endpoint)
[FALLBACK — live schema fetch failed, using cached definition]

FACT TABLE:
  fact_sales: sale_id, date, product_id, product_name, category, customer_id, vendor_id,
              price, units_sold, revenue, discount, net_revenue, returns, region, channel,
              net_profit, year, month, quarter, discount_pct, profit_margin_pct, return_flag

DIMENSION TABLES:
  dim_product:   product_id, product_name, category, price
  dim_customer:  customer_id, customer_name, segment, region, email, join_date, lifetime_value
  dim_vendor:    vendor_id, vendor_name, category, country, rating, contract_start, payment_terms, is_preferred
  dim_geography: region, region_code
  dim_channel:   channel
  dim_date:      date, year, month, quarter, week, day_of_week, is_weekend, month_name

AGGREGATION TABLES (pre-summed — prefer these for totals):
  agg_by_category:   category, total_revenue, total_net_revenue, total_net_profit, total_units_sold, total_discount, total_returns, avg_profit_margin_pct
  agg_by_channel:    channel, total_revenue, total_units_sold, total_net_profit
  agg_by_customer:   customer_id, total_revenue, total_orders, total_units, total_net_profit, avg_order_value
  agg_by_product:    product_id, product_name, category, total_revenue, total_units_sold, total_net_profit, avg_profit_margin_pct
  agg_by_region:     region, total_revenue, total_net_profit, total_units_sold
  agg_by_vendor:     vendor_id, total_revenue, total_orders, total_units, total_net_profit
  agg_daily_trend:   date, year, month, quarter, daily_revenue, daily_profit, daily_units
  agg_by_year_month: year, month, quarter, total_revenue, total_net_profit, total_units, total_orders

RELATIONSHIPS:
  fact_sales.product_id  -> dim_product.product_id
  fact_sales.customer_id -> dim_customer.customer_id
  fact_sales.vendor_id   -> dim_vendor.vendor_id
  fact_sales.region      -> dim_geography.region
  fact_sales.channel     -> dim_channel.channel
  fact_sales.date        -> dim_date.date
"""

_FALLBACK_INVALID_COMBOS = [
    ("agg_by_year_month", "total_units_sold",  "total_units"),
    ("agg_by_year_month", "total_net_revenue",  "total_revenue"),
    ("agg_by_customer",   "total_units_sold",  "total_units"),
    ("agg_by_vendor",     "total_units_sold",  "total_units"),
    ("agg_daily_trend",   "total_revenue",     "daily_revenue"),
    ("agg_daily_trend",   "total_net_profit",  "daily_profit"),
    ("agg_daily_trend",   "total_units_sold",  "daily_units"),
    ("agg_daily_trend",   "total_units",       "daily_units"),
]


def _build_schema_from_live(table_columns: dict) -> tuple:
    """
    Given a dict of {table_name: [col1, col2, ...]} fetched from INFORMATION_SCHEMA,
    builds:
      1. A formatted schema string for the LLM prompt
      2. A list of (table, bad_col, good_col) invalid-combo tuples for quality checks
    """
    # Classify tables
    fact_tables = sorted([t for t in table_columns if t.startswith("fact_")])
    dim_tables  = sorted([t for t in table_columns if t.startswith("dim_")])
    agg_tables  = sorted([t for t in table_columns if t.startswith("agg_")])
    other       = sorted([t for t in table_columns
                          if not t.startswith(("fact_", "dim_", "agg_", "query_"))])

    lines = [
        f"Database: {SQL_DATABASE}   Schema: dbo   Dialect: T-SQL (SQL Server / Fabric SQL Analytics endpoint)",
        f"[Schema fetched live from endpoint — {len(table_columns)} tables]",
        "",
    ]

    # ── FACT TABLES
    lines.append("════════════════════════════════════════════════════════════")
    lines.append("FACT TABLES  (use for row-level detail or custom filters)")
    lines.append("════════════════════════════════════════════════════════════")
    for t in fact_tables:
        cols    = table_columns[t]
        purpose = _TABLE_PURPOSE.get(t, "fact table")
        tschema = _TABLE_SCHEMA_MAP.get(t, "dbo")
        lines.append(f"  {tschema}.{t}  — {purpose}")
        # Print columns in groups of 6 for readability
        for i in range(0, len(cols), 6):
            prefix = "    " if i == 0 else "    "
            lines.append(prefix + ", ".join(cols[i:i+6]))
        lines.append("")

    # ── DIM TABLES
    lines.append("════════════════════════════════════════════════════════════")
    lines.append("DIMENSION TABLES  (use for attribute lookups and filters)")
    lines.append("════════════════════════════════════════════════════════════")
    for t in dim_tables:
        cols    = table_columns[t]
        purpose = _TABLE_PURPOSE.get(t, "dimension table")
        lines.append(f"  {t}  — {purpose}")
        lines.append("    " + ", ".join(cols))
        lines.append("")

    # ── AGG TABLES  (most important — with ✓/✗ annotations)
    lines.append("════════════════════════════════════════════════════════════")
    lines.append("AGGREGATION TABLES  (pre-summed — PREFER THESE for totals)")
    lines.append("⚠ CRITICAL: column names differ between tables.")
    lines.append("  ALWAYS check the EXACT COLUMNS list before using a column.")
    lines.append("════════════════════════════════════════════════════════════")
    lines.append("")

    # Track which tables have which unit columns for quick reference
    unit_col_map = {}  # col_name → [table, ...]
    ALL_UNIT_COLS = {"total_units_sold", "total_units", "daily_units",
                     "total_orders", "avg_order_value", "total_net_revenue",
                     "total_discount", "total_returns", "avg_profit_margin_pct",
                     "avg_order_value", "daily_revenue", "daily_profit"}

    for t in agg_tables:
        cols    = table_columns[t]
        col_set = set(cols)
        purpose = _TABLE_PURPOSE.get(t, "aggregation table")
        lines.append(f"  {t}  → USE FOR: {purpose}")
        lines.append(f"    EXACT COLUMNS: {', '.join(cols)}")

        # Build has/not annotations for common unit columns
        has_cols = []
        not_cols = []
        for uc in ["total_units_sold", "total_units", "daily_units",
                   "total_orders", "avg_order_value"]:
            if uc in col_set:
                has_cols.append(uc)
                unit_col_map.setdefault(uc, []).append(t)
            else:
                not_cols.append(uc)
        if has_cols:
            lines.append(f"    ✓ HAS: {', '.join(has_cols)}")
        if not_cols:
            lines.append(f"    ✗ NO:  {', '.join(not_cols)}")
        lines.append("")

    # ── Quick units column reference (auto-generated)
    lines.append("════════════════════════════════════════════════════════════")
    lines.append("QUICK COLUMN REFERENCE — units/orders column by table")
    lines.append("════════════════════════════════════════════════════════════")
    for uc, tables in sorted(unit_col_map.items()):
        lines.append(f"  {uc:<20} → {', '.join(tables)}")
    # fact_sales raw column
    if fact_tables:
        ft   = fact_tables[0]
        fcols = table_columns[ft]
        if "units_sold" in fcols:
            lines.append(f"  {'units_sold':<20} → {ft} (raw, not pre-aggregated)")
    lines.append("")

    # ── Uploaded tables (from _upload_registry) — injected at schema build time
    upload_tables_in_schema = [
        t for t in table_columns
        if t.startswith("upload_")
    ]
    if upload_tables_in_schema:
        lines.append("════════════════════════════════════════════════════════════")
        lines.append("UPLOADED USER FILES  (temporary — session only)")
        lines.append("These tables were uploaded by users for comparison with Fabric data.")
        lines.append("They live in dbo schema alongside original tables.")
        lines.append("════════════════════════════════════════════════════════════")
        lines.append("")
        for t in upload_tables_in_schema:
            cols    = table_columns[t]
            # Find matching columns with Fabric tables
            fabric_join_cols = [
                c for c in cols
                if c in {"region", "channel", "category", "product_id",
                         "customer_id", "vendor_id", "date", "year", "month"}
            ]
            # Get filename from registry if available
            # _upload_registry values are LISTS of entries, not single dicts
            reg_entry = next(
                (e for entries in _upload_registry.values()
                 for e in (entries if isinstance(entries, list) else [entries])
                 if e.get("table_name") == t),
                None
            )
            fname_hint = f" — originally: {reg_entry['filename']}" if reg_entry and reg_entry.get("filename") else ""
            actual_schema = _TABLE_SCHEMA_MAP.get(t, "dbo")
            lines.append(f"  {actual_schema}.{t}  — USER UPLOADED FILE (temporary, session-scoped){fname_hint}")
            lines.append(f"    COLUMNS: {', '.join(cols)}")
            if fabric_join_cols:
                lines.append(f"    JOIN HINTS: can join to Fabric tables on: {', '.join(fabric_join_cols)}")
                lines.append(f"    EXAMPLE JOIN: SELECT u.*, f.revenue FROM {actual_schema}.{t} u")
                lines.append(f"                  JOIN dbo.fact_sales f ON u.{fabric_join_cols[0]} = f.{fabric_join_cols[0]}")
            lines.append("")
        lines.append("COMPARISON RULES FOR UPLOADED TABLES:")
        lines.append("  - Always use the exact schema prefix shown above (may be dbo or default depending on Lakehouse settings)")
        lines.append("  - Use LEFT JOIN to preserve all rows from the uploaded file")
        lines.append("  - Uploaded columns may differ in name from Fabric — check schema above")
        lines.append("  - For revenue comparison: uploaded net_sales ≈ fact_sales.net_revenue")
        lines.append("  - For profit comparison: uploaded profit ≈ fact_sales.net_profit")
        lines.append("  - For quantity comparison: uploaded quantity ≈ fact_sales.units_sold")
        lines.append("")

    # ── Relationships (static)
    lines.append(_RELATIONSHIPS)

    schema_str = "\n".join(lines)

    # ── Build invalid column combos dynamically
    # For each agg table, find unit-like columns it does NOT have
    # and generate warnings for columns other agg tables DO have
    all_unit_variants = {"total_units_sold", "total_units", "daily_units"}
    invalid_combos = []
    for t in agg_tables:
        col_set = set(table_columns[t])
        for bad_col in all_unit_variants - col_set:
            # Find a good alternative in this table
            good_col = next((c for c in all_unit_variants if c in col_set), None)
            if not good_col:
                # Check daily variants
                daily_variants = {"daily_revenue", "daily_profit", "daily_units"}
                good_col = next((c for c in daily_variants if c in col_set), "a valid column")
            invalid_combos.append((t, bad_col, good_col))
        # Also flag total_net_revenue on tables that don't have it
        if "total_net_revenue" not in col_set and "total_revenue" in col_set:
            invalid_combos.append((t, "total_net_revenue", "total_revenue"))
        # Flag daily_revenue/profit/units on non-daily agg tables
        if t != "agg_daily_trend":
            for dc in ["daily_revenue", "daily_profit", "daily_units"]:
                if dc not in col_set:
                    pass  # only flag if LLM tries to use it on this table

    return schema_str, invalid_combos


def _is_dangerous_sql(sql):
    """True if SQL contains any DML/DDL. Structural patterns only — no hardcoded names."""
    sql_clean = re.sub(r'--[^\n]*', ' ', sql)
    sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)
    sql_upper = sql_clean.upper().strip()
    dangerous_patterns = [
        r'^\s*UPDATE\s+', r'^\s*DELETE\s+', r'^\s*INSERT\s+', r'^\s*DROP\s+',
        r'^\s*ALTER\s+',  r'^\s*TRUNCATE\s+', r'^\s*CREATE\s+', r'^\s*REPLACE\s+',
        r'^\s*MERGE\s+',  r'^\s*EXEC\s+', r'^\s*EXECUTE\s+', r'^\s*GRANT\s+', r'^\s*REVOKE\s+',
        r';\s*UPDATE\s+', r';\s*DELETE\s+', r';\s*INSERT\s+', r';\s*DROP\s+', r';\s*ALTER\s+',
    ]
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            return True
    return False


_READONLY_MSG = "SELECT 'Data modification is not permitted. This system is read-only.' AS message"
def _groq_text(response, default=""):
    content = response.choices[0].message.content
    return content.strip() if content else default

def _check_sql_quality(sql):
    """Lightweight structural checks. Returns a list of issue strings (empty == clean)."""
    issues = []
    sql_lower = sql.lower()

    # CASE WHEN used to pivot a dimension value
    if re.findall(r'CASE\s+WHEN\s+\w+\.\w+\s*=\s*[\'"][^\'"]+[\'"]', sql, re.IGNORECASE):
        issues.append("CASE WHEN pivot detected")
    # LIKE with % wildcard where an exact match was likely intended
    if re.findall(r"LIKE\s+['\"]%[^'\"]*%?['\"]", sql, re.IGNORECASE):
        issues.append("LIKE wildcard detected")
    # Spark-isms that will fail on the T-SQL endpoint
    if re.search(r'\bCURRENT_DATE\b', sql, re.IGNORECASE):
        issues.append("CURRENT_DATE is not valid T-SQL (use GETDATE())")
    if re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE):
        issues.append("LIMIT is not valid T-SQL (use TOP N)")
    if re.search(r'\bQUARTER\s*\(', sql, re.IGNORECASE):
        issues.append("QUARTER() is not valid T-SQL (use DATEPART(QUARTER, ...))")
    # Invalid column name on specific agg tables (from live schema)
    try:
        _, invalid_combos = get_schema_context()
        for table, bad_col, good_col in invalid_combos:
            if table in sql_lower and bad_col in sql_lower:
                issues.append(f"{bad_col} does not exist on {table} — use {good_col}")
    except Exception:
        pass  # don't block SQL generation if schema fetch fails
    return issues


def get_fabric_token():
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]
    url  = f"https://login.microsoftonline.com/{FABRIC_TENANT_ID}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type":    "password",
        "client_id":     FABRIC_CLIENT_ID,
        "client_secret": FABRIC_CLIENT_SECRET,
        "username":      FABRIC_USER_EMAIL,
        "password":      FABRIC_USER_PASSWORD,
        "scope":         "https://api.fabric.microsoft.com/.default"
    })
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"]      = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600)
    print("  Token refreshed")
    return _token_cache["token"]


def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={FABRIC_USER_EMAIL};"
        f"PWD={FABRIC_USER_PASSWORD};"
        f"Authentication=ActiveDirectoryPassword;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, timeout=30)




def get_schema_context() -> tuple:
    """
    Returns (schema_string, invalid_combos_list).
    Fetches live from SQL Analytics endpoint, caches for 6 hours.
    Falls back to hardcoded schema on any error.
    """
    now = time.time()
    if (_SCHEMA_CACHE["schema"] is not None
            and now - _SCHEMA_CACHE["fetched_at"] < _SCHEMA_TTL):
        return _SCHEMA_CACHE["schema"], _SCHEMA_CACHE["invalid_combos"]

    print("  [Schema] Fetching live schema from SQL Analytics endpoint...")
    try:
        conn   = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME NOT IN ('query_result')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)
        rows = cursor.fetchall()
        conn.close()

        # Group columns by table, track schema for upload_ tables
        table_columns  = {}
        table_schemas  = {}   # table_name -> schema
        for table_schema, table_name, col_name in rows:
            table_columns.setdefault(table_name, []).append(col_name)
            table_schemas[table_name] = table_schema

        # Store schema map globally so SQL generation can use correct prefix
        _TABLE_SCHEMA_MAP.update(table_schemas)
        print(f"  [Schema] Upload table schemas: { {k:v for k,v in table_schemas.items() if k.startswith('upload_')} }")

        if not table_columns:
            raise ValueError("No tables returned from INFORMATION_SCHEMA")

        schema_str, invalid_combos = _build_schema_from_live(table_columns)

        _SCHEMA_CACHE["schema"]        = schema_str
        _SCHEMA_CACHE["invalid_combos"] = invalid_combos
        _SCHEMA_CACHE["fetched_at"]    = now

        print(f"  [Schema] Fetched {len(table_columns)} tables successfully.")
        return schema_str, invalid_combos

    except Exception as e:
        print(f"  [Schema] Live fetch failed: {e} — using fallback schema.")
        _SCHEMA_CACHE["schema"]        = _FALLBACK_SCHEMA
        _SCHEMA_CACHE["invalid_combos"] = _FALLBACK_INVALID_COMBOS
        _SCHEMA_CACHE["fetched_at"]    = now  # cache fallback too to avoid hammering
        return _FALLBACK_SCHEMA, _FALLBACK_INVALID_COMBOS

SQL_RULES = """
CRITICAL RULES — READ ALL BEFORE WRITING SQL:

RULE 0 — READ-ONLY:
  Generate only SELECT statements. NEVER UPDATE, INSERT, DELETE, DROP, ALTER, TRUNCATE,
  CREATE, MERGE, or EXEC. If the user asks to modify data, return exactly:
  SELECT 'Data modification is not permitted. This system is read-only.' AS message

RULE 1 — USE THE RIGHT TABLE:
  Always use the agg_ table that matches the user's question dimension.
  ┌─────────────────────────────────────────────────────────────────┐
  │ QUESTION ABOUT       → USE TABLE          → KEY COLUMNS         │
  ├─────────────────────────────────────────────────────────────────┤
  │ category/product type → agg_by_category   → total_units_sold    │
  │ channel/platform      → agg_by_channel    → total_units_sold    │
  │ customer              → agg_by_customer   → total_units         │
  │ product               → agg_by_product    → total_units_sold    │
  │ region/geography      → agg_by_region     → total_units_sold    │
  │ vendor/supplier       → agg_by_vendor     → total_units         │
  │ daily trend           → agg_daily_trend   → daily_units         │
  │ monthly/yearly total  → agg_by_year_month → total_units         │
  │ row-level / filter    → fact_sales + JOIN → units_sold          │
  └─────────────────────────────────────────────────────────────────┘
  Do NOT re-aggregate fact_sales when an agg_ table exists for the dimension.

RULE 2 — VALID COLUMNS ONLY (STRICTLY ENFORCED):
  ⚠ BEFORE writing any column name, verify it exists in the schema above.
  COMMON MISTAKES TO AVOID:
  - agg_by_year_month has NO column called total_units_sold → use total_units
  - agg_by_customer   has NO column called total_units_sold → use total_units
  - agg_by_vendor     has NO column called total_units_sold → use total_units
  - agg_daily_trend   has NO column called total_revenue    → use daily_revenue
  - agg_daily_trend   has NO column called total_units_sold → use daily_units
  - agg_by_year_month has NO column called total_net_revenue
  - dim_vendor has country, NOT region — do NOT mix them
  If a needed column does not exist on the agg table, fall back to fact_sales with JOINs.

RULE 3 — FILTER CORRECTLY:
  - Use exact match (=) not LIKE with % for single-value filters.
  - "country" / "vendor country" → dim_vendor.country
  - "region" / "geographic region" → fact_sales.region or dim_geography.region
    country and region are DIFFERENT — never substitute one for the other.
  - "last quarter" in T-SQL:
      WHERE quarter = CASE WHEN DATEPART(QUARTER, GETDATE()) > 1
                           THEN DATEPART(QUARTER, GETDATE()) - 1 ELSE 4 END
        AND year    = CASE WHEN DATEPART(QUARTER, GETDATE()) > 1
                           THEN YEAR(GETDATE()) ELSE YEAR(GETDATE()) - 1 END

RULE 4 — NO PIVOTING:
  Never use CASE WHEN to pivot dimension values into columns.
  Use GROUP BY across all dimensions instead.

RULE 5 — RETURN ONLY ASKED COLUMNS:
  Select only what the user asked for.
  Exception: for a percentage/rate, also include the raw numerator and denominator.

RULE 6 — LIMIT ONLY WHEN ASKED:
  Add TOP N only when the user says "top N" / "bottom N".
  T-SQL uses TOP placed right after SELECT, e.g. SELECT TOP 10 ...

RULE 7 — COMPLEX PATTERNS (T-SQL):
  - Use a CTE (WITH) for multi-step calculations.
  - DENSE_RANK() for rankings with ties; window alias cannot be used in WHERE — wrap in CTE:
      WITH ranked AS (SELECT ..., DENSE_RANK() OVER (PARTITION BY <group> ORDER BY <metric> DESC) AS rnk FROM ...)
      SELECT * FROM ranked WHERE rnk <= 3
  - RANKINGS MUST BE SORTED IN THE OUTPUT: the OUTER query that selects from the
    ranked CTE MUST end with an explicit ORDER BY so rows display in rank order.
    For "rank products by revenue within each category":
      WITH ranked AS (
        SELECT category, product_name, SUM(revenue) AS total_revenue,
               DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) AS rnk
        FROM dbo.fact_sales GROUP BY category, product_name)
      SELECT category, product_name, total_revenue, rnk
      FROM ranked
      ORDER BY category ASC, total_revenue DESC   -- REQUIRED: sort the final output
    Never return a ranked result without an outer ORDER BY — unsorted ranks look broken.
  - LAG/LEAD: compute in a CTE, then SELECT from it.
  - Running totals: SUM(...) OVER (ORDER BY col ROWS UNBOUNDED PRECEDING).
  - Safe division: value / NULLIF(divisor, 0).

RULE 8 — DATE HANDLING (T-SQL):
  - Use the year / month / quarter columns directly when they exist on the table.
  - Otherwise: YEAR(date), MONTH(date), DATEPART(QUARTER, date).
  - Date ranges: date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'.
  - Current year: YEAR(GETDATE())   Current quarter: DATEPART(QUARTER, GETDATE())
  - RELATIVE DATES — ALWAYS anchor to GETDATE(), never a hardcoded date:
      "last N days"      → date >= DATEADD(day,   -N, CAST(GETDATE() AS date))
      "last 7 days"      → date >= DATEADD(day,   -7, CAST(GETDATE() AS date))
      "last 30 days"     → date >= DATEADD(day,  -30, CAST(GETDATE() AS date))
      "last N months"    → date >= DATEADD(month, -N, CAST(GETDATE() AS date))
      "last 6 months"    → date >= DATEADD(month, -6, CAST(GETDATE() AS date))
      "last week"        → date >= DATEADD(week,  -1, CAST(GETDATE() AS date))
      "last full business week" → the most recent completed Mon–Fri:
          date >= DATEADD(day, -(DATEPART(weekday, GETDATE()) + 6), CAST(GETDATE() AS date))
          AND date <  DATEADD(day, -(DATEPART(weekday, GETDATE()) - 1), CAST(GETDATE() AS date))
      "haven't ordered in the last 6 months but ordered before" →
          customers whose MAX(date) < DATEADD(month,-6,GETDATE())
          AND MIN(date) < DATEADD(month,-6,GETDATE())   (anti-recent, has-history)
  - NEVER answer a relative-date question with a fixed literal date — it must be
    computed from GETDATE() so it stays correct over time.

RULE 8b — INVALID / IMPOSSIBLE DATES:
  - Months are 1–12 only. If the user references an impossible period (e.g. the
    "13th month", "month 0", a 32nd day), DO NOT invent or silently clamp it.
    Return exactly this so the bot can flag it:
      SELECT 'INVALID_DATE: <short reason>' AS message
    e.g. "13th month of 2024" → SELECT 'INVALID_DATE: month 13 does not exist (valid 1-12)' AS message

RULE 9 — SEMANTIC INTERPRETATION:
  - "VIP" / "high value" customers → high dim_customer.lifetime_value (NOT a segment called 'VIP')
  - dim_customer.segment values are ONLY: Premium, Standard, Budget, Enterprise, SMB
  - "test accounts" → email NOT LIKE '%@test%' AND customer_name NOT LIKE '%test%'
  - "non-preferred vendor" → is_preferred = 0  (bit column; use 0/1 not TRUE/FALSE)
  - "sales by year" / "annual trend" → use agg_by_year_month GROUP BY year ORDER BY year ASC
  - "monthly trend" → use agg_by_year_month GROUP BY year, month ORDER BY year, month ASC
  - "daily trend" → use agg_daily_trend ORDER BY date ASC

RULE 9b — GEOGRAPHY & MISSING DIMENSIONS:
  - The database has NO "state", "city", or "zip" columns. The only geographic
    dimension is `region` (in fact_sales / dim_geography / agg_by_region) and
    `country` (only in dim_vendor, for vendors).
  - If the user asks for a geographic grain that does not exist (state, city,
    province, county, district), DO NOT silently substitute country. Instead use
    the closest available level, which is `region`, and alias it clearly as region
    so the answer is honest about what was grouped:
      "which state buys the most" → SELECT region, SUM(revenue) AS total_revenue
        FROM dbo.fact_sales GROUP BY region ORDER BY total_revenue DESC
  - Likewise, if the user asks for "subcategory" (which does not exist), use
    `category`. Never invent a column that is not in the schema.
  - TIME-OF-DAY: the `date` column is DATE-ONLY (no hour/minute). There is NO
    way to answer "busiest hour", "time of day", "morning vs evening", or any
    hourly breakdown. For such requests, return exactly:
      SELECT 'NO_TIMEOFDAY: order data is recorded by date only, not time of day' AS message
    Do NOT fabricate an hour with DATEPART(HOUR, date) — it would always be 0.

RULE 9c — BASKET / COMBINATION & SIMILARITY ANALYSIS:
  - There is NO order-header table: each fact_sales row is ONE product bought by
    one customer on one date (sale_id = line grain).
  - "products bought together" / "product combinations" → self-join fact_sales
    on the same customer AND same date, pairing different products:
      SELECT TOP 10 a.product_name AS product_1, b.product_name AS product_2,
             COUNT(*) AS times_bought_together
      FROM dbo.fact_sales a
      JOIN dbo.fact_sales b
        ON a.customer_id = b.customer_id AND a.date = b.date
       AND a.product_id < b.product_id      -- avoid self-pairs and mirrored duplicates
      GROUP BY a.product_name, b.product_name
      ORDER BY times_bought_together DESC
  - "customers with a similar purchase pattern to X" → similarity must consider
    MULTIPLE dimensions, not just total revenue/units. Compare on at least:
    total revenue, total units, number of orders (COUNT of sales), average order
    value, AND number of distinct categories purchased. Compute the target
    customer's profile in a CTE, then rank other customers by closeness across
    those dimensions (e.g. sum of normalised absolute differences), ascending.

RULE 10 — UPLOADED FILE COMPARISONS:
  - Uploaded tables are named upload_{username} and live in dbo schema.
  - Always use LEFT JOIN to keep all rows from uploaded table.
  - Map uploaded columns to Fabric columns for comparison:
      uploaded net_sales      ≈ fact_sales.net_revenue
      uploaded gross_sales    ≈ fact_sales.revenue
      uploaded profit         ≈ fact_sales.net_profit
      uploaded quantity       ≈ fact_sales.units_sold
      uploaded profit_margin_pct ≈ fact_sales.profit_margin_pct
      uploaded discount_pct   ≈ fact_sales.discount_pct
  - For side-by-side comparison use CTEs:
      WITH uploaded AS (SELECT region, SUM(net_sales) AS uploaded_revenue FROM dbo.upload_X GROUP BY region),
           fabric   AS (SELECT region, SUM(net_revenue) AS fabric_revenue FROM dbo.fact_sales GROUP BY region)
      SELECT COALESCE(u.region, f.region) AS region,
             u.uploaded_revenue, f.fabric_revenue
      FROM uploaded u FULL OUTER JOIN fabric f ON u.region = f.region
      ORDER BY fabric_revenue DESC
  - Always check the UPLOADED USER FILES section in the schema for exact column names.

T-SQL SYNTAX REQUIREMENTS:
  - T-SQL ONLY — NOT Spark SQL, NOT MySQL, NOT PostgreSQL.
  - GETDATE() not CURRENT_DATE; YEAR(GETDATE()) for current year.
  - TOP N not LIMIT N.
  - DATEPART(QUARTER, col) not QUARTER(col).
  - dbo. prefix on EVERY table name.
  - Alias all aggregations (e.g. SUM(revenue) AS total_revenue).
  - HAVING for post-aggregation filters.
  - ORDER BY the main metric DESC unless it is a time series (then ASC).
  - Return ONLY the SQL — no markdown fences, no explanation, no comments.
"""


def trigger_notebook(user_query, user_name, job_id):
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url     = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{FABRIC_NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    )
    body = {
        "executionData": {
            "parameters": {
                "USER_QUERY": {"value": user_query, "type": "string"},
                "USER_NAME":  {"value": user_name,  "type": "string"},
                "JOB_ID":     {"value": job_id,     "type": "string"}
            }
        }
    }
    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code not in (200, 202):
        if resp.status_code == 404:
            raise Exception(
                f"Notebook not found (404). The FABRIC_NOTEBOOK_ID in .env is stale.\n"
                f"Current ID: {FABRIC_NOTEBOOK_ID}\n"
                f"Go to Fabric workspace → find AiRo_Bot_Notebook_Dev → "
                f"copy the ID from the URL and update FABRIC_NOTEBOOK_ID in .env"
            )
        raise Exception(f"Notebook trigger failed: {resp.status_code} {resp.text}")
    location = resp.headers.get("Location", "")
    print(f"Notebook triggered. Job ID: {job_id} Polling URL: {location}")
    return location


def get_job_status(location):
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp    = requests.get(location, headers=headers, timeout=10)
    return resp.json().get("status", "Unknown")


def get_report_url(user_name):
    token       = get_fabric_token()
    headers     = {"Authorization": f"Bearer {token}"}
    report_name = f"PowerBI1_{user_name}_Report"
    items_url   = (
        f"https://api.fabric.microsoft.com/v1/workspaces/"
        f"{FABRIC_WORKSPACE_ID}/items"
    )
    resp = requests.get(items_url, headers=headers)
    if resp.status_code == 200:
        items = resp.json().get("value", [])
        reports = [
            item for item in items
            if item.get("type") == "Report"
            and item.get("displayName", "") == report_name
        ]
        if reports:
            reports_sorted = sorted(
                reports,
                key=lambda x: x.get("modifiedDateTime", ""),
                reverse=True
            )
            report_id = reports_sorted[0]["id"]
            return (
                f"https://app.powerbi.com/groups/"
                f"{FABRIC_WORKSPACE_ID}/reports/{report_id}"
            )
    return None




# ── Upload registry ────────────────────────────────────────────────────────────
# Tracks uploaded files for the current session.
# Structure: {user_name: [{table_name, filename, columns, row_count, status, uploaded_at}, ...]}
# Each user can have MULTIPLE uploaded files simultaneously.
_upload_registry = {}   # user_name -> list of upload entries
_chart_store     = {}   # chart_id  -> {rows_data, x_column, y_column, title, query, stored_at}


# ── Fabric file helpers ────────────────────────────────────────────────────────

def list_lakehouse_files(folder_path):
    """
    List all files in a Lakehouse folder via Fabric Lakehouse API.
    Returns list of dicts with "name" key (filename only).
    """
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/lakehouses/{FABRIC_LAKEHOUSE_ID}/files"
        f"?directory={folder_path}"
    )
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 200:
        items = resp.json().get("value", [])
        files = []
        for item in items:
            name = item.get("name", "")
            # Only include files, not subdirectories
            if name and not item.get("isDirectory", False):
                fname = name.split("/")[-1]
                files.append({"name": fname})
        return files
    elif resp.status_code == 404:
        return []
    else:
        print(f"  [Upload] list_lakehouse_files error: {resp.status_code} {resp.text[:200]}")
        return []


def delete_lakehouse_file(file_path):
    """
    Delete a single file from OneLake Files via DFS API.
    SAFETY: only deletes files inside FABRIC_UPLOAD_FOLDER whose name starts with upload_
    """
    # Safety Layer 1 — must be inside uploads folder
    if not file_path.startswith(FABRIC_UPLOAD_FOLDER):
        print(f"  [SAFETY] Blocked file deletion outside uploads folder: {file_path}")
        return False
    # Safety Layer 2 — filename must start with upload_
    filename = file_path.split("/")[-1]
    if not filename.startswith("upload_"):
        print(f"  [SAFETY] Blocked deletion of non-upload file: {filename}")
        return False
    try:
        token    = _get_onelake_token()
        dfs_path = f"/{FABRIC_WORKSPACE_ID}/{FABRIC_LAKEHOUSE_ID}/{file_path}"
        url      = f"https://onelake.dfs.fabric.microsoft.com{dfs_path}?recursive=false"
        resp     = requests.delete(url,
                                   headers={"Authorization": f"Bearer {token}"},
                                   timeout=15)
        ok = resp.status_code in (200, 202, 204, 404)
        print(f"  [Upload] Delete file: {resp.status_code} {file_path} {'✓' if ok else '✗'}")
        return ok
    except Exception as e:
        print(f"  [Upload] Delete file error (non-fatal): {e}")
        return False


def create_lakehouse_folder(folder_path):
    """
    Create a folder in the Lakehouse Files section via Fabric Lakehouse API.
    On Fabric, folders are virtual and auto-created on first file upload.
    This call is a best-effort no-op — we just verify the lakehouse is reachable.
    """
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    # Just ping the lakehouse to confirm connectivity
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/lakehouses/{FABRIC_LAKEHOUSE_ID}"
    )
    resp = requests.get(url, headers=headers, timeout=15)
    ok = resp.status_code in (200, 201)
    if not ok:
        print(f"  [Upload] create_lakehouse_folder ping: {resp.status_code} {resp.text[:200]}")
    return ok


def _get_onelake_token():
    """
    Get a token scoped for OneLake DFS (Azure Storage audience).
    Requires 'Azure Storage / user_impersonation' permission on the app registration.
    Cached separately from the Fabric API token.
    """
    now = time.time()
    if _onelake_token_cache["token"] and now < _onelake_token_cache["expires_at"] - 60:
        return _onelake_token_cache["token"]
    url  = f"https://login.microsoftonline.com/{FABRIC_TENANT_ID}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type":    "password",
        "client_id":     FABRIC_CLIENT_ID,
        "client_secret": FABRIC_CLIENT_SECRET,
        "username":      FABRIC_USER_EMAIL,
        "password":      FABRIC_USER_PASSWORD,
        "scope":         "https://storage.azure.com/user_impersonation"
    })
    if resp.status_code != 200:
        raise Exception(
            f"OneLake token failed {resp.status_code}: {resp.text[:300]}\n"
            "Ensure 'Azure Storage > user_impersonation' is added to your app's "
            "API permissions and admin consent is granted."
        )
    data = resp.json()
    _onelake_token_cache["token"]      = data["access_token"]
    _onelake_token_cache["expires_at"] = now + data.get("expires_in", 3600)
    print("  [Upload] OneLake token refreshed")
    return _onelake_token_cache["token"]


def upload_file_to_lakehouse(file_bytes, filename, folder_path):
    """
    Upload raw file bytes to OneLake via the DFS (ADLS Gen2) API.

    Requires Azure Storage / user_impersonation permission on the app registration:
    Azure Portal → App registrations → {app} → API permissions →
    Add permission → Azure Storage → Delegated → user_impersonation → Grant admin consent

    DFS protocol (3 steps):
      1. PUT  ?resource=file          — create empty file
      2. PATCH ?action=append         — stream data
      3. PATCH ?action=flush          — commit
    """
    token    = _get_onelake_token()
    dfs_path = f"/{FABRIC_WORKSPACE_ID}/{FABRIC_LAKEHOUSE_ID}/{folder_path}/{filename}"
    base_url = f"https://onelake.dfs.fabric.microsoft.com{dfs_path}"
    headers  = {"Authorization": f"Bearer {token}"}
    size     = len(file_bytes)

    print(f"  [Upload] DFS path: {dfs_path}")
    print(f"  [Upload] File size: {size/1024:.1f} KB")

    # Step 1 — create file
    r1 = requests.put(base_url + "?resource=file", headers=headers, timeout=30)
    print(f"  [Upload] Create: {r1.status_code} {r1.text[:200] if r1.text else ''}")
    if r1.status_code not in (200, 201, 202):
        raise Exception(f"OneLake create failed: {r1.status_code} {r1.text[:300]}")

    # Step 2 — append data
    r2 = requests.patch(
        base_url + f"?action=append&position=0",
        headers={**headers, "Content-Type": "application/octet-stream",
                 "Content-Length": str(size)},
        data=file_bytes, timeout=120
    )
    print(f"  [Upload] Append: {r2.status_code} {r2.text[:200] if r2.text else ''}")
    if r2.status_code not in (200, 202):
        raise Exception(f"OneLake append failed: {r2.status_code} {r2.text[:300]}")

    # Step 3 — flush / commit
    r3 = requests.patch(
        base_url + f"?action=flush&position={size}",
        headers=headers, timeout=30
    )
    print(f"  [Upload] Flush: {r3.status_code} {r3.text[:200] if r3.text else ''}")
    if r3.status_code not in (200, 202):
        raise Exception(f"OneLake flush failed: {r3.status_code} {r3.text[:300]}")

    print(f"  [Upload] ✓ Committed: {dfs_path}")
    return True


def load_file_as_delta_table(filename, folder_path, table_name):
    """
    Trigger Fabric Load Table API to convert a file in Lakehouse Files
    into a managed Delta table. Polls until Succeeded or Failed.
    """
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url     = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/lakehouses/{FABRIC_LAKEHOUSE_ID}/tables/{table_name}/load"
    )
    body = {
        "relativePath": f"{folder_path}/{filename}",
        "pathType":     "File",
        "mode":         "overwrite",
        "recursive":    False,
        "formatOptions": {
            "format":    "Csv",
            "header":    True,
            "delimiter": ","
        }
    }
    print(f"  [LoadTable] POST {url}")
    print(f"  [LoadTable] relativePath: {folder_path}/{filename}")
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    print(f"  [LoadTable] Response: {resp.status_code} {resp.text[:300] if resp.text else ''}")

    if resp.status_code in (200, 202):
        location = resp.headers.get("Location", "")
        if not location:
            print(f"  [LoadTable] ✓ Synchronous success (no Location header)")
            return True
        print(f"  [LoadTable] Polling: {location}")
        for attempt in range(60):   # up to 5 minutes
            time.sleep(5)
            poll = requests.get(location,
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=15)
            status = poll.json().get("status", "Unknown")
            print(f"  [LoadTable] Poll {attempt+1}: {status}")
            if status == "Succeeded":
                print(f"  [LoadTable] ✓ Table {table_name} created successfully")
                return True
            elif status in ("Failed", "Cancelled"):
                print(f"  [LoadTable] ✗ Failed: {poll.json()}")
                return False
        print(f"  [LoadTable] ✗ Timed out after 5 minutes")
        return False
    else:
        print(f"  [LoadTable] ✗ Error: {resp.status_code} {resp.text}")
        return False


def delete_lakehouse_table(table_name):
    """
    Delete a Delta table via AiRo_Upload_Notebook (MODE=delete) and
    also clean up the staging CSV from OneLake Files if it still exists.

    SAFETY: only deletes tables whose name starts with 'upload_'.
    Original tables (fact_sales, dim_*, agg_*) are never touched.
    """
    if not table_name.startswith("upload_"):
        print(f"  [SAFETY] Blocked deletion of non-upload table: {table_name}")
        return False

    # Best-effort: delete the staging CSV from Files/uploads/ if it still exists.
    # This handles cases where the post-upload cleanup failed (e.g. token error).
    csv_file_path = f"{FABRIC_UPLOAD_FOLDER}/{table_name}.csv"
    try:
        deleted = delete_lakehouse_file(csv_file_path)
        if deleted:
            print(f"  [Delete] Staging CSV cleaned up: {csv_file_path}")
    except Exception as fe:
        print(f"  [Delete] CSV cleanup skipped (non-fatal): {fe}")

    # Drop the Delta table via notebook (only way to run DDL)
    try:
        job_id  = str(uuid.uuid4())[:8]
        bot_url = f"{NGROK_DOMAIN}/api/upload_result"
        trigger_upload_notebook(
            table_name = table_name,
            job_id     = job_id,
            bot_url    = bot_url,
            mode       = "delete",
            user_name  = "system",
        )
        print(f"  [Delete] Notebook triggered for DROP TABLE {table_name} (job_id={job_id})")
        return True
    except Exception as e:
        print(f"  [Delete] Failed to trigger delete notebook for {table_name}: {e}")
        return False


# ── Startup cleanup ────────────────────────────────────────────────────────────

def startup_cleanup():
    """
    Called once on bot startup. Cleans up ALL upload_ Delta tables from
    previous sessions by scanning the Lakehouse tables list directly.

    Upload flow uses Files API + Load Table API (no notebook for uploads).
    Cleanup still uses the notebook for DROP TABLE since the SQL Analytics
    endpoint is read-only and can't execute DDL.

    Safety guarantees:
    - Only deletes tables whose name starts with 'upload_'
    - delete_lakehouse_table() enforces this independently
    - Original tables (fact_sales, dim_*, agg_*) are NEVER touched
    """
    print("  [Startup] ── Upload cleanup starting ──────────────────────")
    try:
        # ── Verify lakehouse is reachable
        create_lakehouse_folder(FABRIC_UPLOAD_FOLDER)
        print(f"  [Startup] Lakehouse reachable.")

        # ── Scan for upload_ tables via SQL Analytics endpoint (pyodbc)
        # More reliable than Fabric REST API — works regardless of schema settings
        try:
            conn   = get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME LIKE 'upload%'
            """)
            rows          = cursor.fetchall()
            # Extra filter in Python to ensure name starts with upload_ exactly
            upload_tables = [r[1] for r in rows if r[1].lower().startswith("upload_")]
            print(f"  [Startup] Raw query returned {len(rows)} row(s): {[r[1] for r in rows]}")
            # Also store schema mapping
            for r in rows:
                if r[1].lower().startswith("upload_"):
                    _TABLE_SCHEMA_MAP[r[1]] = r[0]
                    print(f"  [Startup] Found upload table: {r[0]}.{r[1]}")
            conn.close()

            if not upload_tables:
                print("  [Startup] No upload tables from previous session.")
            else:
                print(f"  [Startup] Found {len(upload_tables)} upload table(s): {upload_tables}")
                for tname in upload_tables:
                    ok = delete_lakehouse_table(tname)
                    # delete is async via notebook — ok=True means notebook was triggered
                    print(f"  [Startup] Delete notebook triggered: {tname} → {ok}")

        except Exception as sql_err:
            print(f"  [Startup] SQL scan failed: {sql_err} — using registry fallback")
            known = [e.get("table_name", "")
                     for entries in _upload_registry.values()
                     for e in (entries if isinstance(entries, list) else [entries])
                     if e.get("table_name", "").startswith("upload_")]
            if known:
                print(f"  [Startup] Registry fallback: deleting {known}")
                for tname in known:
                    delete_lakehouse_table(tname)
        _upload_registry.clear()

        print("  [Startup] ── Cleanup complete ────────────────────────────")


    except Exception as e:
        print(f"  [Startup] Cleanup error (non-fatal, bot will continue): {e}")


# ── Background TTL cleanup task ───────────────────────────────────────────────

async def upload_ttl_cleanup_task():
    """
    Background asyncio task — runs every hour.
    Deletes any uploaded Delta tables that are older than UPLOAD_TTL_SECONDS (24h).
    Runs independently of bot restarts — handles long-running sessions.
    """
    CHECK_INTERVAL = 3600  # check every hour
    print(f"  [TTL] Background cleanup task started (TTL={UPLOAD_TTL_SECONDS//3600}h, check every {CHECK_INTERVAL//3600}h)")

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now          = time.time()
        schema_dirty = False

        for uname, entries in list(_upload_registry.items()):
            if not isinstance(entries, list):
                continue
            still_valid = []
            for entry in entries:
                age = now - entry.get("uploaded_at", 0)
                if age > UPLOAD_TTL_SECONDS:
                    table_name = entry.get("table_name", "")
                    filename   = entry.get("filename", "unknown")
                    age_hours  = age / 3600
                    if not table_name.startswith("upload_"):
                        print(f"  [TTL][SAFETY] Skipping non-upload table: {table_name}")
                        still_valid.append(entry)
                        continue
                    print(f"  [TTL] Deleting expired: {table_name} (file: {filename}, age: {age_hours:.1f}h)")
                    ok = delete_lakehouse_table(table_name)
                    if ok:
                        schema_dirty = True
                        print(f"  [TTL] Deleted {table_name}.")
                    else:
                        print(f"  [TTL] Failed to delete {table_name} — will retry.")
                        still_valid.append(entry)
                else:
                    still_valid.append(entry)
            _upload_registry[uname] = still_valid

        if schema_dirty:
            _SCHEMA_CACHE["schema"]     = None
            _SCHEMA_CACHE["fetched_at"] = 0
            print(f"  [TTL] Schema cache invalidated.")

        # Clean up expired chart store entries (30 min TTL)
        chart_cutoff = now - 1800
        expired_charts = [cid for cid, cd in list(_chart_store.items())
                          if cd.get("stored_at", 0) < chart_cutoff]
        for cid in expired_charts:
            _chart_store.pop(cid, None)
        if expired_charts:
            print(f"  [TTL] Cleaned {len(expired_charts)} expired chart(s)")

        total_expired = sum(
            1 for entries in _upload_registry.values()
            if isinstance(entries, list)
            for e in entries
            if now - e.get("uploaded_at", 0) > UPLOAD_TTL_SECONDS
        )
        if total_expired == 0 and not schema_dirty:
            print(f"  [TTL] No expired uploads found.")


# ── File processing ────────────────────────────────────────────────────────────

# Max raw CSV size for the Fabric Files API upload path.
# Files API has no meaningful payload limit — 20MB is a generous sanity ceiling.
MAX_CSV_BYTES = 20 * 1024 * 1024   # 20MB


def _read_file_safe(file_bytes, ext, filename):
    """
    Safely read Excel or CSV into a pandas DataFrame.
    Returns (df, error_message) — error_message is None on success.

    Notes:
    - merge_cells is a WRITE-only parameter (to_excel), not valid for read_excel.
      Merged-cell handling is done post-read by _sanitise_columns(), which renames
      any "Unnamed: X" columns that pandas creates for merged/empty header cells.
    - CSV tries UTF-8 first, falls back to latin-1 for encoding issues.
    """
    try:
        if ext in ("xlsx", "xls"):
            df = pd.read_excel(
                io.BytesIO(file_bytes),
                header=0
                # Note: NO merge_cells here — that param is for to_excel() only.
                # Merged cells produce "Unnamed: X" headers which _sanitise_columns
                # handles by renaming them to col_0, col_1, etc.
            )
        else:
            # Try UTF-8 first, fall back to latin-1 for encoding issues
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding="latin-1")

        return df, None

    except Exception as e:
        return None, str(e)


def _validate_dataframe(df, filename):
    """
    Validate DataFrame after reading.
    Returns (cleaned_df, error_message) — error_message is None on success.
    Handles: empty files, all-NaN columns, completely blank rows.
    """
    if df is None or df.empty:
        return None, f"'{filename}' appears to be empty — no data rows found."

    # Drop completely blank rows
    df = df.dropna(how="all")
    if df.empty:
        return None, f"'{filename}' has no data rows after removing blank rows."

    # Drop completely blank columns
    df = df.dropna(axis=1, how="all")
    if df.empty or len(df.columns) == 0:
        return None, f"'{filename}' has no usable columns after removing blank columns."

    # Must have at least 1 data row
    if len(df) == 0:
        return None, f"'{filename}' has column headers but no data rows."

    # Must have at least 2 columns to be useful
    if len(df.columns) < 2:
        return None, f"'{filename}' has only {len(df.columns)} column — need at least 2."

    return df, None


def _sanitise_columns(df):
    """
    Sanitise column names for SQL compatibility.
    Handles merged cell artifacts (Unnamed: X), duplicates, special chars.
    """
    new_cols = []
    for i, col in enumerate(df.columns):
        col_str = str(col).strip()
        # Replace "Unnamed: X" (merged cell artifact) with "col_X"
        if col_str.startswith("Unnamed:") or col_str == "nan":
            col_str = f"col_{i}"
        # Lowercase + replace non-alphanumeric with underscore
        col_str = re.sub(r"[^a-zA-Z0-9_]", "_", col_str.lower())
        col_str = re.sub(r"_+", "_", col_str).strip("_")
        if not col_str:
            col_str = f"col_{i}"
        new_cols.append(col_str)

    # Deduplicate column names
    seen  = {}
    final = []
    for col in new_cols:
        if col in seen:
            seen[col] += 1
            final.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final.append(col)

    df.columns = final
    return df


def process_uploaded_file(file_bytes, filename, user_name):
    """
    Full pipeline with error handling for all edge cases:
    1. File type validation (test 3)
    2. File size validation (test 5)
    3. Safe read with merged cell handling (test 1)
    4. Empty file detection (test 2)
    5. Duplicate filename handling (test 4)
    6. Multi-file support — appends to registry (test 6)
    Returns (success: bool, job_id: str | error_msg: str)
    """
    try:
        # ── Test 3: Unsupported file type
        ext = filename.lower().rsplit(".", 1)[-1]
        if ext not in ("xlsx", "xls", "csv"):
            return False, (
                f"Unsupported file type: **.{ext}**\n"
                f"Please upload one of: .xlsx, .xls, .csv"
            )

        # Safety: sanitise names for table naming
        safe_user     = re.sub(r"[^a-zA-Z0-9_]", "_", user_name).lower()
        base_filename = filename.rsplit(".", 1)[0]
        safe_filename = re.sub(r"[^a-zA-Z0-9_]", "_", base_filename.strip().lower())
        safe_filename = re.sub(r"_+", "_", safe_filename).strip("_")
        if not safe_filename:
            safe_filename = "file"
        table_name = f"upload_{safe_user}_{safe_filename}"

        if not table_name.startswith("upload_"):
            return False, "[SAFETY] Invalid table name generated."

        # ── Test 5: File size check (before reading — fast fail)
        file_size_mb = len(file_bytes) / (1024 * 1024)
        if len(file_bytes) > 30 * 1024 * 1024:
            return False, (
                f"File too large: **{file_size_mb:.1f} MB**\n"
                f"Maximum allowed size is 15 MB."
            )

        # ── Test 1 + 2: Read file with merged cell handling
        print(f"  [Upload] Reading {filename} ({file_size_mb:.1f} MB)...")
        df, read_err = _read_file_safe(file_bytes, ext, filename)
        if read_err:
            return False, (
                f"Could not read '{filename}':\n`{read_err}`\n"
                f"Check that the file is not corrupted or password-protected."
            )

        # ── Test 2: Validate — empty file, blank rows/cols
        df, val_err = _validate_dataframe(df, filename)
        if val_err:
            return False, val_err

        # ── Test 1: Merged cell warning (non-fatal — we handle it)
        unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed:"))
        merged_warning = ""
        if unnamed_count > 0:
            merged_warning = f" (Note: {unnamed_count} merged/unnamed column(s) were auto-renamed)"
            print(f"  [Upload] {unnamed_count} merged/unnamed columns detected — auto-renamed.")

        # Sanitise column names
        df = _sanitise_columns(df)

        row_count = len(df)
        columns   = list(df.columns)
        print(f"  [Upload] Read {row_count} rows, {len(columns)} columns.{merged_warning}")

        # ── Serialise to CSV
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        csv_mb    = len(csv_bytes) / (1024 * 1024)
        if len(csv_bytes) > MAX_CSV_BYTES:
            return False, (
                f"File is too large: **{csv_mb:.1f} MB** (limit 20 MB).\n"
                f"Try uploading a smaller file or fewer columns."
            )
        print(f"  [Upload] CSV size: {csv_mb:.2f} MB")

        # ── Test 4: Duplicate filename handling
        norm_user      = user_name.lower()
        existing_entry = None
        if norm_user in _upload_registry:
            existing_entry = next(
                (e for e in _upload_registry[norm_user]
                 if e.get("table_name") == table_name),
                None
            )
        if existing_entry:
            status = existing_entry.get("status", "")
            if status == "pending":
                return False, (
                    f"**{filename}** is already being processed.\n"
                    f"Please wait for it to finish before uploading again."
                )
            elif status == "ready":
                print(f"  [Upload] Re-uploading {filename} — replacing existing table {table_name}")

        # Generate job ID (used for registry tracking)
        job_id = str(uuid.uuid4())[:8]

        # ── Step 1: Upload CSV to Lakehouse Files
        # Fabric Files API has no payload limit — works for files up to 20MB+.
        # We upload to Files/uploads/{table_name}.csv so it can be referenced
        # by the Load Table API in the next step.
        csv_filename = f"{table_name}.csv"
        print(f"  [Upload] Uploading CSV to Lakehouse Files ({csv_mb:.2f} MB)...")
        try:
            upload_file_to_lakehouse(csv_bytes, csv_filename, FABRIC_UPLOAD_FOLDER)
        except Exception as upload_err:
            return False, (
                f"Could not upload file to OneLake: {upload_err}"
            )

        # ── Step 2: Load Table API — Fabric reads the file and creates Delta table
        # This is synchronous (polls until Succeeded/Failed) and bypasses the
        # Fabric Jobs API 32KB payload limit entirely. No notebook needed for uploads.
        # ── Step 2: Trigger notebook to read CSV from OneLake and create Delta table
        # Load Table API doesn't work on schema-enabled Lakehouses.
        # The notebook reads the CSV directly from OneLake Files using Spark
        # and creates the Delta table — no base64 encoding needed.
        job_id  = str(uuid.uuid4())[:8]
        bot_url = f"{NGROK_DOMAIN}/api/upload_result"
        onelake_csv_path = f"{FABRIC_UPLOAD_FOLDER}/{csv_filename}"
        print(f"  [Upload] Triggering notebook for Delta table creation — job_id={job_id}...")
        try:
            location = trigger_upload_notebook(
                table_name   = table_name,
                job_id       = job_id,
                bot_url      = bot_url,
                mode         = "upload",
                file_content = onelake_csv_path,  # path, not content
                user_name    = safe_user,
            )
        except Exception as nb_err:
            # Notebook trigger failed — clean up the CSV and return error
            delete_lakehouse_file(onelake_csv_path)
            return False, f"Notebook trigger failed: {nb_err}"

        # ── Step 3: Register as pending — notebook will call /api/upload_result when done
        # CSV stays in OneLake until notebook reads it and creates the Delta table,
        # then notebook deletes it (see Cell 2 in AiRo_Upload_Notebook).
        entry = {
            "status":      "pending",
            "job_id":      job_id,
            "location":    location,
            "table_name":  table_name,
            "filename":    filename,
            "columns":     columns,
            "row_count":   row_count,
            "uploaded_at": time.time(),
        }
        norm_user = user_name.lower()
        if norm_user not in _upload_registry:
            _upload_registry[norm_user] = []
        _upload_registry[norm_user] = [
            e for e in _upload_registry[norm_user]
            if e.get("table_name") != table_name
        ]
        _upload_registry[norm_user].append(entry)
        print(f"  [Upload] Registry: {norm_user} now has {len(_upload_registry[norm_user])} file(s)")
        print(f"  [Upload] Notebook triggered — waiting for result via /api/upload_result")
        return True, job_id

    except Exception as e:
        print(f"  [Upload] process_uploaded_file error: {e}")
        return False, f"Error processing file: {str(e)}"


def trigger_upload_notebook(table_name, job_id, bot_url, mode="upload",
                            file_content="", user_name="system"):
    """
    Trigger AiRo_Upload_Notebook via Fabric Jobs API.

    MODE=upload: file_content is zlib-compressed + base64-encoded CSV.
                 Notebook decompresses: zlib.decompress(base64.b64decode(FILE_CONTENT))
    MODE=delete: drops the Delta table (file_content unused).

    zlib compression reduces CSV size by 75-85%, keeping payloads well under
    Fabric Jobs API's ~32KB JSON limit for files up to ~150KB raw CSV.
    """
    token   = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url     = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{FABRIC_UPLOAD_NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    )
    body = {
        "executionData": {
            "parameters": {
                "USER_NAME":    {"value": user_name,     "type": "string"},
                "FILE_CONTENT": {"value": file_content,  "type": "string"},
                "TABLE_NAME":   {"value": table_name,    "type": "string"},
                "JOB_ID":       {"value": job_id,        "type": "string"},
                "BOT_URL":      {"value": bot_url,       "type": "string"},
                "MODE":         {"value": mode,          "type": "string"},
            }
        }
    }
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    if resp.status_code not in (200, 202):
        raise Exception(f"Notebook trigger failed [{mode}]: {resp.status_code} {resp.text}")
    location = resp.headers.get("Location", "")
    print(f"  [Notebook] Job started. mode={mode} table={table_name} location={location}")
    return location


def get_upload_info(user_name):
    """Returns list of all upload entries for a user, or empty list."""
    norm = user_name.lower()
    return _upload_registry.get(norm, _upload_registry.get(user_name, []))


def get_ready_uploads(user_name):
    """Returns only the ready (successfully processed) uploads for a user."""
    return [e for e in get_upload_info(user_name) if e.get("status") == "ready"]


def get_upload_by_table(user_name, table_name):
    """Returns a specific upload entry by table name."""
    return next(
        (e for e in get_upload_info(user_name) if e.get("table_name") == table_name),
        None
    )


def generate_comparison_suggestion(user_name, user_query):
    """
    LLM generates a comparison SQL between the uploaded table and Fabric tables.
    Auto-detects matching columns and suggests a JOIN.
    """
    ready_uploads = get_ready_uploads(user_name)
    if not ready_uploads:
        return None, "No uploaded files found. Please upload a file first."

    # Force fresh schema fetch — upload tables must be visible
    _SCHEMA_CACHE["schema"]     = None
    _SCHEMA_CACHE["fetched_at"] = 0
    live_schema, _  = get_schema_context()

    # Build uploaded tables context for LLM
    uploaded_tables_context = ""
    for entry in ready_uploads:
        tname      = entry["table_name"]
        fname      = entry["filename"]
        cols       = entry["columns"]
        tschema    = _TABLE_SCHEMA_MAP.get(tname, "dbo")
        full_ref   = f"{tschema}.{tname}"
        print(f"  [Comparison] Available table: {full_ref}")
        uploaded_tables_context += f"""
TABLE: {full_ref}
FILE:  {fname}
COLUMNS: {", ".join(cols)}
"""

    prompt = f"""You are an expert T-SQL analyst. A user has uploaded one or more files that are available as Delta tables.

UPLOADED TABLES (choose the most relevant one for the user request):
{uploaded_tables_context}

FABRIC DATABASE SCHEMA:
{live_schema}

RULES:
- T-SQL only. Use correct schema prefix for each table (shown above).
- Choose the uploaded table that best matches the user request based on filename and column names.
- If the user mentions a specific file name, use that table.
- Find the best JOIN between the chosen uploaded table and an appropriate Fabric table.
- Common join columns: product_id, customer_id, vendor_id, region, channel, category.
- If no direct join exists, use a side-by-side CTE comparison with FULL OUTER JOIN.
- For revenue comparison: uploaded net_sales ≈ fact_sales.net_revenue
- For profit comparison: uploaded profit ≈ fact_sales.net_profit
- Return only SELECT columns useful for comparison.
- Return ONLY the SQL — no markdown, no explanation.

USER REQUEST: {user_query}

Generate the comparison T-SQL query:"""

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=700
    )
    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql, None


# ── LLM functions ─────────────────────────────────────────────────────────────

def classify_intent(user_message, conversation_history):
    # ── Fast-path: skip the LLM for obvious data queries ──────────────────────
    # Most analytics questions open with a clear data verb/phrase. Catching them
    # locally removes one LLM call per query — the single biggest token saver.
    # We only fast-path when there's NO sign of report/upload intent, so those
    # still go to the LLM for accurate routing.
    msg_l = user_message.lower().strip()
    _report_signals = ["report", "dashboard", "power bi", "powerbi", "pbix", "visual report"]
    _upload_signals = ["upload", "compare", ".csv", ".xlsx", "excel file", "my file", "attach"]
    _data_starts = ("show", "list", "get", "give", "find", "display", "what is",
                    "what are", "what's", "how many", "how much", "which",
                    "top ", "bottom ", "count ", "average", "total ", "sum of",
                    "who ", "identify", "breakdown")
    # Self-correcting / contradictory messages ("revenue... no wait, profit")
    # must NOT take the data fast-path — they need clarification. Detect the
    # tell-tale hedging/reversal markers and fall through to the LLM classifier.
    _contradiction_markers = ["actually no", "actually,", "wait,", "wait ", "no wait",
                              "never mind", "nevermind", "scratch that", "i mean",
                              "or maybe", "...", "hmm", "on second thought"]
    _looks_contradictory = sum(m in msg_l for m in _contradiction_markers) >= 1 and (
        msg_l.count("...") >= 1 or "wait" in msg_l or "actually" in msg_l
        or "never mind" in msg_l or "i mean" in msg_l
    )
    if not any(s in msg_l for s in _report_signals + _upload_signals):
        if not _looks_contradictory and (msg_l.startswith(_data_starts) or any(k in msg_l for k in
                    ("revenue", "sales", "customers", "products", "vendors",
                     "orders", "profit", "region", "category"))):
                print("  Intent: data_query (fast-path)")
                return "data_query"

    history_text = ""
    if conversation_history:
        recent = conversation_history[-6:]
        history_text = "\n".join([
            f"{m['role'].upper()}: {m['content']}"
            for m in recent
        ])

    prompt = f"""You are an intent classifier for a business intelligence bot.

Conversation history:
{history_text}

Current message: {user_message}

Classify the intent as exactly one of these:
- show_options: greetings (hi, hello), or the user is asking what the bot can do / what capabilities it has / wants to see a menu of options — e.g. "what can you help with", "tell me about yourself", "what can you do", "show me options". This is for vague exploratory openers, NOT for specific questions.
- chat: general conversation that is NOT a greeting or capability question, casual remarks, non-data questions
- data_query: user wants to see data, metrics, numbers, analysis in chat
- report_request: user explicitly wants a Power BI report, dashboard, visual report
- clarification_needed: the data request is too vague to act on, OR it is self-contradictory / the user changes their mind mid-message (e.g. "revenue... actually no, costs... wait, profit"), OR it mixes several conflicting asks. Anything where you cannot tell which single metric/dimension is wanted.
- file_upload: user wants to upload a file, compare their data, or mentions having an Excel/CSV file

Reply with ONLY the intent word, nothing else."""

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=8,
    )
    intent = _groq_text(response).lower()
    print(f"  Intent: {intent}")

    valid = ["chat", "data_query", "report_request", "clarification_needed", "file_upload", "show_options"]
    if intent not in valid:
        intent = "chat"
    return intent


def _query_tokens(q):
    """Lowercased content words for a query, stopwords removed — used for a
    cheap local overlap pre-filter before spending an LLM call on cache matching."""
    stop = {"the","a","an","of","for","by","in","on","to","and","with","me",
            "show","list","get","give","all","our","we","sell","their","this",
            "is","are","what","which","how","many","much","total","please"}
    words = re.findall(r"[a-z0-9]+", q.lower())
    out = set()
    for w in words:
        if w in stop or len(w) <= 2:
            continue
        # crude singular form so "orders"/"order", "customers"/"customer" align
        if w.endswith("ies") and len(w) > 4:
            w = w[:-3] + "y"
        elif w.endswith("es") and len(w) > 4:
            w = w[:-2]
        elif w.endswith("s") and not w.endswith("ss") and len(w) > 3:
            w = w[:-1]
        out.add(w)
    return out


def find_similar_cached_query(new_query, cached_queries):
    if not cached_queries:
        return None

    # ── Local pre-filter ──────────────────────────────────────────────────────
    # Only consider cached queries that share meaningful content words with the
    # new one. This avoids an LLM call when nothing is even plausibly related,
    # and shrinks the candidate list the LLM has to judge (fewer false hits).
    new_tokens = _query_tokens(new_query)
    candidates = []
    for q in cached_queries:
        overlap = new_tokens & _query_tokens(q)
        if overlap:
            candidates.append(q)
    if not candidates:
        print("  Cache match check: 'none' (no token overlap)")
        return None

    cached_list = "\n".join([f"- {q}" for q in candidates])
    prompt = f"""You are a STRICT semantic-equality checker for SQL query intents.

New query: "{new_query}"

Candidate cached queries:
{cached_list}

Two queries match ONLY IF they would produce the SAME SQL result set —
i.e. the SAME entity/grain, the SAME metrics, AND the SAME dimensions and filters.

Treat as DIFFERENT (reply none) if ANY of these differ:
- the entity or grain (e.g. "customers" vs "orders/invoices" are DIFFERENT)
- the metric (e.g. "total order value" vs "invoice value" are DIFFERENT)
- the columns requested (e.g. adding "customer names" or "product names" makes it DIFFERENT)
- sort direction or top/bottom (e.g. "top 10" vs "bottom 10" are DIFFERENT)
- any filter, date range, or grouping dimension

Only minor wording/synonym differences with identical intent count as a match
(e.g. "revenue by region" vs "total revenue per region").

If an EXACT-intent match exists, reply with ONLY that cached query verbatim.
Otherwise reply with ONLY the word: none"""

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=60
    )
    result = response.choices[0].message.content.strip()
    print(f"  Cache match check: '{result}'")

    if result.lower() == "none":
        return None
    for q in candidates:
        if q.lower() == result.lower():
            return q
    return None


def generate_chat_response(user_message, conversation_history):
    messages = [
        {
            "role":    "system",
            "content": (
                "You are AiRo, a professional business intelligence assistant. "
                "You help users analyze their ecommerce data. "
                "You can answer data questions by querying the database, "
                "generate Power BI reports on request, or compare uploaded Excel/CSV files with Fabric data. "
                "Be professional, concise, and helpful. "
                "Do not use emojis. "
                "If asked what you can do, explain that you can: "
                "1) Answer data questions directly in chat, "
                "2) Generate Power BI reports when asked, "
                "3) Upload an Excel or CSV file to compare with Fabric data, "
                "4) Discuss business metrics and analytics."
            )
        }
    ]
    for msg in conversation_history[-10:]:
        messages.append(msg)
    messages.append({"role": "user", "content": user_message})

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.7,
        max_tokens=500
    )
    return response.choices[0].message.content.strip()


def generate_clarification(user_message, conversation_history):
    messages = [
        {
            "role":    "system",
            "content": (
                "You are AiRo, a professional business intelligence assistant. "
                "The user has asked a vague data question. "
                "Ask one specific clarification question to understand what data they need. "
                "Be professional and concise. No emojis. "
                "Available dimensions: product, category, region, channel, date/time periods. "
                "Available metrics: revenue, net revenue, net profit, units sold, discount, returns."
            )
        }
    ]
    for msg in conversation_history[-6:]:
        messages.append(msg)
    messages.append({"role": "user", "content": user_message})

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.5,
        max_tokens=150
    )
    return response.choices[0].message.content.strip()


def _build_uploaded_tables_context(user_name):
    """
    Builds a context string describing all uploaded files for a user.
    Injected into SQL prompts so the LLM knows about uploaded tables
    even for data_query intent (not just file_upload intent).
    Returns empty string if no files are uploaded.
    """
    if not user_name:
        return ""
    ready = get_ready_uploads(user_name.lower())
    if not ready:
        return ""

    lines = [
        "════════════════════════════════════════════════════",
        "UPLOADED FILES (available for this session):",
        "The user has uploaded the following files as Delta tables.",
        "Use these tables when the user refers to 'my file', 'my data',",
        "'uploaded data', or mentions a specific filename.",
        ""
    ]
    for entry in ready:
        tname   = entry["table_name"]
        fname   = entry["filename"]
        cols    = entry["columns"]
        tschema = _TABLE_SCHEMA_MAP.get(tname, "dbo")
        full_ref = f"{tschema}.{tname}"
        lines.append(f"  FILE: {fname}")
        lines.append(f"  TABLE: {full_ref}")
        lines.append(f"  COLUMNS: {', '.join(cols)}")
        lines.append("")
    lines += [
        "COLUMN MAPPINGS (uploaded → Fabric):",
        "  net_sales      → fact_sales.net_revenue",
        "  gross_sales    → fact_sales.revenue",
        "  profit         → fact_sales.net_profit",
        "  quantity       → fact_sales.units_sold",
        "If user mentions a filename, use that exact table.",
        "If user says 'my data' and has one file, use that table.",
        "If user says 'my data' and has multiple files, pick the most relevant.",
        "════════════════════════════════════════════════════",
    ]
    return "\n".join(lines)


def _resolve_missing_geo(user_message):
    """Deterministic note when the user asks for a geographic grain that does
    not exist (state/city/etc.) — forces the model to use `region` and be
    honest about it, rather than silently substituting country."""
    q = user_message.lower()
    missing = [w for w in ["state", "city", "province", "county", "district", "zip", "postal"]
               if re.search(rf"\b{w}\b", q)]
    if not missing:
        return ""
    term = missing[0]
    return (f'- The user asked about "{term}", which does NOT exist as a column. '
            f'Use `region` instead (the closest available geographic level) and '
            f'alias/label it as region. Do NOT substitute country. '
            f'e.g. SELECT region, SUM(revenue) AS total_revenue FROM dbo.fact_sales '
            f'GROUP BY region ORDER BY total_revenue DESC')


def _resolve_relative_dates(user_message):
    """
    Detect relative-date phrases and compute concrete date literals from the
    real current date. Returns a text block of resolved bounds for the SQL
    prompt, or "" if none found. Deterministic — no LLM, no drift over time.
    """
    q     = user_message.lower()
    today = datetime.date.today()
    lines = []

    def iso(d):
        return d.isoformat()

    # last N days / last N months / last N weeks / last N years
    for num, unit in re.findall(r"last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)", q):
        n = int(num)
        if unit.startswith("day"):
            start = today - datetime.timedelta(days=n)
        elif unit.startswith("week"):
            start = today - datetime.timedelta(weeks=n)
        elif unit.startswith("month"):
            # approximate month math via 30-day step but prefer calendar months
            month = today.month - n
            year  = today.year
            while month <= 0:
                month += 12; year -= 1
            start = datetime.date(year, month, min(today.day, 28))
        else:  # years
            start = datetime.date(today.year - n, today.month, min(today.day, 28))
        lines.append(f'- "last {n} {unit}" → date >= \'{iso(start)}\' AND date <= \'{iso(today)}\'')

    # bare "last 7 days" style already covered; handle "last week"/"last month"/"last year"
    if re.search(r"\blast week\b", q):
        start = today - datetime.timedelta(days=today.weekday() + 7)
        end   = start + datetime.timedelta(days=6)
        lines.append(f'- "last week" → date >= \'{iso(start)}\' AND date <= \'{iso(end)}\'')
    if re.search(r"\blast month\b", q):
        m = today.month - 1; y = today.year
        if m == 0: m = 12; y -= 1
        first = datetime.date(y, m, 1)
        if m == 12: nxt = datetime.date(y+1, 1, 1)
        else:       nxt = datetime.date(y, m+1, 1)
        last = nxt - datetime.timedelta(days=1)
        lines.append(f'- "last month" → date >= \'{iso(first)}\' AND date <= \'{iso(last)}\'')
    if re.search(r"\blast year\b", q):
        lines.append(f'- "last year" → year = {today.year - 1}')

    # last full business week (most recent completed Mon–Fri)
    if "business week" in q or "full week" in q:
        # Monday of current week:
        this_monday = today - datetime.timedelta(days=today.weekday())
        last_monday = this_monday - datetime.timedelta(days=7)
        last_friday = last_monday + datetime.timedelta(days=4)
        lines.append(f'- "last full business week" → date >= \'{iso(last_monday)}\' '
                     f'AND date <= \'{iso(last_friday)}\' (Mon–Fri)')

    # "this month / this year" anchors
    if re.search(r"\bthis month\b", q):
        first = today.replace(day=1)
        lines.append(f'- "this month" → date >= \'{iso(first)}\' AND date <= \'{iso(today)}\'')
    if re.search(r"\bthis year\b", q):
        lines.append(f'- "this year" → year = {today.year}')

    if not lines:
        return ""
    return (f"(today is {iso(today)})\n" + "\n".join(lines)
            + "\nUse the literal dates above directly in the WHERE clause.")


def generate_sql(user_message, conversation_history, user_name=None):
    history_text = ""
    if conversation_history:
        recent = conversation_history[-6:]
        history_text = "\n".join([
            f"{m['role'].upper()}: {m['content']}"
            for m in recent
        ])

    # Fetch live schema (cached 6h)
    live_schema, _ = get_schema_context()

    # Inject uploaded file context if user has files loaded
    uploaded_context = _build_uploaded_tables_context(user_name)
    uploaded_section = ""
    if uploaded_context:
        uploaded_section = f"""
### UPLOADED USER FILES
{uploaded_context}
"""
        print(f"  [SQL] Injecting uploaded file context for {user_name}")

    # ── Deterministic relative-date resolution (no LLM drift) ────────────────
    date_facts   = _resolve_relative_dates(user_message)
    geo_note     = _resolve_missing_geo(user_message)
    date_section = ""
    if date_facts:
        date_section = ("\n### RESOLVED DATE BOUNDS (use these EXACT date literals,\n"
                        "### computed from the real current date — do NOT recompute)\n"
                        + date_facts + "\n")
    if geo_note:
        date_section += "\n### DIMENSION SUBSTITUTION (REQUIRED)\n" + geo_note + "\n"

    prompt = f"""You are an expert T-SQL analyst for the EcommereAnalytics gold layer
(Microsoft Fabric SQL Analytics endpoint).

### DATABASE SCHEMA
{live_schema}
{uploaded_section}
### RULES
{SQL_RULES}

### CONVERSATION HISTORY (for follow-up context)
{history_text}
{date_section}
### USER REQUEST
{user_message}

Think step by step, then return ONLY the T-SQL query:
1. Is the user asking to modify data? If yes, return the read-only refusal SELECT.
2. Does the user refer to uploaded data? If yes, use the UPLOADED USER FILES section.
3. Which table(s)? Prefer an agg_ table for pre-aggregated totals (RULE 1).
4. Do the columns exist in the schema? (RULE 2)
5. Need a window function or CTE? Use RULE 7 patterns.
6. Any semantic terms to interpret? Use RULE 9.
7. Write valid T-SQL. No markdown, no explanation."""

    def _ask(correction_notes=None):
        messages = [{"role": "user", "content": prompt}]
        if correction_notes:
            # Inject the bad SQL + specific corrections as a follow-up turn
            # so the model sees exactly what it did wrong and must fix it
            correction = (
                f"The SQL you just generated has the following errors:\n"
                + "\n".join(f"  - {issue}" for issue in correction_notes)
                + "\n\nPlease rewrite the SQL fixing ONLY these issues. "
                "Return only the corrected SQL with no explanation."
            )
            messages.append({"role": "assistant", "content": sql})
            messages.append({"role": "user",      "content": correction})
        response = _groq_chat(
            model=GROQ_MODEL,
            messages=messages,
            temperature=0,
            max_tokens=700
        )
        out = response.choices[0].message.content.strip()
        return out.replace("```sql", "").replace("```", "").strip()

    sql = _ask()

    # Security: block DML/DDL regardless of what the model produced.
    if _is_dangerous_sql(sql):
        print("  SECURITY: DML/DDL detected — blocked")
        return _READONLY_MSG

    # Quality: one regeneration attempt — pass exact issues so model knows what to fix.
    issues = _check_sql_quality(sql)
    if issues:
        print(f"  SQL quality issues: {issues} — regenerating with corrections...")
        sql = _ask(correction_notes=issues)
        if _is_dangerous_sql(sql):
            return _READONLY_MSG
        # Log whether the fix worked
        remaining = _check_sql_quality(sql)
        if remaining:
            print(f"  SQL still has issues after correction: {remaining}")
        else:
            print(f"  SQL corrected successfully.")

    return sql


def _compute_summary_stats(rows_data, y_column):
    """Compute reference stats (count, sum, avg, min, max) on the metric column
    so the summary can cite concrete numbers — e.g. the average spend that an
    'above average' query is measured against."""
    if not rows_data or not y_column:
        return {}
    vals = []
    for r in rows_data:
        v = r.get(y_column)
        try:
            if v is not None:
                vals.append(float(v))
        except (ValueError, TypeError):
            pass
    if not vals:
        return {"count": len(rows_data)}
    n = len(vals)
    total = sum(vals)
    return {
        "count": len(rows_data),
        "metric": y_column,
        "sum":   round(total, 2),
        "avg":   round(total / n, 2),
        "min":   round(min(vals), 2),
        "max":   round(max(vals), 2),
    }


def generate_data_summary(user_message, rows_data, x_column, y_column):
    if not rows_data:
        return "The query returned no results."

    data_preview = json.dumps(rows_data[:5], indent=2)
    stats        = _compute_summary_stats(rows_data, y_column)
    stats_line   = ""
    if stats:
        parts = [f"total rows: {stats.get('count')}"]
        if "avg" in stats:
            parts.append(f"{stats['metric']} — sum: {stats['sum']:,}, "
                         f"average: {stats['avg']:,}, min: {stats['min']:,}, max: {stats['max']:,}")
        stats_line = "Reference statistics across ALL rows: " + "; ".join(parts)

    prompt = f"""You are AiRo, a professional business intelligence assistant.

The user asked: {user_message}

Query results (first 5 rows of {len(rows_data)} total):
{data_preview}

{stats_line}

Write a brief professional summary of these results.
If the result is a SINGLE value (one row, one metric — e.g. a count or a
total), answer in ONE direct sentence and nothing more: "There are 55 vendors
in total." No preamble, no restating the question, no methodology.
Otherwise use 2-3 sentences.
Lead with the single most important number or finding (the headline).
When the question is comparative or relative (e.g. "more than average",
"growth", "vs last year"), STATE the reference value explicitly (e.g. the
average, or the % change) so the reader has context.
IMPORTANT — if the results are grouped (per category, per region, etc.),
do NOT blend the groups into one combined narrative or mix numbers across
groups. State how many rows/groups qualified and give at most ONE concrete
example from a single group (e.g. "12 products exceed their category average;
in Electronics, X is priced 1,200 vs the category average of 950").
Use exact numbers with thousands separators. Be concise. No emojis."""

    response = _groq_chat(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=160
    )
    return response.choices[0].message.content.strip()


def _json_safe(v):
    """Coerce DB-native types (datetime, date, Decimal, bytes) into JSON-
    serializable Python primitives. Applied at the fetch boundary so no
    downstream card/chart/JSON code ever sees a non-serializable object."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return str(v)


def execute_sql_query(sql):
    conn     = get_sql_connection()
    cursor   = conn.cursor()
    cursor.execute(sql)
    columns  = [desc[0] for desc in cursor.description]
    rows     = cursor.fetchall()
    conn.close()
    rows_data = [
        {col: _json_safe(val) for col, val in zip(columns, row)}
        for row in rows
    ]
    return rows_data, columns


def _all_values_null(rows_data):
    """
    True only when EVERY value across EVERY row is NULL — i.e. the query
    matched nothing real and produced a row (or rows) of pure NULLs, e.g.
    a misspelled region with an outer join yielding one all-NULL row.

    A result set that contains any real value — string OR numeric — is
    usable data. Label-only results (e.g. a list of product names with no
    metric column) are perfectly valid and must NOT be treated as no-data.
    """
    if not rows_data:
        return False

    for row in rows_data:
        for v in row.values():
            if v is not None:
                return False  # found a real value anywhere — data is usable

    # Every value in every row was NULL
    return True


# ── Chart column detection ────────────────────────────────────────────────────

_METRIC_KW    = ["revenue","profit","sales","total","amount","units","orders",
                 "discount","returns","value","price","cost","margin","rate",
                 "count","pct","percent","avg","average"]
_DIMENSION_KW = ["year","month","quarter","week","day","id","name","category",
                 "region","channel","date","code","type","flag","email",
                 "segment","vendor","customer","product"]
_TIME_KW      = ["year","month","quarter","week","day","date","trend",
                 "daily","monthly","yearly","annual"]


def _sample_non_null(col_name, rows_data):
    """Return the first non-null value for a column across rows (not just row 0).
    Many results have NULL in the first row (e.g. a window function's first
    period), which would otherwise mis-classify the column."""
    for r in rows_data:
        v = r.get(col_name)
        if v is not None and v != "":
            return v
    return None


def _col_is_metric(col_name, sample_val):
    cl = col_name.lower()
    if not any(k in cl for k in _METRIC_KW):
        return False
    try:
        float(sample_val); return True
    except (ValueError, TypeError):
        return False


# A column is a TIME DIMENSION only if a time word is its dominant token, not
# merely a substring. "prev_month_revenue" contains "month" but is a revenue
# metric, not a time axis — so we require the time word to be a standalone token
# AND the column not to carry a metric keyword.
def _col_is_time_dimension(col_name):
    cl = col_name.lower()
    tokens = re.split(r"[^a-z0-9]+", cl)           # split on _ and non-alnum
    has_time_token   = any(t in _TIME_KW for t in tokens)
    has_metric_token = any(k in cl for k in _METRIC_KW)
    # genuine time dimension: a time token present, and NOT actually a metric
    return has_time_token and not has_metric_token


def _col_is_time(col_name):
    return any(k in col_name.lower() for k in _TIME_KW)


# Granularity ranking — lower index = finer grain = preferred x-axis
# Used both in detect_chart_columns and _humanise_time_labels
_TIME_GRANULARITY = ["day", "date", "week", "month", "quarter", "year"]


def _col_granularity_rank(col_name):
    """Return granularity rank for a column name (lower = finer). 999 = not a time col."""
    cl = col_name.lower()
    for rank, kw in enumerate(_TIME_GRANULARITY):
        if kw in cl:
            return rank
    return 999


def detect_chart_columns(rows_data, columns):
    """
    Identify the best x (dimension) and y (metric) columns for charting.

    x-axis selection rules (in priority order):
      1. If the query has ONLY time columns + metric (no non-numeric labels),
         pick the most-granular time column — e.g. month beats year.
      2. If there is exactly ONE time column, use it regardless of strings present.
      3. Non-numeric string column (category names, regions, etc.) — use it.
      4. Most-granular numeric time dimension as last resort.

    This means "revenue by year" → year on x-axis (not overridden to month),
    and "monthly revenue 2024" (year + month cols) → month on x-axis.
    """
    if not rows_data or not columns:
        return (columns[0] if columns else None,
                columns[1] if len(columns) > 1 else None)

    first = rows_data[0]

    # Pass 1 — best y_column: numeric + metric keyword. Sample a NON-NULL value
    # so a NULL first row (common with window functions) doesn't hide a metric.
    y_column = None
    for col in columns:
        if _col_is_metric(col, _sample_non_null(col, rows_data)):
            y_column = col; break

    # Classify remaining columns
    non_metric_cols = [c for c in columns if c != y_column]
    time_cols   = []   # genuine time dimensions (year, month, quarter, etc.)
    string_cols = []   # non-numeric label columns (category, region, channel…)

    for col in non_metric_cols:
        val = _sample_non_null(col, rows_data)
        # A column counts as a time axis only if a time WORD is its dominant
        # token (not just a substring) and it isn't itself a metric like
        # "prev_month_revenue".
        if _col_is_time_dimension(col):
            time_cols.append(col)
            continue
        try:
            float(val)
            # numeric but not a time dimension and not a metric → treat as a
            # plain numeric label only if nothing better exists (rare)
        except (ValueError, TypeError):
            string_cols.append(col)

    # Pick x_column
    x_column = None

    if time_cols and not string_cols:
        # Pick most granular time column (month beats year, day beats month etc.)
        # Explicit month-over-year preference as belt-and-suspenders:
        # if both year and month cols exist, always use month as x-axis.
        month_cols = [c for c in time_cols if "month" in c.lower()]
        x_column   = month_cols[0] if month_cols else min(time_cols, key=_col_granularity_rank)

    elif len(time_cols) == 1 and not string_cols:
        # Exactly one time col, no string cols — use it
        x_column = time_cols[0]

    elif string_cols:
        # Prefer string label (e.g. "North", "Online") as x
        x_column = string_cols[0]
        # But if there's also a single time col, favour the time col
        # only when the string looks like a year (4-digit number string)
        if time_cols:
            sample = str(first.get(string_cols[0], "")).strip()
            looks_like_year = sample.isdigit() and len(sample) == 4
            if looks_like_year:
                x_column = min(time_cols, key=_col_granularity_rank)

    elif time_cols:
        x_column = min(time_cols, key=_col_granularity_rank)

    # Pass 3 — fallback y_column: first numeric that isn't x_column
    if not y_column:
        for col in columns:
            if col == x_column: continue
            try:
                float(first.get(col)); y_column = col; break
            except (ValueError, TypeError):
                pass

    if not x_column and columns:        x_column = columns[0]
    if not y_column and len(columns)>1: y_column = columns[1]
    return x_column, y_column


def _build_chart_title(rows_data, x_column, y_column, user_query=""):
    """
    Build a human chart/card title without the 'X by X' bug.
    - Single value / KPI (1 row, or x == y): just the metric label
      e.g. 'Total Orders' not 'Total Orders by Total Orders'
    - Normal: '{metric} by {dimension}'
    """
    def lbl(c):
        return c.replace("_", " ").title() if c else ""

    y_lbl = lbl(y_column)
    x_lbl = lbl(x_column)

    # KPI / single-value: no meaningful dimension to break down by
    if (not x_column) or (x_column == y_column) or (len(rows_data) == 1):
        return y_lbl or (user_query.strip().title() if user_query else "Result")

    # If x and y resolve to the same human label (e.g. both "Percentil 90"),
    # also collapse to a single label.
    if x_lbl and x_lbl == y_lbl:
        return y_lbl

    return f"{y_lbl} by {x_lbl}"


def select_chart_type(rows_data, x_column, y_column, columns):
    """
    Picks the best chart type for the data shape:
      kpi            — single row result
      line           — month/week/day/date/quarter x-axis (any row count)
                       OR year x-axis with >5 rows (enough for a trend)
      pie            — ≤ 6 categories, single metric
      horizontal_bar — ranking / categorical, or year with ≤5 rows
    """
    n = len(rows_data)
    if n == 0:
        return "horizontal_bar"
    if n == 1:
        return "kpi"

    if x_column and _col_is_time(x_column):
        cl = x_column.lower()
        # Sub-year granularity → always a line chart
        if any(k in cl for k in ["month", "week", "day", "date", "quarter"]):
            return "line"
        # Year granularity → line only when enough points to show a real trend
        if "year" in cl and n > 5:
            return "line"
        # Year with ≤5 rows → fall through to pie/bar (comparison, not trend)

    # Count distinct numeric metric columns (excluding x)
    first     = rows_data[0]
    n_metrics = sum(1 for c in columns
                    if c != x_column and _col_is_metric(c, first.get(c, "")))
    if n <= 6 and n_metrics == 1:
        return "pie"
    return "horizontal_bar"


# ── Chart theme constants ─────────────────────────────────────────────────────

_C_BG      = "#1e1e1e"
_C_AXES    = "#252525"
_C_GRID    = "#333333"
_C_ACCENT  = "#118DFF"
_C_TEXT_W  = "#e0e0e0"
_C_TEXT_S  = "#888888"
_PALETTE   = ["#118DFF","#00C4B4","#F4C430","#E05C5C","#9B59B6",
               "#27AE60","#E67E22","#1ABC9C","#E91E8C","#00B0FF"]


def _chart_fig(w=10, h=5):
    """Create a pre-styled dark-theme figure + axes."""
    fig, ax = plt.subplots(figsize=(w, h))
    fig.patch.set_facecolor(_C_BG)
    ax.set_facecolor(_C_AXES)
    for sp in ax.spines.values():
        sp.set_color(_C_GRID)
    ax.tick_params(colors=_C_TEXT_W, labelsize=8.5)
    ax.xaxis.label.set_color(_C_TEXT_S)
    ax.yaxis.label.set_color(_C_TEXT_S)
    return fig, ax


def _chart_title(ax, title):
    ax.set_title(title, color=_C_TEXT_W, fontsize=11, pad=12, fontweight="bold")


def _chart_fmt(v, col_name=""):
    """Format a numeric value for chart labels. Shows the exact number with
    thousands separators (no K/M abbreviation) so counts and totals read
    precisely — e.g. 1265 → '1,265', not '1.3K'."""
    SKIP = ["year","month","quarter","week","day","_id","id_","code"]
    try:
        f  = float(v)
        cl = col_name.lower()
        if any(k in cl for k in SKIP):
            return str(int(f)) if f == int(f) else str(f)
        if f == int(f):
            return f"{int(f):,}"          # whole number → 1,265
        return f"{f:,.2f}"                  # decimal → 1,234.56
    except Exception:
        return str(v)


def _savefig_b64(fig):
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130,
                bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# ── Individual chart renderers ────────────────────────────────────────────────

def _render_horizontal_bar(labels, values, title, y_col):
    import numpy as np
    n       = len(labels)
    height  = max(4.0, n * 0.48 + 1.2)
    fig, ax = _chart_fig(10, height)
    colors  = [_PALETTE[i % len(_PALETTE)] for i in range(n)]
    y_pos   = np.arange(n)
    ax.barh(y_pos, values, color=colors, edgecolor="none", height=0.62)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, color=_C_TEXT_W, fontsize=8.5)
    ax.invert_yaxis()
    max_v = max(values) if values else 1
    ax.set_xlim(0, max_v * 1.20)
    ax.xaxis.set_visible(False)
    ax.yaxis.grid(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_color(_C_GRID)
    for i, (yp, val) in enumerate(zip(y_pos, values)):
        ax.text(val + max_v * 0.013, yp,
                _chart_fmt(val, y_col),
                va="center", ha="left", color=_C_TEXT_W, fontsize=8.0, fontweight="bold")
    _chart_title(ax, title)
    plt.tight_layout(pad=1.6)
    return _savefig_b64(fig)


_MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]


def _parse_date_str(s):
    """
    Try to parse common date string formats.
    Returns (year, month, day) tuple with None for unknown parts, or None if unparseable.
    Handles: YYYY-MM-DD, YYYY-MM, YYYYMM, YYYY/MM/DD, YYYY/MM, DD-Mon-YYYY etc.
    """
    import re
    s = str(s).strip()
    # YYYY-MM-DD or YYYY/MM/DD
    m = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', s)
    if m: return int(m.group(1)), int(m.group(2)), int(m.group(3))
    # YYYY-MM or YYYY/MM
    m = re.match(r'^(\d{4})[-/](\d{1,2})$', s)
    if m: return int(m.group(1)), int(m.group(2)), None
    # YYYYMM (6-digit)
    m = re.match(r'^(\d{4})(\d{2})$', s)
    if m:
        yr, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12: return yr, mo, None
    # Pure 4-digit year
    m = re.match(r'^(\d{4})$', s)
    if m: return int(m.group(1)), None, None
    return None


def _humanise_time_labels(raw_labels, x_col_name, all_rows=None, year_col=None):
    """
    Convert raw time dimension values into clean, human-readable axis labels.

    Handles:
      - Month numbers (1-12)          → Jan … Dec
        Multi-year: adds year suffix  → Jan 2023, Jan 2024
      - Quarter numbers (1-4)         → Q1 … Q4
        Multi-year: adds year suffix  → Q1 2023, Q1 2024
      - Week numbers (1-53)           → Wk 1 … Wk 53
      - Date strings YYYY-MM-DD       → Jan 2024 (or Jan 15 if day matters)
      - Date strings YYYY-MM / YYYYMM → Jan 2024
      - Plain year numbers            → left as-is (2023, 2024)
      - Anything else                 → left as-is
    """
    cl = (x_col_name or "").lower()

    # ── Date string columns (contains "date" or actual date-like values) ──────
    if "date" in cl or (raw_labels and _parse_date_str(raw_labels[0]) is not None
                        and not str(raw_labels[0]).strip().lstrip("-").isdigit()):
        result = []
        for lbl in raw_labels:
            parsed = _parse_date_str(lbl)
            if parsed:
                yr, mo, day = parsed
                if mo is not None and 1 <= mo <= 12:
                    mon = _MONTH_NAMES[mo - 1]
                    result.append(f"{mon} {yr}" if yr else mon)
                elif yr:
                    result.append(str(yr))
                else:
                    result.append(str(lbl))
            else:
                result.append(str(lbl))
        return result

    # ── Handle YYYYMM 6-digit combined values (e.g. 202401 → "Jan 2024") ────────
    # These are all-digit so won't be caught by the date-string branch above.
    if not ("date" in cl) and raw_labels:
        sample = str(raw_labels[0]).strip()
        if len(sample) == 6 and sample.isdigit():
            parsed = _parse_date_str(sample)
            if parsed and parsed[1] is not None:
                result = []
                for lbl in raw_labels:
                    p = _parse_date_str(str(lbl).strip())
                    if p and p[1] and 1 <= p[1] <= 12:
                        result.append(f"{_MONTH_NAMES[p[1]-1]} {p[0]}")
                    else:
                        result.append(str(lbl))
                return result

    # ── Numeric time columns ─────────────────────────────────────────────────
    # Build a per-POSITION year map (not per-value) so repeated month numbers
    # across years get the right suffix: Jan 2023 ≠ Jan 2024.
    multi_year  = False
    year_by_pos = {}   # row index → year int
    if all_rows and year_col:
        years_seen = set()
        for row in all_rows:
            try: years_seen.add(int(float(row.get(year_col))))
            except: pass
        multi_year = len(years_seen) > 1
        if multi_year:
            for i, row in enumerate(all_rows):
                if i < len(raw_labels):
                    try: year_by_pos[i] = int(float(row.get(year_col)))
                    except: pass

    result = []
    for idx, lbl in enumerate(raw_labels):
        s = str(lbl).strip()
        try:
            n = int(float(s))
        except (ValueError, TypeError):
            result.append(s); continue

        yr_suffix = f" {year_by_pos[idx]}" if (multi_year and idx in year_by_pos) else ""

        if "month" in cl and 1 <= n <= 12:
            result.append(f"{_MONTH_NAMES[n-1]}{yr_suffix}")
        elif "quarter" in cl and 1 <= n <= 4:
            result.append(f"Q{n}{yr_suffix}")
        elif "week" in cl and 1 <= n <= 53:
            result.append(f"Wk {n}{yr_suffix}")
        elif "year" in cl:
            result.append(str(n))          # year col → plain number
        elif 1 <= n <= 12 and len(raw_labels) <= 12:
            # Looks like month numbers even without "month" in col name
            result.append(f"{_MONTH_NAMES[n-1]}{yr_suffix}")
        else:
            result.append(s)
    return result


def _render_line(labels, values, title, y_col, x_col="", all_rows=None, year_col=None):
    import numpy as np
    # Convert time labels to human-readable form (month numbers → Jan, multi-year aware)
    labels = _humanise_time_labels(labels, x_col, all_rows=all_rows, year_col=year_col)
    fig, ax = _chart_fig(10, 4.5)
    x = np.arange(len(labels))
    ax.plot(x, values, color=_C_ACCENT, linewidth=2.4,
            marker="o", markersize=5.5,
            markerfacecolor=_C_ACCENT, markeredgewidth=0)
    ax.fill_between(x, values, alpha=0.10, color=_C_ACCENT)
    ax.set_xticks(x)
    rot = 35 if len(labels) > 7 else 0
    ax.set_xticklabels(labels, rotation=rot, ha="right" if rot else "center",
                       color=_C_TEXT_W, fontsize=8.5)
    ax.set_yticks([])
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_color(_C_GRID)
    ax.yaxis.grid(True, color=_C_GRID, linewidth=0.5, linestyle="--")
    # data labels above each point
    mn, mx = min(values), max(values)
    span   = mx - mn if mx != mn else 1
    for xi, yi in zip(x, values):
        offset = span * 0.06
        ax.text(xi, yi + offset, _chart_fmt(yi, y_col),
                ha="center", va="bottom", color=_C_TEXT_W,
                fontsize=7.5, fontweight="bold")
    _chart_title(ax, title)
    plt.tight_layout(pad=1.6)
    return _savefig_b64(fig)


def _render_pie(labels, values, title):
    import matplotlib.patches as mpatches
    import numpy as np
    fig, (ax_pie, ax_leg) = plt.subplots(
        1, 2, figsize=(10, 4.5),
        gridspec_kw={"width_ratios": [1.3, 1]}
    )
    fig.patch.set_facecolor(_C_BG)
    for ax in (ax_pie, ax_leg):
        ax.set_facecolor(_C_BG)

    colors  = _PALETTE[:len(labels)]
    total   = sum(values) or 1
    wedges, _ = ax_pie.pie(
        values, colors=colors, startangle=140,
        wedgeprops={"edgecolor": _C_BG, "linewidth": 2.5},
        pctdistance=0.78
    )
    # percentage labels inside large-enough slices
    for wedge, val in zip(wedges, values):
        angle  = (wedge.theta2 + wedge.theta1) / 2
        px     = 0.60 * np.cos(np.radians(angle))
        py     = 0.60 * np.sin(np.radians(angle))
        pct    = val / total * 100
        if pct > 6:
            ax_pie.text(px, py, f"{pct:.1f}%",
                        ha="center", va="center",
                        color="white", fontsize=8.5, fontweight="bold")
    ax_pie.set_title(title, color=_C_TEXT_W, fontsize=11,
                     pad=12, fontweight="bold")

    # Legend panel with value + pct
    ax_leg.axis("off")
    max_items = min(len(labels), 8)
    row_h     = 0.92 / max_items
    for i, (lbl, val, col) in enumerate(zip(labels[:max_items],
                                             values[:max_items],
                                             colors[:max_items])):
        y_lp = 0.96 - i * row_h
        rect = mpatches.FancyBboxPatch(
            (0.0, y_lp - row_h * 0.38), 0.10, row_h * 0.70,
            boxstyle="round,pad=0.01",
            facecolor=col, edgecolor="none",
            transform=ax_leg.transAxes, clip_on=False
        )
        ax_leg.add_patch(rect)
        ax_leg.text(0.15, y_lp - row_h * 0.03, lbl[:22],
                    transform=ax_leg.transAxes,
                    color=_C_TEXT_W, fontsize=8.5, va="center")
        ax_leg.text(0.99, y_lp - row_h * 0.03,
                    f"{_chart_fmt(val)}  ({val/total*100:.1f}%)",
                    transform=ax_leg.transAxes,
                    color=_C_TEXT_S, fontsize=8.0, va="center", ha="right")
    plt.tight_layout(pad=1.2)
    return _savefig_b64(fig)


def _pick_headline_metric(row, default_col):
    """
    For a single-row 'details' result with many columns, pick the most
    meaningful number to headline. A detail row like an order has price (unit),
    quantity, AND a total/revenue — the headline should be the TOTAL, not the
    unit price. Preference order by column-name keyword; falls back to the
    detector's choice.
    """
    if not row:
        return default_col
    # priority keywords (highest first)
    priority = ["total_revenue", "net_revenue", "revenue", "total_price",
                "total_amount", "amount", "net_profit", "total", "grand_total",
                "lifetime_value", "value"]
    cols_lower = {c: c.lower() for c in row.keys()}
    for kw in priority:
        for c, cl in cols_lower.items():
            if kw in cl:
                v = row.get(c)
                try:
                    float(v)
                    return c
                except (ValueError, TypeError):
                    continue
    return default_col


def _render_kpi(label, value, title):
    fig, ax = plt.subplots(figsize=(6, 2.6))
    fig.patch.set_facecolor(_C_BG)
    ax.set_facecolor(_C_BG)
    ax.axis("off")
    # Large metric value
    ax.text(0.5, 0.62, _chart_fmt(value),
            transform=ax.transAxes, ha="center", va="center",
            fontsize=42, color=_C_ACCENT, fontweight="bold")
    # Label below
    ax.text(0.5, 0.20, label,
            transform=ax.transAxes, ha="center", va="center",
            fontsize=11, color=_C_TEXT_S)
    # Title above
    ax.text(0.5, 0.94, title,
            transform=ax.transAxes, ha="center", va="top",
            fontsize=10, color=_C_TEXT_W, fontweight="bold")
    plt.tight_layout(pad=0.4)
    return _savefig_b64(fig)


# ── Main chart entry point ─────────────────────────────────────────────────────

def generate_chart_base64(rows_data, x_column, y_column, title):
    """
    Dispatches to the right chart renderer based on data shape.
    Chart type is chosen automatically:
      - 1 row             → KPI big-number card
      - time x-axis       → line chart with area fill
      - ≤6 cats, 1 metric → pie / donut with legend
      - everything else   → coloured horizontal bar chart
    """
    try:
        chart_rows  = rows_data[:20]
        all_columns = list(rows_data[0].keys()) if rows_data else []
        chart_type  = select_chart_type(chart_rows, x_column, y_column, all_columns)
        print(f"  [Chart] type={chart_type} rows={len(chart_rows)} x={x_column} y={y_column}")

        if chart_type == "kpi":
            kpi_col = _pick_headline_metric(chart_rows[0], y_column)
            val     = chart_rows[0].get(kpi_col, 0)
            label   = (kpi_col or "").replace("_", " ").title()
            return _render_kpi(label, val, title)

        # Two-row comparison (e.g. "Q1 2024 vs Q1 2023") — the most useful
        # headline is the change between them. Render growth % as the KPI.
        if (len(chart_rows) == 2 and y_column):
            try:
                v_old = float(chart_rows[0].get(y_column) or 0)
                v_new = float(chart_rows[1].get(y_column) or 0)
                if v_old:
                    pct = (v_new - v_old) / abs(v_old) * 100
                    arrow = "▲" if pct >= 0 else "▼"
                    lbl   = f"{(x_column or 'period').replace('_',' ').title()} change"
                    return _render_kpi(lbl, f"{arrow} {pct:+.1f}%", title)
            except (ValueError, TypeError):
                pass

        labels = [str(r.get(x_column, ""))[:28] for r in chart_rows]
        try:
            values = [float(r.get(y_column) or 0) for r in chart_rows]
        except (ValueError, TypeError):
            return None

        # Find a sibling year column — used by _humanise_time_labels for multi-year labels
        year_col = None
        if x_column and "month" in x_column.lower() or x_column and "quarter" in x_column.lower()                 or x_column and "week" in x_column.lower():
            for c in all_columns:
                if c != x_column and c != y_column and "year" in c.lower():
                    year_col = c
                    break

        if chart_type == "pie":
            return _render_pie(labels, values, title)
        if chart_type == "line":
            return _render_line(labels, values, title, y_column or "", x_column or "",
                                all_rows=chart_rows, year_col=year_col)
        return _render_horizontal_bar(labels, values, title, y_column or "")

    except Exception as e:
        print(f"  [Chart] generation error: {e}")
        return None


def format_value(val, col_name=""):
    """Format a value for display. Shows the exact number with thousands
    separators (no K/M abbreviation). Skips formatting for year/month/ID-like
    columns."""
    SKIP_FORMAT_KEYWORDS = ["year", "month", "quarter", "week", "day", "_id", "id_", "code"]
    try:
        f = float(val)
        col_lower = col_name.lower()
        if any(kw in col_lower for kw in SKIP_FORMAT_KEYWORDS):
            return str(int(f)) if f == int(f) else str(f)
        if f == int(f):
            return f"{int(f):,}"          # whole number → 1,265
        return f"{f:,.2f}"                  # decimal → 1,234.56
    except:
        return str(val)


# ── Adaptive Card builders ────────────────────────────────────────────────────

STEP_SEQUENCE = [
    "waiting", "running_1", "running_2", "running_3",
    "running_4", "running_5", "running_6",
]

STEP_DESCRIPTIONS = [
    ("running_1", "Schema extracted from Lakehouse"),
    ("running_2", "SQL query generated by AI"),
    ("running_3", "Report template selected"),
    ("running_4", "SQL executed against Lakehouse"),
    ("running_5", "DirectLake model refreshed"),
    ("running_6", "Report published to workspace"),
]


def build_progress_bar(current_step):
    total    = len(STEP_SEQUENCE) - 1
    idx      = STEP_SEQUENCE.index(current_step) if current_step in STEP_SEQUENCE else 0
    filled   = max(0, idx)
    unfilled = total - filled
    bar      = "█" * filled + "░" * unfilled
    pct      = int((filled / total) * 100)
    return f"{bar}  {pct}%"


def build_report_card(
    user_query, current_step,
    is_done=False, is_failed=False,
    report_url=None, sql=None, template=None,
    rows=None, has_inline_data=False
):
    if is_done:
        header_color, header_text = "Good", "Report Ready"
    elif is_failed:
        header_color, header_text = "Attention", "Error"
    else:
        header_color, header_text = "Accent", "Generating Report..."

    progress_bar   = build_progress_bar(current_step) if not is_done and not is_failed else "██████  100%"
    progress_color = "Good" if is_done else "Accent"

    friendly_template = None
    if template:
        friendly_template = {
            "T1_revenue_product_time": "Revenue by Product over Time",
            "T2_revenue_by_product":   "Revenue by Product",
            "T3_revenue_trend":        "Revenue Trend",
            "T4_revenue_by_region":    "Revenue by Region",
            "T5_units_by_channel":     "Units by Channel",
            "LLM_FALLBACK":            "Custom AI-generated layout",
        }.get(template, template)

    current_idx = STEP_SEQUENCE.index(current_step) if current_step in STEP_SEQUENCE else 0

    checklist_facts = []
    for step_key, step_desc in STEP_DESCRIPTIONS:
        step_idx = STEP_SEQUENCE.index(step_key)
        if is_done or step_idx < current_idx:
            checklist_facts.append({"title": "✓", "value": step_desc})
        elif step_idx == current_idx:
            checklist_facts.append({"title": "...", "value": step_desc})
        else:
            checklist_facts.append({"title": "○", "value": step_desc})

    extra_facts = []
    if sql:
        extra_facts.append({"title": "SQL", "value": sql})
    if friendly_template:
        extra_facts.append({"title": "Template", "value": friendly_template})
    if rows is not None:
        extra_facts.append({"title": "Rows", "value": str(rows)})

    details_items = [
        {"type": "TextBlock", "text": "Processing Steps", "weight": "Bolder", "size": "Small", "color": "Accent"},
        {"type": "FactSet", "facts": checklist_facts}
    ]
    if extra_facts:
        details_items.append({"type": "TextBlock", "text": "Report Details", "weight": "Bolder", "size": "Small", "color": "Accent", "spacing": "Medium"})
        details_items.append({"type": "FactSet", "facts": extra_facts})

    body = [
        {"type": "TextBlock", "text": "AiRo Bot", "size": "Small", "color": "Accent", "weight": "Bolder"},
        {"type": "TextBlock", "text": header_text, "size": "Large", "weight": "Bolder", "color": header_color, "spacing": "None"},
        {
            "type": "ColumnSet", "spacing": "Medium",
            "columns": [
                {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "Question:", "weight": "Bolder", "size": "Small", "color": "Accent"}]},
                {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": user_query, "wrap": True, "size": "Small"}]}
            ]
        },
        {"type": "TextBlock", "text": progress_bar, "size": "Small", "color": progress_color, "spacing": "Medium"},
        {"type": "ActionSet", "spacing": "Medium", "actions": [{"type": "Action.ToggleVisibility", "title": "Show Details", "targetElements": ["details_container"]}]},
        {"type": "Container", "id": "details_container", "isVisible": False, "spacing": "Small", "style": "emphasis", "items": details_items}
    ]

    actions = []
    if is_done and report_url:
        actions.append({"type": "Action.OpenUrl", "title": "Open Report", "url": report_url, "style": "positive"})
        if has_inline_data:
            actions.append({"type": "Action.Execute", "title": "View in Teams", "verb": "view_inline", "data": {"action": "view_inline"}})

    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5",
            "body": body,
            "actions": actions
        }
    }


def build_cache_confirmation_card(new_query, matched_query, intent):
    body = [
        {"type": "TextBlock", "text": "AiRo Bot", "size": "Small", "color": "Accent", "weight": "Bolder"},
        {"type": "TextBlock", "text": "Similar Query Found", "size": "Large", "weight": "Bolder", "color": "Accent", "spacing": "None"},
        {"type": "TextBlock", "text": f"Your question: **{new_query}**", "wrap": True, "size": "Small", "spacing": "Medium"},
        {"type": "TextBlock", "text": "I already have results for a similar query:", "wrap": True, "size": "Small", "spacing": "Small"},
        {"type": "TextBlock", "text": f"_{matched_query}_", "wrap": True, "size": "Small", "color": "Accent", "spacing": "None"},
        {"type": "TextBlock", "text": "Would you like to use the existing results or run a fresh query?", "wrap": True, "size": "Small", "spacing": "Medium"}
    ]
    actions = [
        {"type": "Action.Execute", "title": "Use Existing Results", "verb": "use_cache", "data": {"action": "use_cache", "matched_query": matched_query, "new_query": new_query, "intent": intent}},
        {"type": "Action.Execute", "title": "Run Fresh Query", "verb": "run_fresh", "data": {"action": "run_fresh", "new_query": new_query, "intent": intent}}
    ]
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.5", "body": body, "actions": actions}
    }


def _build_data_table(rows_data, y_column, table_limit=15):
    """
    Shared helper — builds the Results header + an Adaptive Card 1.5 Table element.
    AC Table is the ONLY reliable way to get proper column alignment in Teams —
    ColumnSet with horizontalAlignment is ignored by the Teams renderer.
    Returns a list of Adaptive Card body elements.
    """
    elements     = []
    all_cols     = list(rows_data[0].keys()) if rows_data else []
    if not all_cols:
        return elements

    # Narrow results (1–2 columns, e.g. a list of product/customer names) stay
    # visually compact per row, so we can show many more before the card feels
    # heavy. Wider tables get a moderate bump too so common lists (e.g. all 55
    # vendors with id/name/revenue) aren't cut to 15.
    if len(all_cols) <= 2:
        table_limit = max(table_limit, 50)
    elif len(all_cols) <= 4:
        table_limit = max(table_limit, 30)

    display_rows = rows_data[:table_limit]

    total_rows = len(rows_data)
    if total_rows > table_limit:
        row_note = (f"Showing {len(display_rows)} of {total_rows} — "
                    f"open the interactive chart or report for all {total_rows}")
    else:
        row_note = f"{total_rows} row{'s' if total_rows != 1 else ''}"

    # Results / row count header
    elements.append({
        "type": "ColumnSet", "spacing": "Medium",
        "columns": [
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": "Results",
                        "weight": "Bolder", "size": "Small", "color": "Accent"}]},
            {"type": "Column", "width": "auto",
             "items": [{"type": "TextBlock", "text": row_note,
                        "size": "Small", "isSubtle": True}]}
        ]
    })

    # Determine which columns are numeric (right-aligned)
    def _is_numeric_col(col, sample_rows):
        for r in sample_rows[:5]:
            v = r.get(col, "")
            if v in (None, "", "None"): continue
            try: float(v); return True
            except: return False
        return False

    numeric_cols = {c for c in all_cols if _is_numeric_col(c, display_rows)}

    # ── Adaptive Card 1.5 Table ──────────────────────────────────────────────
    # Table element is the only way to get reliable column alignment in Teams.
    # Each column definition controls width + alignment for the whole column.
    n_cols = len(all_cols)
    col_defs = []
    for i, c in enumerate(all_cols):
        is_num = c in numeric_cols
        # Tester recommendation: table contents read better center-aligned in
        # the Teams card (numbers and text alike).
        col_defs.append({
            "width":               1 if i == 0 else 1,
            "horizontalCellContentAlignment": "center"
        })

    # Header row
    header_cells = []
    for i, c in enumerate(all_cols):
        is_num = c in numeric_cols
        header_cells.append({
            "type": "TableCell",
            "items": [{
                "type":   "TextBlock",
                "text":   c.replace("_", " ").title(),
                "weight": "Bolder",
                "size":   "Small",
                "color":  "Accent",
                "wrap":   False
            }],
            "style": "emphasis"
        })

    # Data rows
    table_rows = [{"type": "TableRow", "cells": header_cells, "style": "emphasis"}]
    for row in display_rows:
        cells = []
        for i, col in enumerate(all_cols):
            raw_val     = row.get(col, "")
            display_val = format_value(raw_val, col)
            is_metric   = (col == y_column)
            cells.append({
                "type": "TableCell",
                "items": [{
                    "type":   "TextBlock",
                    "text":   str(display_val),
                    "size":   "Small",
                    "wrap":   False,
                    "color":  "Accent" if is_metric else "Default",
                    "weight": "Bolder" if is_metric else "Default"
                }]
            })
        table_rows.append({"type": "TableRow", "cells": cells})

    elements.append({
        "type":            "Table",
        "columns":         col_defs,
        "rows":            table_rows,
        "showGridLines":   False,
        "firstRowAsHeader": True,
        "spacing":         "Small"
    })

    return elements


def build_data_query_card(user_query, summary, rows_data, x_column, y_column, chart_base64, sql, chart_id=None):
    """
    Main inline data-query result card.

    Layout
    ──────
    [AiRo Bot label]
    [Question — bold]
    [AI summary — subtle]
    [Chart image — full width, type chosen by select_chart_type()]
    [Results header + row count]
    [─────────────────────────]
    [Column headers (Accent, bold)]
    [Data rows — metric col highlighted]
    [Show SQL ▾]  (collapsible)
    ──────────────────────────────────
    [Generate Power BI Report]  (primary action)
    """
    body = [
        # ── Header ──────────────────────────────────────────────────────────
        {
            "type": "ColumnSet", "spacing": "None",
            "columns": [
                {"type": "Column", "width": "stretch",
                 "items": [
                     {"type": "TextBlock", "text": "AiRo Bot",
                      "size": "Small", "color": "Accent", "weight": "Bolder"},
                     {"type": "TextBlock", "text": user_query,
                      "wrap": True, "size": "Medium", "weight": "Bolder",
                      "color": "Default", "spacing": "None"},
                 ]},
            ]
        },
        # ── AI summary ───────────────────────────────────────────────────────
        {"type": "TextBlock", "text": summary, "wrap": True,
         "size": "Small", "spacing": "Small", "isSubtle": True},
    ]

    # ── Chart ────────────────────────────────────────────────────────────────
    if chart_base64:
        body.append({
            "type": "Image",
            "url": f"data:image/png;base64,{chart_base64}",
            "size": "Stretch", "spacing": "Medium"
        })

    # ── Data table ────────────────────────────────────────────────────────────
    if rows_data:
        body.extend(_build_data_table(rows_data, y_column))

    # ── SQL toggle ────────────────────────────────────────────────────────────
    body.append({
        "type": "ActionSet", "spacing": "Small",
        "actions": [{
            "type": "Action.ToggleVisibility",
            "title": "Show SQL ▾",
            "targetElements": ["sql_block"]
        }]
    })
    body.append({
        "type": "Container", "id": "sql_block",
        "isVisible": False, "spacing": "None",
        "style": "emphasis",
        "items": [{
            "type": "TextBlock", "text": sql,
            "wrap": True, "size": "Small", "fontType": "Monospace"
        }]
    })

    actions = [{
        "type":  "Action.Execute",
        "title": "Generate Power BI Report",
        "verb":  "generate_report",
        "data":  {"action": "generate_report"},
        "style": "positive"
    }]
    if chart_id:
        chart_url = f"{NGROK_DOMAIN}/chart/{chart_id}"
        actions.append({
            "type":  "Action.OpenUrl",
            "title": "View Interactive Chart 📊",
            "url":   chart_url
        })
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type":    "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5",
            "body":    body,
            "actions": actions
        }
    }


def build_inline_report_card(user_query, rows_data, x_column, y_column, chart_base64):
    """
    'View in Teams' inline report card — shown when user clicks the button
    on a completed report card.  Same layout as build_data_query_card but
    without the SQL toggle and Generate Report button.
    """
    body = [
        {"type": "TextBlock", "text": "AiRo Bot — Inline Report",
         "size": "Small", "color": "Accent", "weight": "Bolder"},
        {"type": "TextBlock", "text": user_query, "wrap": True,
         "size": "Medium", "weight": "Bolder", "spacing": "None"},
    ]

    if chart_base64:
        body.append({
            "type": "Image",
            "url": f"data:image/png;base64,{chart_base64}",
            "size": "Stretch", "spacing": "Medium"
        })

    if rows_data:
        body.extend(_build_data_table(rows_data, y_column))

    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type":    "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5",
            "body":    body
        }
    }


def make_activity_with_card(card_attachment):
    activity             = Activity(type="message")
    activity.attachments = [card_attachment]
    return activity




# ── Quick-options card system ──────────────────────────────────────────────────
# Reusable clickable option menus that also accept typed letters (A/B/C/D).
# Non-intrusive: the bot remains a free-text assistant; these are shortcuts only.

def build_options_card(title, description, options, footer_note=None):
    """
    Build an Adaptive Card with clickable option buttons.
    `options` is a list of (label, prompt_text) tuples.
    Each button click sends `prompt_text` into chat as if the user typed it.
    Also stores a letter map (A, B, C...) in the returned card data for
    the typed-letter fallback to use.
    """
    letters = "ABCDEFGH"
    body = [
        {"type": "TextBlock", "text": "AiRo Bot", "size": "Small", "color": "Accent", "weight": "Bolder"},
        {"type": "TextBlock", "text": title, "size": "Large", "weight": "Bolder", "color": "Accent", "spacing": "None"},
    ]
    if description:
        body.append({"type": "TextBlock", "text": description, "wrap": True, "size": "Small", "spacing": "Small"})

    option_lines = []
    for i, (label, _prompt) in enumerate(options[:8]):
        letter = letters[i]
        option_lines.append(f"**{letter}.** {label}")
    body.append({
        "type": "TextBlock",
        "text": "\n\n".join(option_lines),
        "wrap": True,
        "size": "Small",
        "spacing": "Medium"
    })

    if footer_note:
        body.append({"type": "TextBlock", "text": footer_note, "wrap": True, "size": "Small", "color": "Default", "isSubtle": True, "spacing": "Medium"})
    else:
        body.append({"type": "TextBlock", "text": "Tap an option below, or type the letter (A, B, C...).", "wrap": True, "size": "Small", "isSubtle": True, "spacing": "Medium"})

    actions = []
    for i, (label, prompt) in enumerate(options[:8]):
        letter = letters[i]
        actions.append({
            "type": "Action.Execute",
            "title": f"{letter}. {label}",
            "verb": "quick_option",
            "data": {"action": "quick_option", "prompt": prompt}
        })

    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5",
            "body": body,
            "actions": actions
        }
    }


def store_pending_options(user_name, options):
    """
    Store the active option set for a user so typed letters (A/B/C) can be
    resolved on their next message. options = list of (label, prompt_text).
    """
    session = get_user_session(user_name)
    session["pending_options"] = {
        letter: prompt for letter, (label, prompt) in
        zip("ABCDEFGH", options[:8])
    }


def resolve_typed_option(user_name, user_message):
    """
    If the user's message is just a single letter (A-H, case-insensitive,
    optionally with trailing punctuation) AND there's a pending option set
    for them, return the matching prompt text. Otherwise return None.
    """
    session  = get_user_session(user_name)
    pending  = session.get("pending_options")
    if not pending:
        return None

    cleaned = user_message.strip().rstrip(".").upper()
    if len(cleaned) == 1 and cleaned in pending:
        prompt = pending[cleaned]
        session["pending_options"] = None  # consume — one-shot
        return prompt
    return None


def build_welcome_options_card():
    """The default greeting menu shown on 'hi', 'hello', etc."""
    options = [
        ("Build a Power BI report",         "__action:report_request"),
        ("Ask a question about my data",    "__action:ask_clarify"),
        ("Upload a file to compare",        "__action:file_upload"),
        ("Something else",                  "__action:chat_help"),
    ]
    card = build_options_card(
        title="Hi, I'm AiRo",
        description="Your business intelligence assistant for ecommerce data. What would you like help with?",
        options=options
    )
    return card, options


def build_no_data_options_card(user_query):
    """
    Shown when a query cannot be answered from the existing Fabric schema —
    either because SQL returned 0 rows, the LLM signalled it can't form
    a valid query, or SQL execution genuinely failed.
    """
    options = [
        ("Upload a new dataset",        "__action:file_upload"),
        ("Ask a different question",    "__action:ask_different"),
        ("Rephrase this question",      "__action:rephrase"),
    ]
    card = build_options_card(
        title="Information Not Found",
        description=(
            f"I couldn't find information for **\"{user_query}\"** in the available data. "
            f"It may not be present, or the question might need rephrasing. "
            f"Here are a few things I can do:"
        ),
        options=options,
        footer_note="Tap an option below, or type the letter (A, B, C)."
    )
    return card, options


# ── Cache helpers ─────────────────────────────────────────────────────────────

def get_user_session(user_name):
    return _session_store.setdefault(user_name, {
        "history": [], "results": {},
        "current_job_id": None, "current_query": None,
        "posted_result": None, "last_inline": None,
        "pending_options": None
    })


def store_result(user_name, query, result):
    session = get_user_session(user_name)
    session["results"][query] = result
    print(f"  Stored result for query: '{query}'")


def get_cached_result(user_name, query):
    return get_user_session(user_name)["results"].get(query)


# ── Intent handlers ───────────────────────────────────────────────────────────

GREETING_EXACT = {
    "hi", "hello", "hey", "hii", "hiii", "yo", "sup", "good morning",
    "good afternoon", "good evening", "greetings", "start", "menu",
    "hi there", "hello there"
}

# Phrases that indicate the user wants the options menu, even if not an
# exact greeting — covers "what can you help with", "show me options", etc.
# Kept deliberately narrow to avoid matching real data questions
# (e.g. "help me with revenue by region" must NOT trigger this).
OPTIONS_REQUEST_KEYWORDS = [
    "what can you do", "what can you help me with", "what do you do",
    "what are my options", "give me options", "give me some options",
    "show me options", "show options", "list options",
    "options to select from", "select an option",
    "what can i ask you", "what should i ask you",
]

def _is_greeting(text):
    cleaned = text.strip().lower().rstrip("!.?")
    if cleaned in GREETING_EXACT:
        return True
    return any(kw in cleaned for kw in OPTIONS_REQUEST_KEYWORDS)


async def handle_chat(context: TurnContext, user_message, user_name):
    # Note: greeting/show_options detection now happens upstream in
    # _route_user_message before this is called, so handle_chat is only
    # reached for genuine free-form chat messages.
    session  = get_user_session(user_name)
    history  = session["history"]
    response = generate_chat_response(user_message, history)
    history.append({"role": "user", "content": user_message})
    history.append({"role": "assistant", "content": response})
    if len(history) > 20:
        session["history"] = history[-20:]
    await context.send_activity(response)


async def handle_clarification(context: TurnContext, user_message, user_name):
    session  = get_user_session(user_name)
    history  = session["history"]
    response = generate_clarification(user_message, history)
    history.append({"role": "user", "content": user_message})
    history.append({"role": "assistant", "content": response})
    session["history"] = history
    await context.send_activity(response)


def _detect_ambiguity(user_message):
    """
    Cheap, local (no LLM) check for genuinely ambiguous data questions that
    benefit from a clarifying question. Returns (clarify_title, options) where
    each option is (label, resolved_prompt) — the resolved prompt is a rewritten
    query that re-enters the data flow unambiguously — or None if the query is
    clear enough to run directly.

    Only fires on a few high-value ambiguity patterns to avoid friction:
      1. "top/bottom N <entity>" with no metric  → by revenue? orders? units?
      2. "show/list orders/sales ..." that could be a count OR a list
    """
    q = user_message.lower().strip()

    # Belt-and-suspenders: never re-clarify a prompt that is itself a resolved
    # clarification answer (carries explicit metric/intent phrasing we generated).
    _resolved_markers = ["show each individual record with details",
                         "by total revenue", "by number of orders", "by total profit"]
    if any(m in q for m in _resolved_markers):
        return None

    # ── Pattern 1: ranking without a metric ──────────────────────────────────
    has_rank   = bool(re.search(r"\b(top|bottom|highest|lowest|best|worst)\s+\d*\b", q))
    has_metric = any(m in q for m in [
        "revenue", "sales", "order value", "orders", "profit", "units",
        "quantity", "margin", "lifetime value", "spend", "spent", "amount",
        "value", "count"
    ])
    rank_entity = any(e in q for e in ["customer", "product", "vendor", "supplier",
                                       "region", "channel", "category", "item"])
    if has_rank and rank_entity and not has_metric:
        base = user_message.strip().rstrip("?")
        return (
            "Which metric should I rank by?",
            [
                ("By total revenue",     f"{base} by total revenue"),
                ("By number of orders",  f"{base} by number of orders"),
                ("By total profit",      f"{base} by total profit"),
            ]
        )

    # ── Pattern 2: count vs list ambiguity for orders/sales over a range ──────
    mentions_orders = any(w in q for w in ["orders", "sales", "transactions", "invoices"])
    is_listish      = any(w in q for w in ["show", "list", "display", "get", "give", "find"])
    asks_count      = any(w in q for w in ["how many", "count", "number of", "total number"])
    has_timeframe   = bool(re.search(r"\b(between|from|during|in|for|last|this|q[1-4]|"
                                     r"jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|"
                                     r"20\d\d)\b", q))
    if mentions_orders and is_listish and has_timeframe and not asks_count:
        base = user_message.strip().rstrip("?")
        return (
            "Do you want the full list or just the count?",
            [
                ("List the individual records", f"{base} — show each individual record with details"),
                ("Just the total count",        f"how many {base}"),
            ]
        )

    return None


def _empty_result_is_valid(user_message, sql):
    """
    Decide whether a 0-row result is a VALID 'none' answer (existence/anti-join
    question) vs a genuine NO-MATCH (filter matched nothing real).

    Two-stage for accuracy with minimal token cost:
      1. Keyword / SQL-shape fast-path (zero tokens) for the obvious cases.
      2. LLM fallback for novel phrasing — only runs on the rare 0-row case.
    Returns True if the empty result should be shown as a clear 'none' answer.
    """
    q   = user_message.lower()
    sql_l = (sql or "").lower()

    # ── Fast-path: strong existence/anti-join signals in the question ─────────
    existence_words = [
        "never", "not ordered", "without", "no order", "haven't", "have not",
        "didn't", "did not", "neither", "none ", "no sales", "zero sales",
        "zero orders", "never bought", "never purchased", "not purchased",
        "not bought", "no purchase", "yet to",
        # Data-quality / integrity checks — an empty result means the data is
        # CLEAN, which is a meaningful answer, not a dead-end (tests #44-46):
        "duplicate", "duplicated", "same email", "shared email", "sharing the same",
        "not matching", "doesn't match", "does not match", "mismatch",
        "no associated", "unassociated", "orphan", "missing customer",
        "missing vendor", "missing product", "invalid reference",
        # Multi-condition existence ("orders containing at least 3 categories"):
        "at least",
    ]
    if any(w in q for w in existence_words):
        return True

    # "both X and Y" / "all of" intersection questions are existence-style
    if ("both " in q or "all of " in q) and any(
            e in q for e in ["bought", "purchased", "ordered", "customers", "users"]):
        return True

    # SQL-shape signals: anti-join (LEFT/NOT) + IS NULL, or NOT EXISTS / NOT IN,
    # a HAVING COUNT(...) comparison (intersections AND duplicate checks like
    # HAVING COUNT(*) > 1), or a value-inequality integrity check. These
    # structurally produce a meaningful empty set, not a failed filter.
    sql_antijoin = (
        ("left join" in sql_l and "is null" in sql_l) or
        ("join" in sql_l and "is null" in sql_l and "where" in sql_l) or
        "not exists" in sql_l or
        "not in (" in sql_l or
        bool(re.search(r"having\s+count\s*\(.*?\)\s*[=><]", sql_l)) or
        bool(re.search(r"where\s+.*\s*(<>|!=)\s*", sql_l))   # integrity mismatch check
    )
    if sql_antijoin:
        return True

    # ── LLM fallback for anything ambiguous (rare path; 0-row only) ───────────
    try:
        prompt = (
            "A SQL query returned ZERO rows. Decide if zero rows is a MEANINGFUL "
            "answer to the user's question (i.e. the correct answer is genuinely "
            "'none' / 'no such records exist'), or if it more likely means the "
            "query's filter simply did not match anything (a dead-end the user "
            "should rephrase).\n\n"
            f'User question: "{user_message}"\n'
            f"SQL: {sql}\n\n"
            "Answer with ONLY one word:\n"
            "  valid   → zero rows is a correct, meaningful 'none' answer\n"
            "  nomatch → zero rows is a dead-end / likely filter miss"
        )
        resp = _groq_chat(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=4,
        )
        verdict = (resp.choices[0].message.content or "").strip().lower()
        print(f"  [Empty] LLM verdict on 0-row result: '{verdict}'")
        return verdict.startswith("valid")
    except GroqRateLimitError:
        # On rate limit, fall back to the safer default: show options card.
        return False
    except Exception as e:
        print(f"  [Empty] classifier fallback error ({e}) — defaulting to options card")
        return False


def _phrase_empty_answer(user_message):
    """
    A direct, natural 'the answer is none' sentence for a valid empty result.
    Tester feedback (#41, #45): the reply should read like a person answering —
    e.g. "No customers were found sharing the same email address" — not a
    boilerplate card. We tailor the noun/phrasing to the question where we can
    recognise it, and fall back to a clean generic sentence otherwise.
    """
    q = user_message.lower()

    # Recognisable patterns → tailored one-liner
    if "duplicate" in q or "same email" in q or "sharing the same" in q or "shared email" in q:
        return ("No duplicates were found — every customer in the data has a "
                "unique email address.")
    if "no associated" in q or "orphan" in q or "missing customer" in q:
        return ("Every order in the data has an associated customer — there are "
                "no orphaned or unattributed orders.")
    if "not matching" in q or "mismatch" in q or "does not match" in q or "doesn't match" in q:
        return ("No mismatches were found — all records are internally consistent "
                "for that check.")
    if "at least" in q and ("categor" in q or "product" in q):
        return ("No orders were found meeting that condition — no single order "
                "contains products from that many different categories.")
    if "never" in q or "haven't" in q or "have not" in q or "didn't" in q or "did not" in q:
        return ("None — no records in the data match that condition.")

    # Generic fallback — still a plain sentence, not a card
    return (
        "The answer is none — no records in the data match that condition. "
        "If you expected results, the data may simply not contain any, or you "
        "can rephrase to broaden the criteria."
    )


_MONTH_NAMES = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,
    "oct":10,"nov":11,"dec":12,
}


def _data_query_precheck(user_message):
    """
    Deterministic, no-LLM guard for impossible / unanswerable data questions.
    Returns a ready-to-send reply string if the query should be short-circuited,
    else None. Hardens the previously LLM-dependent behaviours.
    """
    q = user_message.lower()

    # ── #35 — impossible month number (13th month, month 0, month 13..99) ────
    # Patterns like "13th month", "month 13", "the 0th month".
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+month\b", q)
    if not m:
        m = re.search(r"\bmonth\s+(\d{1,2})\b", q)
    if m:
        mon = int(m.group(1))
        if mon < 1 or mon > 12:
            return (
                f"There's no month {mon} — months run from 1 (January) to "
                "12 (December). Did you mean a month in that range, for example "
                "November (11) or December (12)?"
            )

    # ── #22 — time-of-day questions (data is date-only, no hour) ─────────────
    timeofday_signals = [
        "busiest hour", "what hour", "which hour", "by hour", "per hour",
        "hour of the day", "time of day", "hourly", "morning or evening",
        "morning vs evening", "am or pm", "what time of day", "peak hour",
        "busiest time", "time of the day",
    ]
    if any(s in q for s in timeofday_signals):
        return (
            "I can't break this down by hour or time of day — orders are "
            "recorded by date only, without a time component. I can show you "
            "orders by day, day-of-week, month, quarter, or year instead. "
            "Would any of those help?"
        )

    # ── #44 — line-item integrity questions the data model can't express ─────
    # Each fact_sales row IS a complete single-product transaction (sale_id is
    # the line grain — there is no separate order header vs. line items). So
    # "order total vs sum of line items" has no meaning here; say so plainly
    # instead of letting the model invent a bogus comparison.
    lineitem_signals = [
        "sum of line items", "line items", "line item", "order lines",
        "invoice lines", "order header", "basket total",
    ]
    if any(s in q for s in lineitem_signals) and (
            "match" in q or "total" in q or "sum" in q or "reconcile" in q):
        return (
            "This data doesn't have separate order headers and line items — each "
            "sale record is a single-product transaction, complete in itself. "
            "So there's no 'order total vs sum of line items' to reconcile.\n\n"
            "If you're checking data quality, I can verify things like "
            "revenue = price × units sold, or net_revenue = revenue − discount, "
            "across all records. Want me to run one of those checks?"
        )

    # ── #34 — self-contradictory / mind-changing message ─────────────────────
    contradiction_markers = ["actually no", "actually,", "no wait", "wait,",
                             "never mind", "nevermind", "scratch that", "i mean",
                             "...", "on second thought"]
    hits = sum(mk in q for mk in contradiction_markers)
    # Also count how many distinct metrics are mentioned — multiple conflicting
    # metrics with a reversal marker = genuinely contradictory.
    metrics_mentioned = sum(kw in q for kw in
                            ["revenue", "cost", "costs", "profit", "margin", "sales"])
    if hits >= 1 and (("wait" in q or "actually" in q or "never mind" in q
                       or "i mean" in q or q.count("...") >= 1)
                      and metrics_mentioned >= 2):
        return (
            "It sounds like you're weighing a few different things — I want to "
            "get the right one. Which would you like to see?\n\n"
            "• **Revenue**  • **Costs**  • **Profit**\n\n"
            "Just tell me which (or describe what you're after) and I'll pull it."
        )

    return None


async def handle_data_query(context: TurnContext, user_message, user_name):
    session = get_user_session(user_name)

    # ── Deterministic pre-checks (no LLM) ────────────────────────────────────
    # Catch impossible / unanswerable requests in Python so correctness does
    # not depend on the model emitting a sentinel. Returns a ready reply string.
    precheck = _data_query_precheck(user_message)
    if precheck:
        await context.send_activity(precheck)
        print(f"  [Precheck] Short-circuited: {precheck[:60]}...")
        return

    # ── Clarify genuinely ambiguous queries before spending a SQL call ───────
    # Skip if this message is itself a resolved clarification (avoids loops).
    if not session.get("_skip_clarify"):
        ambiguity = _detect_ambiguity(user_message)
        if ambiguity:
            title, options = ambiguity
            store_pending_options(user_name, options)
            card = build_options_card(
                title=title,
                description="Just to make sure I answer exactly what you need:",
                options=options,
                footer_note="Reply with a letter (A, B, …) or tap an option."
            )
            await context.send_activity(make_activity_with_card(card))
            print(f"  [Clarify] Ambiguous query → asked: {title}")
            return
    session["_skip_clarify"] = False  # reset one-shot bypass

    cached_queries = list(session["results"].keys())
    matched_query  = find_similar_cached_query(user_message, cached_queries)

    if matched_query:
        session["pending_action"] = {"type": "data_query", "new_query": user_message, "matched_query": matched_query}
        card = build_cache_confirmation_card(user_message, matched_query, "data_query")
        await context.send_activity(make_activity_with_card(card))
        return

    await _run_fresh_data_query(context, user_message, user_name, session["history"])


def correct_sql_after_db_error(user_message, bad_sql, db_error, conversation_history, user_name=None):
    """
    Unlike _check_sql_quality's heuristic pre-checks inside generate_sql(), this
    feeds the REAL error returned by the SQL Server engine back to the model —
    catching mistakes (e.g. wrong column names) that pass the heuristic checks
    but fail at execution time. Mirrors the same validate-and-retry shape used
    elsewhere in this bot.
    """
    live_schema, _ = get_schema_context()
    prompt = f"""You are an expert T-SQL analyst for the EcommereAnalytics gold layer
(Microsoft Fabric SQL Analytics endpoint).

### DATABASE SCHEMA
{live_schema}

### RULES
{SQL_RULES}

### USER REQUEST
{user_message}

### YOUR PREVIOUS SQL (FAILED)
{bad_sql}

### REAL DATABASE ERROR
{db_error}

Fix the SQL so it executes successfully against the schema above. Common cause:
a column name that doesn't literally exist in the table you queried — check the
schema above carefully for the correct column name. Return ONLY the corrected
T-SQL query, no markdown, no explanation."""
    response = _groq_chat(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=700
    )
    sql = response.choices[0].message.content.strip()
    return sql.replace("```sql", "").replace("```", "").strip()


async def _run_fresh_data_query(context: TurnContext, user_message, user_name, history):
    await context.send_activity("Querying the data...")
    try:
        sql                = generate_sql(user_message, history, user_name=user_name)
        print(f"  Generated SQL: {sql}")

        # LLM signalled it cannot form a valid query (read-only refusal is also
        # returned for unanswerable/out-of-scope requests in some edge cases)
        if sql.strip() == _READONLY_MSG.strip():
            print("  [NoData] LLM returned read-only refusal — likely unanswerable request")
            card, options = build_no_data_options_card(user_message)
            store_pending_options(user_name, options)
            await context.send_activity(make_activity_with_card(card))
            return

        rows_data, columns = None, None
        try:
            rows_data, columns = execute_sql_query(sql)
        except Exception as db_err:
            # Real execution error (e.g. invalid column) — not caught by the
            # heuristic quality check inside generate_sql. Feed the actual
            # error back for one correction pass before giving up.
            print(f"  [SQL] Execution failed, attempting one correction pass: {db_err}")
            sql = correct_sql_after_db_error(user_message, sql, str(db_err), history, user_name=user_name)
            print(f"  [SQL] Corrected SQL:\n{sql}")
            rows_data, columns = execute_sql_query(sql)
        print(f"  Query returned {len(rows_data)} rows")

        # Single-message sentinels from SQL generation — surface them plainly,
        # never as a chart/table.
        if (len(rows_data) == 1 and len(columns) == 1
                and str(columns[0]).lower() == "message"):
            msg_val = str(list(rows_data[0].values())[0])
            if msg_val.startswith("INVALID_DATE:"):
                reason = msg_val.split(":", 1)[1].strip()
                await context.send_activity(
                    f"That date doesn't look valid — {reason}. "
                    "Could you double-check it? For example, months run from "
                    "1 (January) to 12 (December)."
                )
                print(f"  [InvalidDate] {reason}")
                return
            if msg_val.startswith("NO_TIMEOFDAY:"):
                reason = msg_val.split(":", 1)[1].strip()
                await context.send_activity(
                    f"I can't answer that — {reason}. "
                    "I can break orders down by day, month, quarter, or year instead. "
                    "Would any of those help?"
                )
                print(f"  [NoTimeOfDay] {reason}")
                return
            if "read-only" in msg_val.lower() or "not permitted" in msg_val.lower():
                await context.send_activity(
                    "I can't make changes to the data — I have **read-only** access. "
                    "I can help you find and analyse data instead."
                )
                print("  [Readonly] SQL-level modification attempt refused")
                return

        if not rows_data:
            # A 0-row result has two very different meanings:
            #   (a) VALID "none" — an existence/anti-join question correctly finds
            #       nothing (e.g. "products never ordered", "customers who bought
            #       both X and Y"). The right response is a clear "the answer is none".
            #   (b) NO MATCH — a normal query whose filter matched nothing real
            #       (e.g. a misspelled region). The right response is the options card.
            # _empty_result_is_valid() decides accurately using the question AND the
            # SQL shape, with a keyword fast-path and an LLM fallback for novel phrasing.
            if _empty_result_is_valid(user_message, sql):
                friendly = _phrase_empty_answer(user_message)
                await context.send_activity(friendly)
                print("  [Empty] Existence/anti-join query → clear 'none' answer")
                return
            card, options = build_no_data_options_card(user_message)
            store_pending_options(user_name, options)
            await context.send_activity(make_activity_with_card(card))
            return

        # A query can technically "succeed" but return rows where every
        # value is NULL (e.g. GROUP BY a filter that matched nothing real,
        # like a misspelled region name with an outer join producing one
        # all-NULL row). That's not usable data — treat it as no-data too.
        if _all_values_null(rows_data):
            print("  [NoData] Query succeeded but all values are NULL — treating as no data")
            card, options = build_no_data_options_card(user_message)
            store_pending_options(user_name, options)
            await context.send_activity(make_activity_with_card(card))
            return

        x_column, y_column = detect_chart_columns(rows_data, columns)
        summary            = generate_data_summary(user_message, rows_data, x_column, y_column)
        chart_base64       = None
        # Default title for label-only / no-metric results (no chart generated).
        # Always assigned so the downstream _chart_store never hits an unbound 'title'.
        title              = user_message.strip().title()
        if x_column and y_column:
            # Build a descriptive chart title
            y_label = y_column.replace('_', ' ').title()
            x_label = x_column.replace('_', ' ').title()
            # If x is month and data spans multiple years, note the year range
            if 'month' in x_column.lower() and 'year' in str(list(rows_data[0].keys())).lower():
                years = sorted({r.get('year') or r.get('Year') for r in rows_data if r.get('year') or r.get('Year')} - {None})
                if len(years) > 1:
                    x_label = f"Month ({years[0]}–{years[-1]})"
            if x_column == y_column or len(rows_data) == 1:
                title = y_label
            else:
                title = f"{y_label} by {x_label}"
            chart_base64 = generate_chart_base64(rows_data, x_column, y_column, title)

        # Store chart data for Task Module interactive view (30 min TTL)
        chart_id = str(uuid.uuid4())[:8]
        _chart_store[chart_id] = {
            "rows_data":  rows_data,
            "x_column":  x_column,
            "y_column":  y_column,
            "title":     title,
            "query":     user_message,
            "stored_at": time.time()
        }

        result = {"rows_data": rows_data, "x_column": x_column, "y_column": y_column, "user_query": user_message, "sql": sql}
        store_result(user_name, user_message, result)

        session = get_user_session(user_name)
        history.append({"role": "user", "content": user_message})
        history.append({"role": "assistant", "content": f"Query executed. {len(rows_data)} rows returned. {summary}"})
        if len(history) > 20:
            history = history[-20:]
        session["history"] = history

        card = build_data_query_card(user_message, summary, rows_data, x_column, y_column, chart_base64, sql, chart_id=chart_id)
        await context.send_activity(make_activity_with_card(card))

    except GroqRateLimitError as e:
        wait_hint = f" Please try again in about {e.retry_after}." if e.retry_after else " Please try again in a few minutes."
        print(f"  Data query error: rate limit reached — {e}")
        await context.send_activity(
            "I'm temporarily over capacity on the AI service and can't process "
            "this query right now." + wait_hint
        )
        return

    except Exception as e:
        error_str = str(e)
        print(f"  Data query error: {error_str}")

        # Infrastructure/connection errors are NOT "no data" — show the real
        # error so the user (or developer) knows it's a system issue, not a
        # data-availability issue.
        infra_error_markers = [
            "Login failed", "timeout", "ConnectionError", "08001",
            "ODBC", "Could not login", "authentication", "08S01"
        ]
        if any(marker.lower() in error_str.lower() for marker in infra_error_markers):
            await context.send_activity(
                "I'm having trouble connecting to the database right now. "
                "Please try again in a moment."
            )
            return

        # Otherwise it's most likely a schema mismatch — show the no-data
        # options card so the user has a clear next step.
        card, options = build_no_data_options_card(user_message)
        store_pending_options(user_name, options)
        await context.send_activity(make_activity_with_card(card))


async def _serve_cached_data_query(context: TurnContext, user_message, matched_query, user_name):
    result       = get_cached_result(user_name, matched_query)
    rows_data    = result["rows_data"]
    x_column     = result["x_column"]
    y_column     = result["y_column"]
    sql          = result["sql"]
    summary      = generate_data_summary(user_message, rows_data, x_column, y_column)
    chart_base64 = None
    if x_column and y_column:
        title        = _build_chart_title(rows_data, x_column, y_column, user_message)
        chart_base64 = generate_chart_base64(rows_data, x_column, y_column, title)
    card = build_data_query_card(user_message, summary, rows_data, x_column, y_column, chart_base64, sql)
    await context.send_activity(make_activity_with_card(card))


async def handle_report_request(context: TurnContext, user_message, user_name):
    session        = get_user_session(user_name)
    history        = session["history"]
    cached_queries = list(session["results"].keys())
    matched_query  = find_similar_cached_query(user_message, cached_queries)

    if matched_query:
        cached = get_cached_result(user_name, matched_query)
        if cached and cached.get("report_url"):
            session["pending_action"] = {"type": "report_request", "new_query": user_message, "matched_query": matched_query}
            card = build_cache_confirmation_card(user_message, matched_query, "report_request")
            await context.send_activity(make_activity_with_card(card))
            return

    history.append({"role": "user", "content": user_message})
    session["history"] = history
    await _run_fresh_report(context, user_message, user_name)


async def _run_fresh_report(context: TurnContext, user_message, user_name):
    job_id  = str(uuid.uuid4())[:8]
    session = get_user_session(user_name)
    session["current_job_id"] = job_id
    session["current_query"]  = user_message
    session["posted_result"]  = None

    try:
        location = trigger_notebook(user_message, user_name, job_id)
        await poll_and_update_card(context, location, user_message, user_name, job_id)
    except Exception as e:
        print(f"Error: {e}")
        await context.send_activity(f"Sorry, something went wrong generating the report.\n\nError: `{str(e)}`")


async def _serve_cached_report(context: TurnContext, user_message, matched_query, user_name):
    result          = get_cached_result(user_name, matched_query)
    report_url      = result.get("report_url")
    rows_data       = result.get("rows_data")
    x_column        = result.get("x_column")
    y_column        = result.get("y_column")
    has_inline_data = bool(rows_data and x_column and y_column)

    session = get_user_session(user_name)
    if has_inline_data:
        session["last_inline"] = {"rows_data": rows_data, "x_column": x_column, "y_column": y_column, "user_query": user_message}

    card = build_report_card(user_message, "done", is_done=True, report_url=report_url, has_inline_data=has_inline_data)
    await context.send_activity(make_activity_with_card(card))


async def handle_inline_view(context: TurnContext, user_name):
    session = get_user_session(user_name)
    result  = session.get("last_inline")

    if not result:
        results = session.get("results", {})
        if results:
            latest_key = list(results.keys())[-1]
            result     = results[latest_key]

    if not result:
        await context.send_activity("Inline report data is no longer available. Please run your query again.")
        return

    rows_data  = result["rows_data"]
    x_column   = result["x_column"]
    y_column   = result["y_column"]
    user_query = result.get("user_query", "")

    await context.send_activity("Generating inline report...")
    title        = _build_chart_title(rows_data, x_column, y_column, user_query)
    chart_base64 = generate_chart_base64(rows_data, x_column, y_column, title)
    inline_card  = build_inline_report_card(user_query, rows_data, x_column, y_column, chart_base64)
    await context.send_activity(make_activity_with_card(inline_card))


async def handle_generate_report_from_card(context: TurnContext, user_name):
    session = get_user_session(user_name)
    results = session.get("results", {})

    if not results:
        await context.send_activity("No recent query found. Please ask your data question again.")
        return

    latest_key = list(results.keys())[-1]
    result     = results[latest_key]
    user_query = result.get("user_query", latest_key)

    await context.send_activity(f"Generating a Power BI report for: **{user_query}**")
    await _run_fresh_report(context, user_query, user_name)



async def handle_file_upload(context: TurnContext, user_message, user_name):
    """Respond to file upload intent — send upload link or check status."""
    upload_info = get_upload_info(user_name)

    # upload_info is now a list of entries
    pending = [e for e in upload_info if e.get("status") == "pending"]
    failed  = [e for e in upload_info if e.get("status") == "failed"]
    ready   = [e for e in upload_info if e.get("status") == "ready"]

    # If any pending uploads — notify
    if pending:
        names = ", ".join(f"**{e['filename']}**" for e in pending)
        await context.send_activity(
            f"{names} {'is' if len(pending)==1 else 'are'} still being processed. "
            "Please wait a moment and try again."
        )
        return

    # Clear failed entries and notify
    if failed:
        for e in failed:
            await context.send_activity(
                f"Previous upload of **{e['filename']}** failed: `{e.get('error', 'Unknown error')}`. "
                "Please try uploading again."
            )
        norm = user_name.lower()
        if norm in _upload_registry:
            _upload_registry[norm] = [e for e in _upload_registry[norm] if e.get("status") != "failed"]

    # If user has ready files and is asking to compare
    comparison_keywords = ["compare", "versus", "vs", "match", "against", "analyse", "analyze"]
    if ready and any(kw in user_message.lower() for kw in comparison_keywords):
        await handle_comparison_query(context, user_message, user_name)
        return

    # If files are ready — show what's loaded
    if ready:
        file_list = "\n".join(
            f"• **{e['filename']}** ({e['row_count']:,} rows) → `{e['table_name']}`"
            for e in ready
        )
        await context.send_activity(
            f"You have {len(ready)} file(s) loaded:\n{file_list}\n\n"
            "You can ask me to compare any of them with Fabric data, or upload another file below."
        )

    upload_url = f"{NGROK_DOMAIN}/upload"
    card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5",
            "body": [
                {"type": "TextBlock", "text": "AiRo Bot", "size": "Small", "color": "Accent", "weight": "Bolder"},
                {"type": "TextBlock", "text": "Upload Your File", "size": "Large", "weight": "Bolder", "color": "Accent", "spacing": "None"},
                {
                    "type": "TextBlock",
                    "text": "Upload an Excel or CSV file to compare it with data in the Fabric Lakehouse.",
                    "wrap": True, "size": "Small", "spacing": "Medium"
                },
                {
                    "type": "FactSet",
                    "spacing": "Medium",
                    "facts": [
                        {"title": "Supported formats", "value": ".xlsx, .xls, .csv"},
                        {"title": "Max file size",     "value": "15 MB"},
                        {"title": "Session scope",     "value": "File is available for this session only"},
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": "After uploading, come back here and ask something like: 'Compare my vendor prices with Fabric vendor data'",
                    "wrap": True, "size": "Small", "color": "Accent", "spacing": "Medium"
                }
            ],
            "actions": [
                {"type": "Action.OpenUrl", "title": "Upload File", "url": upload_url, "style": "positive"}
            ]
        }
    }
    await context.send_activity(make_activity_with_card(card))


async def handle_comparison_query(context: TurnContext, user_message, user_name):
    """Run a comparison query between uploaded file and Fabric data."""
    ready_uploads = get_ready_uploads(user_name.lower())
    if not ready_uploads:
        await context.send_activity(
            "No uploaded files found for your session. Please upload a file first."
        )
        return

    session = get_user_session(user_name)
    history = session["history"]

    if len(ready_uploads) == 1:
        file_desc = f"**{ready_uploads[0]['filename']}** ({ready_uploads[0]['row_count']:,} rows)"
    else:
        file_desc = f"{len(ready_uploads)} uploaded files"

    await context.send_activity(f"Comparing {file_desc} with Fabric data...")

    try:
        sql, err = generate_comparison_suggestion(user_name, user_message)
        if err:
            await context.send_activity(f"Could not generate comparison: {err}")
            return

        print(f"  Comparison SQL: {sql}")

        if _is_dangerous_sql(sql):
            await context.send_activity("The generated query was blocked for security reasons.")
            return

        rows_data, columns = execute_sql_query(sql)

        if not rows_data:
            await context.send_activity(
                "The comparison query returned no results. "
                "The uploaded data may not have matching records with the Fabric data."
            )
            return

        x_column, y_column = detect_chart_columns(rows_data, columns)
        summary = generate_data_summary(user_message, rows_data, x_column, y_column)

        chart_base64 = None
        if x_column and y_column:
            title        = "Comparison: " + _build_chart_title(rows_data, x_column, y_column, user_message)
            chart_base64 = generate_chart_base64(rows_data, x_column, y_column, title)

        # Store result
        result = {"rows_data": rows_data, "x_column": x_column, "y_column": y_column,
                  "user_query": user_message, "sql": sql}
        store_result(user_name, user_message, result)

        history.append({"role": "user", "content": user_message})
        history.append({"role": "assistant", "content": f"Comparison complete. {len(rows_data)} rows returned. {summary}"})
        session["history"] = history[-20:]

        # Build card with upload info banner
        card_body = [
            {"type": "TextBlock", "text": "AiRo Bot", "size": "Small", "color": "Accent", "weight": "Bolder"},
            {"type": "TextBlock", "text": "File Comparison Results", "size": "Large", "weight": "Bolder", "color": "Accent", "spacing": "None"},
            {
                "type": "ColumnSet", "spacing": "Medium",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "Your file:", "weight": "Bolder", "size": "Small", "color": "Accent"}]},
                    {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": f"{upload_info['filename']} ({upload_info['row_count']:,} rows)", "size": "Small"}]}
                ]
            },
            {"type": "TextBlock", "text": summary, "wrap": True, "size": "Small", "spacing": "Small"},
        ]

        if chart_base64:
            card_body.append({"type": "Image", "url": f"data:image/png;base64,{chart_base64}", "size": "Stretch", "spacing": "Medium"})

        # Data table (shared helper)
        if rows_data:
            card_body.extend(_build_data_table(rows_data, y_column))

        # SQL toggle
        card_body.append({"type": "ActionSet", "spacing": "Medium", "actions": [{"type": "Action.ToggleVisibility", "title": "Show SQL", "targetElements": ["comp_sql_container"]}]})
        card_body.append({"type": "Container", "id": "comp_sql_container", "isVisible": False, "spacing": "Small", "style": "emphasis", "items": [{"type": "TextBlock", "text": sql, "wrap": True, "size": "Small"}]})

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.5", "body": card_body,
                "actions": [{"type": "Action.Execute", "title": "Generate Power BI Report", "verb": "generate_report", "data": {"action": "generate_report"}}]
            }
        }
        await context.send_activity(make_activity_with_card(card))

    except Exception as e:
        print(f"  Comparison error: {e}")
        await context.send_activity(f"Comparison failed: `{str(e)}`")


# ── Polling ───────────────────────────────────────────────────────────────────

async def poll_and_update_card(context: TurnContext, location, user_query, user_name, job_id):

    initial_card = build_report_card(user_query, "waiting")
    response     = await context.send_activity(make_activity_with_card(initial_card))

    card_activity_id = None
    if response and hasattr(response, "id") and response.id:
        card_activity_id = response.id
        print(f"  Initial card sent. Activity ID: {card_activity_id}")
    else:
        print("  Warning: Could not get activity ID")

    async def update_card(step, is_done=False, is_failed=False,
                          report_url=None, sql=None, template=None,
                          rows=None, has_inline_data=False):
        card = build_report_card(
            user_query, step,
            is_done=is_done, is_failed=is_failed,
            report_url=report_url, sql=sql,
            template=template, rows=rows,
            has_inline_data=has_inline_data
        )
        if card_activity_id:
            activity      = make_activity_with_card(card)
            activity.id   = card_activity_id
            try:
                await context.update_activity(activity)
                print(f"  Card updated: step={step}")
            except Exception as e:
                print(f"  Card update failed: {e} — sending new card")
                await context.send_activity(make_activity_with_card(card))
        else:
            await context.send_activity(make_activity_with_card(card))

    step_schedule = [
        (0, "waiting"), (10, "running_1"), (25, "running_2"),
        (50, "running_3"), (80, "running_4"), (110, "running_5"), (140, "running_6"),
    ]

    start_time = time.time()
    last_step  = "waiting"

    for i in range(120):
        await asyncio.sleep(10)
        elapsed = time.time() - start_time

        current_step = "waiting"
        for threshold, step in step_schedule:
            if elapsed >= threshold:
                current_step = step

        try:
            job_status = get_job_status(location)
        except Exception as e:
            print(f"  Job poll error: {e}")
            job_status = "Unknown"

        print(f"  Poll {i+1}: job={job_status} elapsed={int(elapsed)}s step={current_step}")

        if current_step != last_step:
            await update_card(current_step)
            last_step = current_step

        if job_status == "Completed":
            print("  Job completed — waiting for notebook POST...")
            await asyncio.sleep(8)

            session       = get_user_session(user_name)
            posted_result = session.get("posted_result")

            print(f"  Expected job_id: {job_id}")
            print(f"  Posted result job_id: {posted_result.get('job_id') if posted_result else 'None'}")

            sql = template = rows = rows_data = x_column = y_column = report_url = None

            if posted_result and posted_result.get("job_id") == job_id:
                print("  Job ID matched.")
                template   = posted_result.get("template")
                rows_data  = posted_result.get("rows_data")
                x_column   = posted_result.get("x_column")
                y_column   = posted_result.get("y_column")
                sql        = posted_result.get("sql")
                report_url = posted_result.get("report_url")
                if rows_data:
                    rows = len(rows_data)
            else:
                print("  Job ID mismatch — falling back to workspace lookup")

            if not report_url:
                report_url = get_report_url(user_name)

            has_inline_data = bool(rows_data and x_column and y_column)
            print(f"  has_inline_data: {has_inline_data}")

            if report_url:
                store_result(user_name, user_query, {
                    "rows_data": rows_data, "x_column": x_column,
                    "y_column": y_column, "user_query": user_query,
                    "sql": sql, "template": template, "report_url": report_url
                })
                if has_inline_data:
                    session["last_inline"] = {"rows_data": rows_data, "x_column": x_column, "y_column": y_column, "user_query": user_query}

                await update_card("done", is_done=True, report_url=report_url, sql=sql, template=template, rows=rows, has_inline_data=has_inline_data)
            else:
                await update_card("failed", is_failed=True)
            return

        if job_status in ("Failed", "Cancelled"):
            await update_card("failed", is_failed=True)
            return

    await update_card("failed", is_failed=True)


# ── Main agent message handler ────────────────────────────────────────────────

@AGENT_APP.activity("message")
async def on_message(context: TurnContext, _state: TurnState):
    activity = context.activity

    # Handle Action.Execute card button clicks (arrive as message with activity.value set)
    if activity.value:
        action_data = activity.value if isinstance(activity.value, dict) else {}
        action      = action_data.get("action", "")
        user_name   = (activity.from_property.name or "user").replace(" ", "_")
        session     = get_user_session(user_name)

        if action == "view_inline":
            await handle_inline_view(context, user_name)
            return True

        elif action == "generate_report":
            await handle_generate_report_from_card(context, user_name)
            return True

        elif action == "use_cache":
            matched_query             = action_data.get("matched_query")
            new_query                 = action_data.get("new_query")
            intent                    = action_data.get("intent")
            session["pending_action"] = None
            if intent == "data_query":
                await _serve_cached_data_query(context, new_query, matched_query, user_name)
            elif intent == "report_request":
                await _serve_cached_report(context, new_query, matched_query, user_name)
            return True

        elif action == "run_fresh":
            new_query                 = action_data.get("new_query")
            intent                    = action_data.get("intent")
            session["pending_action"] = None
            history                   = session["history"]
            if intent == "data_query":
                await _run_fresh_data_query(context, new_query, user_name, history)
            elif intent == "report_request":
                await _run_fresh_report(context, new_query, user_name)
            return True

        elif action == "quick_option":
            prompt = action_data.get("prompt", "")
            session["pending_options"] = None
            if prompt:
                session["_skip_clarify"] = True
                await _route_user_message(context, prompt, user_name)
            return True

    user_message = (activity.text or "").strip()
    if not user_message:
        return True

    user_name = (activity.from_property.name or "user").replace(" ", "_")
    print(f"Received: '{user_message}' from '{user_name}'")

    # Typed-letter shortcut (A/B/C/D) resolves against the last shown option card
    resolved_prompt = resolve_typed_option(user_name, user_message)
    if resolved_prompt:
        print(f"  [Options] Typed letter resolved to: '{resolved_prompt}'")
        get_user_session(user_name)["_skip_clarify"] = True
        await _route_user_message(context, resolved_prompt, user_name)
        return True

    await _route_user_message(context, user_message, user_name)
    return True


def _is_destructive_request(user_message):
    """
    True if the user is asking to MODIFY data (delete/update/insert/etc.).
    Catches natural-language phrasing, not just SQL keywords, so the bot can
    decline plainly and upfront. The system is read-only → always refused.
    """
    q = user_message.lower()
    destructive_verbs = [
        "delete", "remove", "drop", "erase", "wipe", "purge",
        "update", "modify", "edit", "alter", "overwrite",
        "insert", "add a ", "add an ", "add new", "truncate",
        "reset the", "clear the", "change the",
    ]
    data_objects = [
        "order", "customer", "product", "vendor", "record", "row", "table",
        "sale", "invoice", "entry", "data", "transaction", "account", "item",
        "price", "field", "value",
    ]
    if not any(v in q for v in destructive_verbs):
        return False
    return any(o in q for o in data_objects)


async def _route_user_message(context: TurnContext, user_message, user_name):
    """Shared routing logic for both typed messages and resolved quick-options."""
    session = get_user_session(user_name)
    history = session["history"]
    # Pseudo-action prompts from option cards — handled directly,
    # never sent through intent classification or SQL generation.
    if user_message.startswith("__action:"):
        action_key = user_message.split(":", 1)[1]
        await _handle_pseudo_action(context, action_key, user_name)
        return

    # ── Destructive-intent gate ──────────────────────────────────────────────
    # The system is strictly read-only. If the user asks to delete / modify /
    # insert data, say so PLAINLY and IMMEDIATELY — do not route it through SQL
    # generation (which would return a confusing fake "query result"). This also
    # clears any half-finished pending action so a later "yes"/"delete it" can't
    # resurrect a destructive flow.
    if _is_destructive_request(user_message):
        session["pending_action"]  = None
        session["pending_options"] = None
        await context.send_activity(
            "I can't make changes to the data — I have **read-only** access. "
            "That means I can't delete, edit, add, or update any records "
            "(orders, customers, products, etc.).\n\n"
            "I can, however, help you **find and analyse** data: totals, trends, "
            "rankings, comparisons, or a Power BI report. What would you like to see?"
        )
        print(f"  [Readonly] Destructive request refused plainly: '{user_message}'")
        return

    # Fast-path: obvious exact greetings skip the LLM call entirely.
    # Everything else (including "tell me about yourself", "what can you
    # help with" in any phrasing) is classified by the LLM as show_options.
    if _is_greeting(user_message):
        intent = "show_options"
        print(f"  Intent: show_options (fast-path)")
    else:
        try:
            intent = classify_intent(user_message, history)
        except GroqRateLimitError as e:
            wait_hint = f" Please try again in about {e.retry_after}." if e.retry_after else " Please try again in a few minutes."
            print(f"  Intent classification rate-limited — {e}")
            await context.send_activity(
                "I'm temporarily over capacity on the AI service and can't "
                "process this right now." + wait_hint
            )
            return

    if intent == "show_options":
        card, options = build_welcome_options_card()
        store_pending_options(user_name, options)
        await context.send_activity(make_activity_with_card(card))
        history.append({"role": "user", "content": user_message})
        history.append({"role": "assistant", "content": "Showed welcome options menu."})
        session["history"] = history[-20:]
    elif intent == "chat":
        await handle_chat(context, user_message, user_name)
    elif intent == "clarification_needed":
        await handle_clarification(context, user_message, user_name)
    elif intent == "data_query":
        await handle_data_query(context, user_message, user_name)
    elif intent == "report_request":
        await handle_report_request(context, user_message, user_name)
    elif intent == "file_upload":
        await handle_file_upload(context, user_message, user_name)
    else:
        await handle_chat(context, user_message, user_name)


async def _handle_pseudo_action(context: TurnContext, action_key, user_name):
    """
    Handles option-card selections that map to an action rather than a
    real natural-language question. These never go through SQL generation —
    they either ask the user a genuine follow-up or trigger the right flow.
    """
    if action_key == "report_request":
        await context.send_activity(
            "Sure — what would you like the Power BI report to cover? "
            "For example: \"revenue by product over time\" or \"channel performance this year\"."
        )

    elif action_key == "ask_clarify":
        await context.send_activity(
            "What would you like to know? You can ask about revenue, profit, units sold, "
            "discounts, returns — by product, category, region, channel, or time period."
        )

    elif action_key == "file_upload":
        await handle_file_upload(context, "I want to upload a file", user_name)

    elif action_key == "chat_help":
        session  = get_user_session(user_name)
        response = generate_chat_response("What else can you help me with?", session["history"])
        await context.send_activity(response)

    elif action_key == "ask_different":
        await context.send_activity(
            "No problem — what would you like to ask instead?"
        )

    elif action_key == "rephrase":
        await context.send_activity(
            "Go ahead and rephrase your question — I'll give it another try."
        )

    else:
        await context.send_activity("Let me know what you'd like help with.")


@AGENT_APP.activity("invoke")
async def on_invoke(context: TurnContext, _state: TurnState):
    """Handle Action.Execute card button invokes."""
    activity    = context.activity
    user_name   = (activity.from_property.name or "user").replace(" ", "_")
    action_data = (activity.value or {}).get("action", {})
    verb        = action_data.get("verb", "")
    data        = action_data.get("data", {})
    action      = data.get("action", verb)
    session     = get_user_session(user_name)

    if action == "view_inline":
        await handle_inline_view(context, user_name)
    elif action == "generate_report":
        await handle_generate_report_from_card(context, user_name)
    elif action == "use_cache":
        matched_query             = data.get("matched_query")
        new_query                 = data.get("new_query")
        intent                    = data.get("intent")
        session["pending_action"] = None
        if intent == "data_query":
            await _serve_cached_data_query(context, new_query, matched_query, user_name)
        elif intent == "report_request":
            await _serve_cached_report(context, new_query, matched_query, user_name)
    elif action == "run_fresh":
        new_query                 = data.get("new_query")
        intent                    = data.get("intent")
        session["pending_action"] = None
        history                   = session["history"]
        if intent == "data_query":
            await _run_fresh_data_query(context, new_query, user_name, history)
        elif intent == "report_request":
            await _run_fresh_report(context, new_query, user_name)
    elif action == "quick_option":
        prompt = data.get("prompt", "")
        session["pending_options"] = None
        if prompt:
            await _route_user_message(context, prompt, user_name)

    # Return 200 invoke response so Teams doesn't show an error popup
    await context.send_activity(Activity(type="invokeResponse", value={"status": 200}))
    return True


# ── HTTP endpoints ────────────────────────────────────────────────────────────

# FIX 3: The /api/messages handler must use the SDK's adapter.process(request, agent)
#   pattern instead of the old adapter.process_activity(auth_header, activity, callback).
#
#   The JWT middleware (jwt_authorization_middleware) validates the Bearer token from
#   Teams and attaches a ClaimsIdentity object to the request BEFORE the handler runs.
#   The CloudAdapter.process() then reads that ClaimsIdentity from the request internally
#   via AiohttpRequestAdapter.get_claims_identity() — this is how process_activity()
#   receives a proper ClaimsIdentity instead of the raw auth header string.
#
#   The middleware also needs request.app["agent_configuration"] to be set — we set this
#   to the SERVICE_CONNECTION AgentAuthConfiguration pulled from the connection manager.

# FIX 4: Wire up jwt_authorization_middleware on the /api/messages route only.
#   The middleware reads request.app["agent_configuration"] (AgentAuthConfiguration)
#   to validate the Bearer token Teams sends. We pull that config from the
#   connection manager using get_default_connection_configuration().
#   The /api/notebook_result route is intentionally left outside the middleware
#   (it's called from your internal Fabric notebook, not from Teams).
# ── HTTP endpoints ────────────────────────────────────────────────────────────

async def messages(req: web.Request) -> web.Response:
    """Main Teams message endpoint — JWT validated, then passed to SDK adapter."""
    # Run JWT middleware manually so we don't need a subapp
    auth_config = CONNECTION_MANAGER.get_default_connection_configuration()
    from microsoft_agents.hosting.core.authorization import JwtTokenValidator
    token_validator = JwtTokenValidator(auth_config)
    auth_header = req.headers.get("Authorization", "")

    if auth_header:
        try:
            token  = auth_header.split(" ")[1]
            claims = await token_validator.validate_token(token)
            req["claims_identity"] = claims
        except (ValueError, IndexError) as e:
            print(f"  JWT validation error: {e}")
            return web.json_response({"error": str(e)}, status=401)
    else:
        if auth_config.ANONYMOUS_ALLOWED:
            req["claims_identity"] = token_validator.get_anonymous_claims()
        else:
            return web.json_response({"error": "Authorization header not found"}, status=401)

    result = await start_agent_process(req, AGENT_APP, ADAPTER)
    return result or web.Response(status=200)


async def notebook_result(req: web.Request) -> web.Response:
    """Receives result data posted directly from the Fabric notebook."""
    try:
        body      = await req.json()
        user_name = body.get("user_name", body.get("user", "")).replace(" ", "_")
        if user_name:
            session                  = get_user_session(user_name)
            session["posted_result"] = {
                "rows_data":  body.get("rows_data", []),
                "x_column":   body.get("x_column"),
                "y_column":   body.get("y_column"),
                "user_query": body.get("query", ""),
                "template":   body.get("template"),
                "report_url": body.get("report_url"),
                "job_id":     body.get("job_id")
            }
            print(f"  Notebook result received for {user_name} — job_id={body.get('job_id')} — {len(body.get('rows_data', []))} rows")
        return web.Response(status=200, text="OK")
    except Exception as e:
        print(f"  Notebook result error: {e}")
        return web.Response(status=500, text=str(e))


async def upload_page(req: web.Request) -> web.Response:
    """Serves the HTML file upload page."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AiRo Bot — File Upload</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #1e1e1e; color: #ffffff; font-family: 'Segoe UI', sans-serif;
         display: flex; align-items: center; justify-content: center; min-height: 100vh; }
  .card { background: #2d2d2d; border-radius: 12px; padding: 40px; width: 100%; max-width: 480px;
          box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
  .logo { color: #118DFF; font-size: 13px; font-weight: 700; letter-spacing: 1px;
          text-transform: uppercase; margin-bottom: 8px; }
  h1 { font-size: 24px; font-weight: 700; color: #ffffff; margin-bottom: 8px; }
  p  { font-size: 14px; color: #888888; margin-bottom: 28px; line-height: 1.5; }
  .drop-zone { border: 2px dashed #444; border-radius: 8px; padding: 40px 20px;
               text-align: center; cursor: pointer; transition: border-color 0.2s;
               margin-bottom: 20px; }
  .drop-zone:hover, .drop-zone.dragover { border-color: #118DFF; }
  .drop-zone .icon { font-size: 36px; margin-bottom: 12px; }
  .drop-zone p  { margin: 0; color: #666; font-size: 13px; }
  .drop-zone p strong { color: #118DFF; }
  input[type=file] { display: none; }
  .file-name { font-size: 13px; color: #118DFF; margin-top: 8px; min-height: 20px; }
  .btn { display: block; width: 100%; padding: 14px; background: #118DFF; color: white;
         border: none; border-radius: 8px; font-size: 15px; font-weight: 600;
         cursor: pointer; transition: background 0.2s; }
  .btn:hover { background: #0e6fd4; }
  .btn:disabled { background: #444; cursor: not-allowed; }
  .hint { font-size: 12px; color: #555; margin-top: 16px; text-align: center; }
  .progress { display: none; margin-top: 16px; }
  .progress-bar { height: 6px; background: #444; border-radius: 3px; overflow: hidden; }
  .progress-fill { height: 100%; background: #118DFF; width: 0; transition: width 0.3s; border-radius: 3px; }
  .status { font-size: 13px; color: #888; margin-top: 8px; text-align: center; }
  .success { color: #2ecc71; font-size: 15px; font-weight: 600; text-align: center; margin-top: 16px; display: none; }
  .error   { color: #e74c3c; font-size: 13px; text-align: center; margin-top: 16px; display: none; }
</style>
</head>
<body>
<div class="card">
  <div class="logo">AiRo Bot</div>
  <h1>Upload Your File</h1>
  <p>Upload an Excel or CSV file to compare it with data in the Fabric Lakehouse.</p>

  <div class="drop-zone" id="dropZone">
    <div class="icon">📂</div>
    <p>Drag and drop your file here, or <strong>click to browse</strong></p>
    <p style="margin-top:8px; font-size:11px;">Supports .xlsx, .xls, .csv &nbsp;|&nbsp; Max 15 MB</p>
  </div>
  <input type="file" id="fileInput" accept=".xlsx,.xls,.csv" onchange="validateFile(this)">
  <div class="file-name" id="fileName"></div>

  <input type="text" id="userName" placeholder="Your Teams display name (e.g. Yashovardhan_Rawat)"
         style="width:100%;padding:10px 12px;background:#1e1e1e;border:1px solid #444;border-radius:6px;
                color:#fff;font-size:13px;margin-top:16px;margin-bottom:16px;outline:none;">

  <button class="btn" id="uploadBtn" disabled onclick="uploadFile()">Upload File</button>

  <div class="progress" id="progress">
    <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    <div class="status" id="statusText">Uploading...</div>
  </div>
  <div class="success" id="successMsg"></div>
  <div class="error"   id="errorMsg"></div>

  <div class="hint">After uploading, return to Teams and ask AiRo to compare your data.</div>
</div>

<script>
const dropZone  = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const fileName  = document.getElementById('fileName');
const uploadBtn = document.getElementById('uploadBtn');
const userNameInput = document.getElementById('userName');

dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', e => {
  e.preventDefault(); dropZone.classList.remove('dragover');
  if (e.dataTransfer.files.length) { fileInput.files = e.dataTransfer.files; updateFile(); }
});
fileInput.addEventListener('change', updateFile);
userNameInput.addEventListener('input', checkReady);

function validateFile(input) {
  const f = input.files[0];
  if (!f) return;
  const ext = f.name.split('.').pop().toLowerCase();
  const allowedExts = ['xlsx', 'xls', 'csv'];
  const errorMsg = document.getElementById('errorMsg');
  // Test 3: wrong file type
  if (!allowedExts.includes(ext)) {
    errorMsg.textContent = 'Unsupported file type: .' + ext + '. Please upload .xlsx, .xls, or .csv';
    errorMsg.style.display = 'block';
    input.value = '';
    fileName.textContent = '';
    uploadBtn.disabled = true;
    return;
  }
  // Test 5: file too large
  if (f.size > 15 * 1024 * 1024) {
    errorMsg.textContent = 'File too large: ' + (f.size/1024/1024).toFixed(1) + ' MB. Maximum is 15 MB.';
    errorMsg.style.display = 'block';
    input.value = '';
    fileName.textContent = '';
    uploadBtn.disabled = true;
    return;
  }
  errorMsg.style.display = 'none';
  updateFile();
}
function updateFile() {
  const f = fileInput.files[0];
  if (f) {
    const sizeMB = (f.size/1024/1024).toFixed(2);
    const sizeColor = f.size > 3*1024*1024 ? '#f39c12' : '#118DFF';
    fileName.innerHTML = f.name + ' <span style="color:' + sizeColor + '"">(' + sizeMB + ' MB)</span>';
    if (f.size > 3*1024*1024) {
      fileName.innerHTML += ' <span style="color:#f39c12;font-size:11px"> — large file, processing may take longer</span>';
    }
    checkReady();
  }
}
function checkReady() {
  uploadBtn.disabled = !(fileInput.files.length && userNameInput.value.trim());
}

async function uploadFile() {
  const file     = fileInput.files[0];
  const userName = userNameInput.value.trim();
  if (!file || !userName) return;

  uploadBtn.disabled = true;
  document.getElementById('progress').style.display = 'block';
  document.getElementById('successMsg').style.display = 'none';
  document.getElementById('errorMsg').style.display   = 'none';

  const fill   = document.getElementById('progressFill');
  const status = document.getElementById('statusText');

  fill.style.width = '30%'; status.textContent = 'Uploading file...';

  const formData = new FormData();
  formData.append('file', file);
  formData.append('user_name', userName);

  try {
    fill.style.width = '60%'; status.textContent = 'Processing file...';
    const resp = await fetch('/api/upload', { method: 'POST', body: formData });
    const data = await resp.json();
    fill.style.width = '100%';
    if (resp.ok && data.success) {
      status.textContent = 'Done!';
      const msg = document.getElementById('successMsg');
      msg.textContent = '✅ File uploaded successfully! You can now ask AiRo to compare your data in Teams.';
      msg.style.display = 'block';
    } else {
      throw new Error(data.error || 'Upload failed');
    }
  } catch (err) {
    fill.style.width = '100%'; fill.style.background = '#e74c3c';
    status.textContent = 'Failed';
    const msg = document.getElementById('errorMsg');
    msg.textContent = 'Error: ' + err.message;
    msg.style.display = 'block';
    uploadBtn.disabled = false;
  }
}
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def api_upload(req: web.Request) -> web.Response:
    """Receives uploaded file from the HTML upload page."""
    try:
        reader    = await req.multipart()
        file_bytes = None
        filename   = None
        user_name  = None

        async for part in reader:
            if part.name == "file":
                filename   = part.filename or "upload.csv"
                file_bytes = await part.read()
            elif part.name == "user_name":
                raw        = await part.read()
                user_name  = raw.decode("utf-8").strip().replace(" ", "_").lower()

        if not file_bytes or not filename:
            return web.json_response({"success": False, "error": "No file received."}, status=400)
        if not user_name:
            return web.json_response({"success": False, "error": "User name is required."}, status=400)

        # File size check at HTTP layer — 30MB raw ceiling (b64 check in process_uploaded_file
        # will catch anything over ~15MB raw with a clearer message before this point)
        file_size_mb = len(file_bytes) / (1024 * 1024)
        if len(file_bytes) > 30 * 1024 * 1024:
            return web.json_response({
                "success": False,
                "error": f"File too large ({file_size_mb:.1f} MB). Maximum allowed size is 15 MB."
            }, status=400)

        # Test 3: File type check at HTTP layer (double-check before pandas)
        ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
        if ext not in ("xlsx", "xls", "csv"):
            return web.json_response({
                "success": False,
                "error": f"Unsupported file type: .{ext}. Please upload .xlsx, .xls, or .csv only."
            }, status=400)

        # Test 4: Check if same file is already pending for this user
        norm_user = user_name.lower()
        base_fn   = filename.rsplit(".", 1)[0]
        safe_fn   = re.sub(r"[^a-zA-Z0-9_]", "_", base_fn.strip().lower())
        safe_fn   = re.sub(r"_+", "_", safe_fn).strip("_") or "file"
        tname     = f"upload_{re.sub(chr(91) + r'^a-zA-Z0-9_' + chr(93), '_', norm_user)}_{safe_fn}"
        if norm_user in _upload_registry:
            existing = next((e for e in _upload_registry[norm_user]
                             if e.get("table_name") == tname and e.get("status") == "pending"), None)
            if existing:
                return web.json_response({
                    "success": False,
                    "error": f"'{filename}' is already being processed. Please wait before uploading again."
                }, status=409)

        print(f"  [Upload] Received: {filename} ({len(file_bytes)/1024:.1f} KB) from {user_name}")

        success, result = process_uploaded_file(file_bytes, filename, user_name)

        if success:
            # Upload is now synchronous (Files API + Load Table API).
            # By the time we get here the Delta table already exists and the
            # registry entry is marked "ready" — no notebook callback needed.
            norm_user = user_name.lower()
            entry = next(
                (e for entries in [_upload_registry.get(norm_user, [])]
                 for e in entries if e.get("job_id") == result),
                {}
            )
            return web.json_response({
                "success":   True,
                "job_id":    result,
                "message":   "File uploaded and table created. Return to Teams to query your data.",
                "row_count": entry.get("row_count", 0),
            })
        else:
            return web.json_response({"success": False, "error": result}, status=500)

    except Exception as e:
        print(f"  [Upload] api_upload error: {e}")
        return web.json_response({"success": False, "error": str(e)}, status=500)


async def upload_result(req: web.Request) -> web.Response:
    """
    Receives the result from AiRo_Upload_Notebook after Delta table creation.
    Updates the upload registry and notifies the user in Teams.
    """
    try:
        body       = await req.json()
        job_id     = body.get("job_id", "")
        user_name  = body.get("user_name", "").replace(" ", "_")
        table_name = body.get("table_name", "")
        row_count  = body.get("row_count", 0)
        columns    = body.get("columns", [])
        status     = body.get("status", "unknown")
        error      = body.get("error", "")

        print(f"  [UploadResult] job_id={job_id} user={user_name} status={status} rows={row_count}")

        # Safety check — only accept upload_ table names
        if table_name and not table_name.startswith("upload_"):
            print(f"  [SAFETY] Blocked upload_result for non-upload table: {table_name}")
            return web.Response(status=400, text="Invalid table name")

        # Find matching pending entry in list registry
        registry_entry = None
        registry_user  = None
        for uname, entries in _upload_registry.items():
            if isinstance(entries, list):
                for entry in entries:
                    if entry.get("job_id") == job_id:
                        registry_entry = entry
                        registry_user  = uname
                        break
            if registry_entry:
                break

        if not registry_entry:
            print(f"  [UploadResult] No matching job_id in registry: {job_id} — may be delete job, ignoring.")
            return web.Response(status=200, text="OK")

        if status in ("success", "deleted"):
            if status == "success":
                registry_entry["status"]    = "ready"
                registry_entry["row_count"] = row_count
                registry_entry["columns"]   = columns
            import asyncio
            await asyncio.sleep(3)
            _SCHEMA_CACHE["schema"]     = None
            _SCHEMA_CACHE["fetched_at"] = 0
            print(f"  [UploadResult] Schema cache invalidated — {table_name} ({status}).")
            print(f"  [UploadResult] Upload complete for {user_name}: {row_count:,} rows in {table_name}")

        else:
            registry_entry["status"] = "failed"
            registry_entry["error"]  = error
            print(f"  [UploadResult] Upload FAILED for {user_name}: {error}")

        return web.Response(status=200, text="OK")

    except Exception as e:
        print(f"  [UploadResult] Error: {e}")
        return web.Response(status=500, text=str(e))


async def chart_page(req: web.Request) -> web.Response:
    """
    Serves an interactive Chart.js chart inside a Teams Task Module iframe.
    Route: GET /chart/{chart_id}
    Two tabs: Chart (interactive) and Data Table.
    """
    chart_id = req.match_info.get("chart_id", "")
    data     = _chart_store.get(chart_id)
    if not data:
        return web.Response(
            text="<html><body style='font-family:sans-serif;padding:32px'>"
                 "<h3>Chart not found or expired.</h3>"
                 "<p>Charts expire after 30 minutes. Run the query again to get a fresh chart.</p>"
                 "</body></html>",
            content_type="text/html"
        )

    rows_data  = data["rows_data"]
    x_column   = data["x_column"] or ""
    y_column   = data["y_column"] or ""
    title      = data["title"]
    query      = data["query"]
    try:
        return _render_chart_page(rows_data, x_column, y_column, title, query)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print("  [ChartPage] ERROR building interactive chart:\n" + tb)
        return web.Response(
            status=200,
            content_type="text/html",
            text="<html><body style='font-family:sans-serif;padding:32px'>"
                 "<h3>Couldn't render the interactive chart.</h3>"
                 "<p>The data table on the card still has your full results. "
                 "This view had trouble plotting this particular result shape.</p>"
                 f"<pre style='color:#b91c1c;font-size:11px;white-space:pre-wrap'>{str(e)}</pre>"
                 "</body></html>"
        )


def _render_chart_page(rows_data, x_column, y_column, title, query):
    all_cols   = list(rows_data[0].keys()) if rows_data else []
    chart_type = select_chart_type(rows_data, x_column, y_column, all_cols)

    # A single-value result (one row, one metric) has nothing to plot across an
    # axis — rendering it as a bar produces a meaningless one-bar chart with a
    # duplicated axis. Detect it and render a clean big-number KPI instead.
    is_kpi = (chart_type == "kpi") or (len(rows_data) == 1 and bool(y_column))

    # Build labels and values (cap at 30 for readability)
    chart_rows = rows_data[:30]
    labels     = [str(r.get(x_column, "")) for r in chart_rows]
    try:
        values = [float(r.get(y_column, 0) or 0) for r in chart_rows]
    except (ValueError, TypeError):
        values = [0.0] * len(labels)

    # Humanise time labels (non-fatal — fall back to raw labels on any error)
    if x_column and _col_is_time(x_column) and not is_kpi:
        try:
            year_col = next((c for c in all_cols
                             if c != x_column and "year" in c.lower()), None)
            labels   = _humanise_time_labels(labels, x_column,
                                             all_rows=chart_rows, year_col=year_col)
        except Exception as _e:
            print(f"  [ChartPage] time-label humanise skipped ({_e})")

    # Chart.js type + orientation
    cjs_map       = {"line": "line", "horizontal_bar": "bar",
                     "pie": "pie", "kpi": "bar"}
    cjs_type      = cjs_map.get(chart_type, "bar")
    is_horizontal = "true" if chart_type == "horizontal_bar" else "false"
    is_pie        = "true" if chart_type == "pie" else "false"

    labels_json  = json.dumps(labels)
    values_json  = json.dumps(values)
    y_label      = y_column.replace("_", " ").title()
    pie_colors   = json.dumps(["#118DFF","#00C4B4","#F4C430","#E05C5C","#9B59B6",
                                "#27AE60","#E67E22","#1ABC9C","#E91E8C","#00B0FF"])

    # KPI display: the single value + its label, formatted exactly with separators
    kpi_value    = ""
    if is_kpi:
        v = values[0] if values else 0
        kpi_value = f"{int(v):,}" if float(v) == int(v) else f"{v:,.2f}"
    kpi_label    = y_label or "Value"
    is_kpi_js    = "true" if is_kpi else "false"

    # Full data table (up to 50 rows, up to 8 cols)
    table_cols   = all_cols[:8]
    table_rows   = [[str(r.get(c, "")) for c in table_cols] for r in rows_data[:50]]
    headers_json = json.dumps([c.replace("_", " ").title() for c in table_cols])
    table_json   = json.dumps(table_rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
        background:#f8fafc;color:#1e293b;padding:14px;font-size:13px}}
  .hdr h2{{font-size:14px;font-weight:700;color:#1e3a5f}}
  .hdr p{{font-size:11px;color:#64748b;margin-top:3px}}
  .tabs{{display:flex;gap:6px;margin:10px 0}}
  .tab{{padding:5px 14px;border-radius:6px;border:1px solid #e2e8f0;
        cursor:pointer;font-size:12px;background:#fff;color:#64748b}}
  .tab.on{{background:#1e3a5f;color:#fff;border-color:#1e3a5f}}
  .panel{{display:none}}.panel.on{{display:block}}
  .wrap{{background:#fff;border-radius:10px;padding:14px;
         box-shadow:0 1px 4px rgba(0,0,0,.08);height:360px;
         display:flex;align-items:center;justify-content:center}}
  canvas{{max-height:330px}}
  table{{width:100%;border-collapse:collapse;font-size:11px;
         background:#fff;border-radius:10px;overflow:hidden;
         box-shadow:0 1px 4px rgba(0,0,0,.08)}}
  th{{background:#1e3a5f;color:#fff;padding:7px 10px;text-align:left;font-weight:600}}
  td{{padding:5px 10px;border-bottom:1px solid #f1f5f9}}
  tr:last-child td{{border-bottom:none}}
  tr:nth-child(even) td{{background:#f8fafc}}
  .num{{color:#118dff;font-weight:600;text-align:right}}
  .kpi-box{{background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.08);
            height:360px;display:flex;flex-direction:column;align-items:center;
            justify-content:center;gap:10px}}
  .kpi-num{{font-size:64px;font-weight:800;color:#118DFF;line-height:1}}
  .kpi-lbl{{font-size:15px;color:#64748b;font-weight:600}}
</style>
</head>
<body>
<div class="hdr"><h2>{title}</h2><p>{query}</p></div>
<div class="tabs">
  <button class="tab on" onclick="show('chart',this)">📊 Chart</button>
  <button class="tab"    onclick="show('table',this)">📋 Data</button>
</div>
<div id="p-chart" class="panel on">
  <div id="kpi-box" class="kpi-box" style="display:none">
    <div class="kpi-num">{kpi_value}</div>
    <div class="kpi-lbl">{kpi_label}</div>
  </div>
  <div id="chart-wrap" class="wrap"><canvas id="c"></canvas></div>
</div>
<div id="p-table" class="panel">
  <table id="t"></table>
</div>
<script>
const L={labels_json},V={values_json},T="{cjs_type}",
      IH={is_horizontal},IP={is_pie},IS_KPI={is_kpi_js},
      PC={pie_colors},
      TH={headers_json},TR={table_json},YL="{y_label}";

// Exact-number formatter — thousands separators, no K/M abbreviation.
function fmtNum(v){{
  if(typeof v!=="number"||isNaN(v))return v;
  return Number.isInteger(v)?v.toLocaleString():v.toLocaleString(undefined,
    {{minimumFractionDigits:2,maximumFractionDigits:2}});
}}

function show(name,btn){{
  document.querySelectorAll(".panel").forEach(p=>p.classList.remove("on"));
  document.querySelectorAll(".tab").forEach(b=>b.classList.remove("on"));
  document.getElementById("p-"+name).classList.add("on");
  btn.classList.add("on");
}}

if(IS_KPI){{
  // Single-value result — show the big number, hide the chart canvas entirely.
  document.getElementById("kpi-box").style.display="flex";
  document.getElementById("chart-wrap").style.display="none";
}} else {{
  const ds=IP
    ?{{data:V,backgroundColor:PC,borderWidth:2,borderColor:"#fff"}}
    :{{label:YL,data:V,
       backgroundColor:IH?V.map((_,i)=>PC[i%PC.length]):"rgba(17,141,255,0.8)",
       borderColor:"#118DFF",borderWidth:IH?0:2,
       pointBackgroundColor:"#118DFF",
       fill:T==="line",tension:0.35}};

  new Chart(document.getElementById("c").getContext("2d"),{{
    type:T,data:{{labels:L,datasets:[ds]}},
    options:{{
      indexAxis:IH?"y":"x",
      responsive:true,maintainAspectRatio:false,
      plugins:{{
        legend:{{display:IP}},
        tooltip:{{callbacks:{{label:ctx=>{{
          const v=IH?ctx.parsed.x:(ctx.parsed.y??ctx.parsed);
          return typeof v==="number"?" "+fmtNum(v):" "+v;
        }}}}}}
      }},
      scales:IP?{{}}:{{
        x:{{grid:{{color:"#f1f5f9"}},ticks:{{font:{{size:10}},
          callback:function(val){{
            // category axis: show the label; value axis: format the number
            const lab=this.getLabelForValue(val);
            return IH?fmtNum(Number(lab)):lab;
          }}
        }}}},
        y:{{grid:{{color:"#f1f5f9"}},ticks:{{font:{{size:10}},
          callback:function(val){{return IH?this.getLabelForValue(val):fmtNum(val);}}
        }}}}
      }}
    }}
  }});
}}

const tbl=document.getElementById("t");
tbl.innerHTML="<thead><tr>"+TH.map(h=>`<th>${{h}}</th>`).join("")+"</tr></thead>"
  +"<tbody>"+TR.map(row=>"<tr>"+row.map(cell=>{{
    const n=cell!==""&&!isNaN(parseFloat(cell));
    return`<td class="${{n?"num":""}}">${{cell}}</td>`;
  }}).join("")+"</tr>").join("")+"</tbody>";
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


app = web.Application()
app.router.add_post("/api/messages",        messages)
app.router.add_post("/api/notebook_result", notebook_result)
app.router.add_post("/api/upload_result",   upload_result)
app.router.add_get( "/upload",              upload_page)
app.router.add_post("/api/upload",          api_upload)
app.router.add_get( "/chart/{chart_id}",    chart_page)

async def on_startup(app_instance):
    """Called by aiohttp on startup — starts background tasks."""
    asyncio.create_task(upload_ttl_cleanup_task())
    print("  [TTL] Background TTL cleanup task registered.")


app.on_startup.append(on_startup)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3978))
    print(f"AiRo Bot running on port {port}")
    # Clean up previous session uploads on startup
    startup_cleanup()
    web.run_app(app, host="0.0.0.0", port=port)