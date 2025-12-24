import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_mysql_type_category(mysql_type):
    """Determine the category of a MySQL data type."""
    mysql_type = (mysql_type or "").lower()
    
    if "tinyint(1)" in mysql_type:
        return "boolean"
    elif "bigint" in mysql_type:
        return "bigint"
    elif "tinyint" in mysql_type:
        return "tinyint"
    elif "smallint" in mysql_type or "mediumint" in mysql_type:
        return "smallint"
    elif "int" in mysql_type:
        return "int"
    elif "float" in mysql_type or "double" in mysql_type or "decimal" in mysql_type or "numeric" in mysql_type:
        return "float"
    elif "datetime" in mysql_type or "timestamp" in mysql_type:
        return "datetime"
    elif "date" in mysql_type:
        return "date"
    elif "time" in mysql_type:
        return "time"
    elif "year" in mysql_type:
        return "year"
    elif "blob" in mysql_type or "binary" in mysql_type or "varbinary" in mysql_type:
        return "binary"
    elif "json" in mysql_type:
        return "json"
    elif "enum" in mysql_type or "set" in mysql_type:
        return "enum"
    elif "varchar" in mysql_type or "text" in mysql_type or "char" in mysql_type:
        return "string"
    else:
        return "unknown"


def transform_data_types(data, column_types):
    """Transform MySQL column types to PostgreSQL-compatible formats on a DataFrame."""
    for column, mysql_type in column_types.items():
        if data[column].isnull().all():
            continue
        
        category = get_mysql_type_category(mysql_type)
        
        if category == "boolean":
            data[column] = data[column].apply(lambda x: bool(x) if pd.notnull(x) else None)
        elif category == "bigint":
            data[column] = data[column].astype("Int64").where(pd.notna(data[column]), None)
        elif category == "int":
            max_value = data[column].max(skipna=True)
            if pd.notna(max_value) and max_value > 2_147_483_647:
                data[column] = data[column].astype("Int64")
            else:
                data[column] = data[column].astype("Int32").where(pd.notna(data[column]), None)
        elif category in ["tinyint", "smallint"]:
            data[column] = data[column].astype("Int32").where(pd.notna(data[column]), None)
        elif category == "float":
            data[column] = data[column].astype(float).where(pd.notna(data[column]), None)
        elif category == "datetime":
            data[column] = pd.to_datetime(data[column], errors="coerce")
            data[column] = data[column].apply(lambda x: None if pd.isna(x) else (x if x.year >= 1000 else pd.Timestamp("1000-01-01 00:00:00")))
        elif category in ["string", "enum"]:
            data[column] = data[column].apply(lambda x: str(x) if pd.notnull(x) else None)
        # binary, json, date, time, year types generally work without transformation
    
    return data


def map_mysql_to_postgres_type(mysql_type):
    """Map MySQL data types to PostgreSQL data types."""
    mysql_type_lower = (mysql_type or "").lower()
    category = get_mysql_type_category(mysql_type)

    # Direct mapping for common categories
    direct_map = {
        "boolean": "BOOLEAN",
        "tinyint": "SMALLINT",
        "smallint": "SMALLINT",
        "bigint": "BIGINT",
        "int": "INTEGER",
        "year": "INTEGER",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",
        "binary": "BYTEA",
        "json": "JSONB",
        "enum": "VARCHAR(255)",
    }

    if category in direct_map:
        return direct_map[category]

    if category == "float":
        if "decimal" in mysql_type_lower or "numeric" in mysql_type_lower:
            m = re.search(r"\((\d+),(\d+)\)", mysql_type)
            if m:
                return f"NUMERIC({m.group(1)},{m.group(2)})"
            return "NUMERIC"
        if "double" in mysql_type_lower:
            return "DOUBLE PRECISION"
        return "REAL"

    if category == "string":
        if "char" in mysql_type_lower and "varchar" not in mysql_type_lower:
            m = re.search(r"\((\d+)\)", mysql_type)
            if m:
                return f"CHAR({m.group(1)})"
            return "CHAR(255)"
        if "varchar" in mysql_type_lower:
            m = re.search(r"\((\d+)\)", mysql_type)
            if m:
                return f"VARCHAR({m.group(1)})"
            return "VARCHAR(255)"
        return "TEXT"

    logger.warning(f"Unknown MySQL type: {mysql_type}, using TEXT")
    return "TEXT"
