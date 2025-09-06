import pandas as pd

def infer_schema(df: pd.DataFrame):
    schema = {}
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            schema[col] = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            schema[col] = "datetime"
        else:
            # Try datetime parse fallback
            try:
                pd.to_datetime(df[col])
                schema[col] = "datetime"
            except Exception:
                schema[col] = "categorical"
    return schema

def clean_data(df: pd.DataFrame, schema: dict):
    df = df.copy()
    for col, typ in schema.items():
        if typ == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif typ == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
