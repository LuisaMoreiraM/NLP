import os
import numpy as np

def data_dirs(from_file: str, data_raw: str | None = None, data_processed: str | None = None):
    """Devuelve (raw_dir, processed_dir) basándose en la ruta del archivo que llama."""
    base = os.path.abspath(os.path.join(os.path.dirname(from_file), "..", ".."))
    raw = data_raw or os.path.join(base, "data", "raw")
    processed = data_processed or os.path.join(base, "data", "processed")
    return raw, processed

def drop_duplicates_sort(df, subset, sort_by=None, ascending=True):
    """Drop duplicates y sort opcional; devuelve una copia."""
    out = df.drop_duplicates(subset=subset, keep="first")
    if sort_by is not None:
        out = out.sort_values(by=sort_by, ascending=ascending)
    return out.copy()

def clean_cas_ec(df, cas_col="CAS Number", ec_col="EC Number"):
    """Normaliza -, '', ' ' -> NaN"""
    for col in (cas_col, ec_col):
        if col in df.columns:
            df[col] = df[col].replace(['-', '', ' '], np.nan)
    return df

def not_both_null(df, a: str, b: str):
    """Filtra filas donde no sean ambos nulos a la vez."""
    return df[~(df[a].isna() & df[b].isna())].copy()
