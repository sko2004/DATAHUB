"""
Module 5: Metadata Extraction & Indexing Service
Core service that parses CSV, JSON, and Parquet files to extract statistical metadata.
"""
import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats


def compute_sha256(file_path: str) -> str:
    """Compute SHA-256 hash of a file for content-addressable storage."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _compute_column_stats(series: pd.Series) -> Dict[str, Any]:
    """Compute per-column statistics for numeric and categorical data."""
    stats: Dict[str, Any] = {
        "dtype": str(series.dtype),
        "null_count": int(series.isna().sum()),
        "null_pct": round(float(series.isna().mean() * 100), 2),
        "unique_count": int(series.nunique()),
    }

    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        if len(clean) > 0:
            stats.update({
                "mean": round(float(clean.mean()), 6),
                "std": round(float(clean.std()), 6),
                "min": round(float(clean.min()), 6),
                "max": round(float(clean.max()), 6),
                "median": round(float(clean.median()), 6),
                "q1": round(float(clean.quantile(0.25)), 6),
                "q3": round(float(clean.quantile(0.75)), 6),
                "skewness": round(float(scipy_stats.skew(clean)), 4),
                "kurtosis": round(float(scipy_stats.kurtosis(clean)), 4),
            })
    elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        top_vals = series.value_counts().head(5).to_dict()
        stats["top_values"] = {str(k): int(v) for k, v in top_vals.items()}

    return stats


def _compute_histogram(series: pd.Series, bins: int = 10) -> Optional[Dict]:
    """Compute histogram bins for numeric columns."""
    if not pd.api.types.is_numeric_dtype(series):
        return None
    clean = series.dropna()
    if len(clean) < 2:
        return None
    counts, edges = np.histogram(clean, bins=bins)
    return {
        "counts": counts.tolist(),
        "bin_edges": [round(e, 4) for e in edges.tolist()],
    }


def extract_metadata_from_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Core extraction logic: statistics + distributions from a DataFrame."""
    column_schema: Dict[str, str] = {col: str(df[col].dtype) for col in df.columns}
    statistics: Dict[str, Any] = {}
    distributions: Dict[str, Any] = {}

    for col in df.columns:
        statistics[col] = _compute_column_stats(df[col])
        hist = _compute_histogram(df[col])
        if hist:
            distributions[col] = {"histogram": hist}

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns_schema": column_schema,
        "statistics": statistics,
        "distributions": distributions,
    }


def extract_custom_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Try to auto-detect model metrics columns (accuracy, f1, loss, etc.)."""
    metric_keywords = ["accuracy", "f1", "loss", "precision", "recall", "auc", "mse", "mae", "rmse", "score"]
    found: Dict[str, Any] = {}
    for col in df.columns:
        if any(kw in col.lower() for kw in metric_keywords):
            clean = df[col].dropna()
            if pd.api.types.is_numeric_dtype(df[col]) and len(clean) > 0:
                found[col] = {
                    "latest": round(float(clean.iloc[-1]), 6),
                    "max": round(float(clean.max()), 6),
                    "mean": round(float(clean.mean()), 6),
                }
    return found


def load_file_to_dataframe(file_path: str, file_type: str) -> pd.DataFrame:
    """Load CSV, JSON, or Parquet file into a pandas DataFrame."""
    file_type = file_type.lower().strip(".")
    if file_type == "csv":
        return pd.read_csv(file_path)
    elif file_type == "json":
        return pd.read_json(file_path)
    elif file_type in ("parquet", "pq"):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def analyze_file(file_path: str, file_type: str = None) -> Dict[str, Any]:
    """
    Main entry point: analyze a data file and return full metadata dictionary.
    This is automatically triggered on every commit (Module 5 core deliverable).
    Pass `file_type` explicitly when the stored path has no extension (CAS blobs).
    """
    path = Path(file_path)
    # Prefer the explicitly-passed type; fall back to the path's own suffix
    suffix = (file_type or path.suffix).lower().lstrip(".")
    if suffix not in ("csv", "json", "parquet", "pq"):
        raise ValueError(f"Unsupported file extension: {suffix}")

    df = load_file_to_dataframe(str(path), suffix)
    meta = extract_metadata_from_dataframe(df)
    meta["custom_metrics"] = extract_custom_metrics(df)
    meta["file_name"] = path.name
    meta["file_type"] = suffix

    return meta
