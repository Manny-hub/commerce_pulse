from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


def coalesce(*vals):
    """Return first non-null/meaningful value."""
    for v in vals:
        if v is None:
            continue
        # treat empty strings as null
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def deep_get(d: Any, path: str, default=None):
    """Safely get nested dict keys: deep_get(doc, 'shipping.address.city')."""
    cur = d
    for part in path.split("."):
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(part, default)
        else:
            return default
    return cur


def parse_ts(value: Any) -> pd.Timestamp:
    """
    Parse mixed timestamps:
    - ISO with Z: 2023-02-19T20:43:27Z
    - ISO without Z: 2023-07-23T12:07:13
    - Slash format: 2023/04/18 17:12:36
    - Space format: 2023-06-14 04:03
    - Unix epoch seconds: 1703109546
    """
    if value is None:
        return pd.NaT

    # epoch seconds
    if isinstance(value, (int, float)) and value > 10_000_000:
        return pd.to_datetime(int(value), unit="s", utc=True)

    s = str(value).strip()

    # normalize Z
    # pandas handles "Z" with utc=True; it also handles many formats automatically
    try:
        # Try with utc parsing first
        return pd.to_datetime(s, utc=True, errors="raise")
    except Exception:
        pass

    # Try common fallback patterns
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            # assume UTC if no tz info
            return ts.tz_localize("UTC")
        except Exception:
            continue

    # last resort: coerce
    return pd.to_datetime(s, utc=True, errors="coerce")


def normalize_currency(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    # basic sanity: allow NGN/USD etc
    if re.fullmatch(r"[A-Z]{3}", s):
        return s
    return s or None


def normalize_status(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    # common variants: SUCCESS/FAILED, sometimes "state"/"status"/"payment_status"
    return s or None