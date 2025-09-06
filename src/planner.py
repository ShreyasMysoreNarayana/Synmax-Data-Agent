from typing import Dict, Any, List, Optional
import re, difflib

def plan_from_nl(question: str, available_cols: List[str]) -> Dict[str, Any]:
    q = question.strip().lower()

    n = _extract_int(q)
    year = _extract_year(q)
    filters = _extract_filters(q, available_cols)

    # Meta Data schema
    if any(p in q for p in ['how many columns','number of columns','columns count','col count','dataset shape','shape','dimensions']):
        return {'type':'meta:shape'}
    if any(p in q for p in ['list columns','what are the columns','show columns','column names','headers','features','schema']):
        return {'type':'meta:columns'}
    if any(p in q for p in ['dtypes','data types','types of columns','show dtypes']):
        return {'type':'meta:dtypes'}
    if any(p in q for p in ['describe','summary stats','summary statistics','stats']):
        return {'type':'meta:describe'}
    if 'head' in q or 'first rows' in q or 'show first' in q:
        return {'type':'meta:head','n': n or 5}
    if 'tail' in q or 'last rows' in q or 'show last' in q:
        return {'type':'meta:tail','n': n or 5}
    if any(p in q for p in ['missing values','nulls','nans','na values','na summary']):
        return {'type':'meta:missing'}
    if any(p in q for p in ['duplicate rows','duplicates']):
        return {'type':'meta:duplicates'}

    # Unique value counts 
    if 'unique' in q or 'distinct' in q:
        col = _resolve_col_from_text(q, available_cols)
        if col:
            return {'type':'unique_count','col': col}
    if any(p in q for p in ['value counts','frequency','distribution','breakdown']):
        col = _resolve_col_from_text(q, available_cols)
        if col:
            return {'type':'value_counts','col': col, 'n': n or 10}

    # Count rows
    if any(w in q for w in ['how many rows','count rows','row count','number of rows','rows']):
        # e.g., "count rows by year"
        grp = _resolve_col_after_by_or_per(q, available_cols)
        if grp:
            return {'type':'group_count','group_by':[grp]}
        # or "in 2024"
        f = filters.copy()
        if year and 'year' in available_cols:
            f['year'] = year
        return {'type':'aggregate','group_by':[],'ops':{'*':'count'},'filters': f}

    # Top N rows by column 
    # e.g., "top 10 rows by scheduled_quantity where state_abb = tx"
    m = re.search(r'(?:top|largest)\s+(\d+)\s+rows?\s+by\s+([a-z0-9_ ]+)', q)
    if m:
        top_n = int(m.group(1))
        by_col = _resolve_col_from_text(m.group(2), available_cols)
        if by_col:
            return {'type': 'sort_top', 'by': [by_col], 'ascending': False, 'top_n': top_n, 'filters': filters}

    # Sum / Average / etc 
    if any(w in q for w in ['sum','total']):
        target = _first_present(available_cols, ['scheduled_quantity','shipments','volume','amount','value'])
        gb = _resolve_col_after_by_or_per(q, available_cols)
        gb_list = [gb] if gb else (['year'] if 'year' in available_cols and ('by year' in q or year) else [])
        if year and 'year' in available_cols:
            filters['year'] = year
        return {'type':'aggregate','group_by': gb_list,'ops':{target:'sum'} if target else {},'filters': filters}

    if any(w in q for w in ['average','mean','avg']):
        target = _resolve_col_from_text(q, available_cols) or _first_present(available_cols, ['scheduled_quantity','shipments','volume','delay_hours','amount','value'])
        gb = _resolve_col_after_by_or_per(q, available_cols)
        gb_list = [gb] if gb else (['year'] if 'year' in available_cols and ('by year' in q or year) else [])
        if year and 'year' in available_cols:
            filters['year'] = year
        return {'type':'aggregate','group_by': gb_list,'ops':{target:'mean'} if target else {},'filters': filters}

    # Correlation / outliers 
    if 'correlation' in q or 'correlate' in q or 'relationship' in q or 'corr' in q:
        cols = [c for c in ['scheduled_quantity','shipments','volume','delay_hours','rec_del_sign'] if c in available_cols]
        return {'type':'correlation','cols': cols}
    if 'outlier' in q or 'anomaly' in q or 'weird' in q:
        col = _resolve_col_from_text(q, available_cols) or _first_present(available_cols, ['scheduled_quantity','delay_hours','volume','shipments'])
        return {'type':'anomaly:zscore','col': col or (available_cols[0] if available_cols else None), 'threshold': 3.0}

    # Trend phrasing 
    if 'trend' in q or 'over time' in q or 'by year' in q:
        target = _resolve_col_from_text(q, available_cols) or _first_present(available_cols, ['scheduled_quantity','shipments','volume','delay_hours'])
        gb_list = ['year'] if 'year' in available_cols else []
        return {'type':'aggregate','group_by': gb_list,'ops':{target:'mean'} if target else {},'filters': {}}

    # default fallback
    return {'type':'aggregate','group_by':[],'ops':{'*':'count'},'filters':{}}



def _first_present(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    # fuzzy backup
    if candidates:
        match = difflib.get_close_matches(candidates[0], cols, n=1, cutoff=0.7)
        if match:
            return match[0]
    return None

def _extract_year(text: str) -> Optional[int]:
    m = re.search(r'\b(20\d{2})\b', text)
    return int(m.group(1)) if m else None

def _extract_int(text: str) -> Optional[int]:
    m = re.search(r'\b(\d{1,4})\b', text)
    return int(m.group(1)) if m else None

def _normalize(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())

def _resolve_col_from_text(qpiece: str, cols: List[str]) -> Optional[str]:
    text = qpiece.lower()
    # exact
    for c in cols:
        if c.lower() in text:
            return c
    # fuzzy
    match = difflib.get_close_matches(text.strip(), cols, n=1, cutoff=0.8)
    return match[0] if match else None

def _resolve_col_after_by_or_per(q: str, cols: List[str]) -> Optional[str]:
    m = re.search(r'(?: by | per )([a-z0-9_ ]+)$', q)
    if not m:  # handle "by year" anywhere
        m = re.search(r'by ([a-z0-9_ ]+)', q)
    if not m:
        return None
    return _resolve_col_from_text(m.group(1), cols)

def _extract_filters(q: str, cols: List[str]) -> Dict[str,Any]:
    f: Dict[str,Any] = {}
    # equality: where <col> = <val>
    m = re.search(r'where\s+([a-z0-9_ ]+)\s*=\s*([a-z0-9_/\-]+)', q)
    if m:
        col = _resolve_col_from_text(m.group(1), cols)
        val = m.group(2).strip()
        if col:
            f[col] = _coerce_literal(val)
    # contains where <col> contains <text>
    m = re.search(r'where\s+([a-z0-9_ ]+)\s+contains\s+([a-z0-9_/\-]+)', q)
    if m:
        col = _resolve_col_from_text(m.group(1), cols)
        val = m.group(2).strip()
        if col:
            f[col] = {'contains': val}
    return f

def _coerce_literal(s: str):
    try:
        return int(s)
    except:
        try:
            return float(s)
        except:
            return s.upper() if len(s) <= 5 else s
