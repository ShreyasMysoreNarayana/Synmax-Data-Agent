import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from sklearn.ensemble import IsolationForest

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any] | None) -> pd.DataFrame:
    data = df.copy()
    if not filters:
        return data
    for k, v in filters.items():
        if k not in data.columns:
            continue
        s = data[k]
        if isinstance(v, dict):
            if 'between' in v:
                lo, hi = v['between']
                data = data[(s >= lo) & (s <= hi)]
            if 'in' in v:
                data = data[s.isin(v['in'])]
            if 'contains' in v:
                data = data[s.astype(str).str.contains(str(v['contains']), case=False, na=False)]
            if 'startswith' in v:
                data = data[s.astype(str).str.startswith(str(v['startswith']), na=False)]
            if 'endswith' in v:
                data = data[s.astype(str).str.endswith(str(v['endswith']), na=False)]
            if 'gt' in v:   data = data[s > v['gt']]
            if 'gte' in v:  data = data[s >= v['gte']]
            if 'lt' in v:   data = data[s < v['lt']]
            if 'lte' in v:  data = data[s <= v['lte']]
        else:
            data = data[s == v]
    return data

def aggregate(df, group_by=None, ops=None, filters=None):
    data = apply_filters(df, filters)
    if not ops:
        return data
    agg_map = {c:o for c,o in (ops or {}).items() if c != '*' and o in ['count','sum','mean','min','max','median','std']}
    if group_by:
        out = data.groupby(group_by, dropna=False).agg(agg_map).reset_index()
        if (ops or {}).get('*') == 'count':
            counts = data.groupby(group_by, dropna=False).size().reset_index(name='row_count')
            out = counts.merge(out, on=group_by, how='left')
    else:
        out = data.agg(agg_map).to_frame().T
        if (ops or {}).get('*') == 'count':
            out.insert(0, 'row_count', [len(data)])
    return out

def correlations(df, cols=None, method='pearson'):
    num = df.select_dtypes(include=['number'])
    if cols:
        cols = [c for c in cols if c in num.columns]
        if cols:
            num = num[cols]
    return num.corr(method=method)

def zscore_outliers(df, col: str, threshold: float=3.0):
    s = df[col].astype(float)
    z = (s - s.mean()) / (s.std(ddof=0) + 1e-9)
    return df[np.abs(z) >= threshold]

def iforest_outliers(df, cols: List[str], contamination: float=0.01, random_state: int=42):
    use = [c for c in cols if c in df.columns]
    X = df[use].select_dtypes(include=['number']).dropna()
    if X.empty:
        return df.iloc[0:0]
    clf = IsolationForest(contamination=contamination, random_state=random_state)
    pred = clf.fit_predict(X)
    mask = (pred == -1)
    return df.loc[X.index[mask]]

def sort_top(df, by: List[str], top_n: int=10, ascending: bool=False, filters=None):
    data = apply_filters(df, filters)
    by = [c for c in by if c in data.columns]
    if not by:
        return data.head(top_n)
    return data.sort_values(by=by, ascending=ascending).head(int(top_n))

# meta helpers 
def meta_shape(df): return pd.DataFrame({'rows':[len(df)], 'columns':[df.shape[1]]})
def meta_columns(df): return pd.DataFrame({'column': df.columns, 'dtype': df.dtypes.astype(str).values})
def meta_dtypes(df): return meta_columns(df)
def meta_head(df, n=5): return df.head(int(n))
def meta_tail(df, n=5): return df.tail(int(n))
def meta_describe(df):
    num = df.select_dtypes(include=['number'])
    if num.empty: return pd.DataFrame({'note':['No numeric columns to describe.']})
    return num.describe().T.reset_index().rename(columns={'index':'column'})
def unique_count(df, col): return pd.DataFrame({'column':[col], 'unique_count':[df[col].nunique(dropna=True)]})
def value_counts(df, col, n=10):
    vc = df[col].value_counts(dropna=False).head(int(n)).reset_index()
    vc.columns = [col, 'count']
    return vc
def missing_summary(df):
    miss = df.isna().sum().rename('missing')
    pct = (df.isna().mean()*100).rename('missing_pct')
    return pd.concat([miss, pct], axis=1).reset_index().rename(columns={'index':'column'}).sort_values('missing', ascending=False, ignore_index=True)
def duplicates_count(df): return pd.DataFrame({'duplicate_rows':[int(df.duplicated().sum())]})
def group_count(df, group_by: List[str]):
    group_by = [g for g in group_by if g in df.columns]
    if not group_by: return pd.DataFrame({'row_count':[len(df)]})
    return df.groupby(group_by, dropna=False).size().reset_index(name='row_count')
