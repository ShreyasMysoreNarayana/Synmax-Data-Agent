from rich import print as rprint
import pandas as pd
import argparse

from .dataset import load_dataframe
from .preprocess import infer_schema, clean_data
from .planner import plan_from_nl
from .analysis import (
    aggregate, correlations, zscore_outliers, iforest_outliers, sort_top,
    meta_shape, meta_columns, meta_dtypes, meta_head, meta_tail, meta_describe,
    unique_count, value_counts, missing_summary, duplicates_count, group_count
)
from .answer import format_answer
from .utils import banner
from .insights import generate_insights  # This is optional; safely no-ops without keys

def parse_args():
    ap = argparse.ArgumentParser(description='SynMax Data Agent')
    ap.add_argument('--data-path', type=str, default=None, help='Local path to dataset (csv/xlsx/parquet)')
    ap.add_argument('--from-url', type=str, default=None, help='HTTP/HTTPS or Google Drive link to download')
    ap.add_argument('--sep', type=str, default=None, help='CSV delimiter override (e.g., "," or "|" )')
    ap.add_argument('--sheet', type=str, default=None, help='Excel sheet name to load')
    ap.add_argument('--date-col', type=str, default=None, help='Column to derive a year from (optional)')
    ap.add_argument('--insights', action='store_true', help='Generate caveated insights (uses LLM if key set)')
    return ap.parse_args()

def execute_plan(df: pd.DataFrame, plan: dict):
    t = plan.get('type','')

    # Meta Data Schema
    if t == 'meta:shape':        return meta_shape(df), 'Dataset shape'
    if t == 'meta:columns':      return meta_columns(df), 'Columns and dtypes'
    if t == 'meta:dtypes':       return meta_dtypes(df), 'Data types'
    if t == 'meta:describe':     return meta_describe(df), 'Numeric summary statistics'
    if t == 'meta:head':         return meta_head(df, plan.get('n',5)), f"Head (first {plan.get('n',5)} rows)"
    if t == 'meta:tail':         return meta_tail(df, plan.get('n',5)), f"Tail (last {plan.get('n',5)} rows)"
    if t == 'meta:missing':      return missing_summary(df), 'Missing values by column'
    if t == 'meta:duplicates':   return duplicates_count(df), 'Duplicate rows count'

    # Unique value counts
    if t == 'unique_count':      return unique_count(df, plan['col']), f"Unique count of {plan['col']}"
    if t == 'value_counts':      return value_counts(df, plan['col'], plan.get('n',10)), f"Value counts for {plan['col']} (top {plan.get('n',10)})"

    # Group counts
    if t == 'group_count':       return group_count(df, plan.get('group_by',[])), f"Row counts by {', '.join(plan.get('group_by',[]))}"

    # Aggregates Calculations
    if t == 'aggregate':
        ops     = plan.get('ops', {}) or {}
        filters = plan.get('filters', {}) or {}
        group_by= plan.get('group_by', []) or []

        # row-count per group
        if ops == {'*': 'count'} and group_by:
            return group_count(df, group_by), f"Row counts by {', '.join(group_by)}"

        # row-count but optionally filtered, no grouping
        if ops == {'*': 'count'}:
            data = df.copy()
            for k, v in filters.items():
                if k in data.columns:
                    if isinstance(v, dict) and 'between' in v:
                        lo, hi = v['between']
                        data = data[(data[k] >= lo) & (data[k] <= hi)]
                    else:
                        data = data[data[k] == v]
            return pd.DataFrame({'row_count':[len(data)]}), 'Row count with optional filters'

        # standard numeric aggregations
        return aggregate(df, group_by=group_by, ops=ops, filters=filters), 'Aggregate with group_by/filters'

    # Correlation Calculations
    if t == 'correlation':
        cols = plan.get('cols') or []
        return correlations(df, cols=cols), 'Pairwise Pearson correlations'

    # Sorting for Top-N
    if t == 'sort_top':
        by = plan.get('by') or []
        top_n = int(plan.get('top_n', 10))
        ascending = bool(plan.get('ascending', False))
        flt = plan.get('filters', {}) or {}
        return sort_top(df, by=by, top_n=top_n, ascending=ascending, filters=flt), f"Sorted by {by} (ascending={ascending}) top {top_n}"

    # Anomalies detection
    if t.startswith('anomaly:zscore'):
        col = plan.get('col')
        thr = float(plan.get('threshold', 3.0))
        if col is None or col not in df.columns:
            col = df.select_dtypes(include=['number']).columns[0]
        return zscore_outliers(df, col, thr), f'Z-score outliers on {col} (|z| >= {thr})'

    if t == 'anomaly:iforest':
        cols = plan.get('cols') or list(df.select_dtypes(include=['number']).columns)[:4]
        cont = float(plan.get('contamination', 0.01))
        return iforest_outliers(df, cols, cont), f'IsolationForest anomalies on {cols} (contamination={cont})'

    
    return df.head(10), 'Default preview'

def main():
    banner()
    args = parse_args()

    # Load Data
    df, fmt = load_dataframe(path=args.data_path, url=args.from_url, sep=args.sep, sheet_name=args.sheet)
    rprint(f'[green]Dataset loaded[/green] (format: {fmt}) with shape {df.shape}')

    # Cleaned Data Schema
    schema = infer_schema(df)
    rprint('[cyan]Inferred schema:[/cyan] ' + ', '.join(f'{k}:{v}' for k,v in schema.items()))
    df = clean_data(df, schema)

    # Derive year from chosen date column, or a sensible guess
    target_dt = args.date_col
    if target_dt and target_dt in df.columns:
        try:
            df['year'] = pd.to_datetime(df[target_dt], errors='coerce', utc=True).dt.year
        except Exception:
            pass
    else:
        name_hint = [c for c in df.columns if any(k in c.lower() for k in ['date','day','gas_day','eff_gas_day','timestamp'])]
        dt_cols = [c for c,t in schema.items() if t == 'datetime']
        candidate = (name_hint[0] if name_hint else (dt_cols[0] if dt_cols else None))
        if candidate:
            try:
                df['year'] = pd.to_datetime(df[candidate], errors='coerce', utc=True).dt.year
            except Exception:
                pass

    
    while True:
        q = input('\nAsk a question > ').strip()
        if q.lower() in ('exit','quit'):
            print('Goodbye!')
            break
        plan = plan_from_nl(q, list(df.columns))
        result, method_note = execute_plan(df, plan)
        print(format_answer(result, plan, method_note))

        # Optional: caveated insights (requires OPENAI_API_KEY or ANTHROPIC_API_KEY)
        if args.insights:
            try:
                schema_map = {c:str(dt) for c,dt in df.dtypes.items()}
                insight = generate_insights(q, plan, result if hasattr(result, "head") else pd.DataFrame(), schema_map)
                if insight:
                    print("\nInsights (caveated):")
                    print(insight)
            except Exception:
                # stays silent if insights fail; core result already printed
                pass

if __name__ == '__main__':
    main()
