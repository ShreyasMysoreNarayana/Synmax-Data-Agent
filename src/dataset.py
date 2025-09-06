import os
import re
import requests
import pandas as pd
from typing import Optional, Tuple
from .config import DEFAULT_DATA_PATH

def _extract_drive_file_id(url: str) -> Optional[str]:
    m = re.search(r'/d/([A-Za-z0-9_-]{20,})/', url)
    if m:
        return m.group(1)
    # alt pattern (?id=)
    m = re.search(r'[?&]id=([A-Za-z0-9_-]{20,})', url)
    return m.group(1) if m else None

def _download_url(url: str, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if 'drive.google.com' in url:
        file_id = _extract_drive_file_id(url)
        if not file_id:
            raise ValueError("Could not parse Google Drive file id from URL.")
        dl = f'https://drive.google.com/uc?export=download&id={file_id}'
        r = requests.get(dl, allow_redirects=True)
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            f.write(r.content)
        return out_path
    # generic HTTP(S)
    r = requests.get(url, allow_redirects=True)
    r.raise_for_status()
    with open(out_path, 'wb') as f:
        f.write(r.content)
    return out_path

def _guess_ext_from_headers(url: str, resp_headers: dict) -> str:
    ct = (resp_headers.get('Content-Type') or '').lower()
    if 'parquet' in ct:
        return '.parquet'
    if 'excel' in ct or 'spreadsheetml' in ct or 'xlsx' in url:
        return '.xlsx'
    if 'csv' in ct or url.lower().endswith('.csv'):
        return '.csv'
    return ''

def _ensure_local_from_url(url: str, target_dir: str = 'data') -> str:
    # code block to derive a filename
    fname = None
    # if URL ends with a file-like name
    m = re.search(r'/([^/?#]+)$', url)
    if m and '.' in m.group(1):
        fname = m.group(1)
    if not fname:
        # fallback name with guessed extension
        fname = 'downloaded_dataset'

    # provisional path 
    path = os.path.join(target_dir, fname)
    try:
        os.makedirs(target_dir, exist_ok=True)
        r = requests.get(url if 'drive.google.com' not in url else f'https://drive.google.com/uc?export=download&id={_extract_drive_file_id(url)}', allow_redirects=True)
        r.raise_for_status()
        # if no ext in fname the we try to guess from headers
        if '.' not in fname:
            ext = _guess_ext_from_headers(url, r.headers) or '.csv'
            path = os.path.join(target_dir, f'{fname}{ext}')
        with open(path, 'wb') as f:
            f.write(r.content)
        return path
    except Exception:
        
        if '.' not in fname:
            fname += '.csv'
        path = os.path.join(target_dir, fname)
        return _download_url(url, path)

def _read_dataframe(local_path: str, sep: Optional[str] = None, sheet_name: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
    ext = os.path.splitext(local_path)[1].lower()
    if ext in ('.csv', '.txt'):
        df = pd.read_csv(local_path, sep=sep if sep else None)
        fmt = 'csv'
    elif ext in ('.xlsx', '.xls'):
        df = pd.read_excel(local_path, sheet_name=sheet_name)
        fmt = 'excel'
    elif ext in ('.parquet',):
        df = pd.read_parquet(local_path)
        fmt = 'parquet'
    else:
        # Let's try csv
        try:
            df = pd.read_csv(local_path, sep=sep if sep else None)
            fmt = 'csv?'
        except Exception as e:
            raise ValueError(f'Unsupported dataset format: {ext}') from e
    return df, fmt

def load_dataframe(path: Optional[str] = None,
                   url: Optional[str] = None,
                   sep: Optional[str] = None,
                   sheet_name: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
    """
    Load a dataset from:
      - local path (path)
      - URL/Google Drive (url)
      - env default (DEFAULT_DATA_PATH)
      - prompt the user if none provided
    Supports CSV, Excel, Parquet.
    """
    chosen_path = path

    if url and not chosen_path:
        chosen_path = _ensure_local_from_url(url)

    if not chosen_path:
        # fallback: env default
        chosen_path = DEFAULT_DATA_PATH

    if not os.path.exists(chosen_path):
        # interactive prompt (only if running in a terminal)
        try:
            candidate = input(f'Dataset not found at {chosen_path}. Enter a local file path or press Enter to cancel: ').strip()
        except EOFError:
            candidate = ''
        if candidate:
            chosen_path = candidate

    if not os.path.exists(chosen_path):
        raise FileNotFoundError(f"Dataset not found. Tried: '{chosen_path}'. Provide --data-path or --from-url or set DATA_PATH.")

    return _read_dataframe(chosen_path, sep=sep, sheet_name=sheet_name)
