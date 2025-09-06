# -*- coding: utf-8 -*-
from typing import Dict, Any
import pandas as pd

def format_answer(result: pd.DataFrame, plan: Dict[str, Any], method_note: str) -> str:
    # PowerShell text block
    if isinstance(result, pd.DataFrame):
        shape = f"returned {result.shape[0]} rows x {result.shape[1]} columns."
        preview = result.head(10).to_string(index=False)
    else:
        shape = str(result)
        preview = str(result)

    out = []
    out.append(f"Answer: {shape}")
    out.append("Evidence:")
    out.append(f"  - Plan: {repr(plan)}")
    out.append(f"  - Method: {method_note}")
    if isinstance(result, pd.DataFrame):
        out.append("  - Preview:")
        out.append(preview)
    return "\n".join(out)
