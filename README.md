# **SynMax Data Agent**



A fast, CLI-only data agent for large tabular datasets (CSV/XLSX/Parquet). Works fully offline (no keys required). Optionally uses an LLM for short “Insights (caveated)” bullets.



### **Features**



* Dataset-agnostic: CSV, Excel, Parquet; local files or URL (incl. Google Drive sharable links).
* Schema \& cleaning: robust type inference, numeric/datetime coercion, missing-value summary, drop all-null columns.
* Natural language CLI: ask for counts, group-bys, aggregates, top-N, correlations, and anomalies.
* Deterministic executor: plans map to safe Pandas ops (no hidden state).
* Analytics:
* Meta: shape, columns, dtypes, head/tail, missing, duplicates
* Aggregates: sum/mean/median/min/max/std with group by
* Distributions: value counts (top-K)
* Trends: by year (derived from a chosen date column)
* Anomalies: z-score outliers (univariate); IsolationForest (multivariate, if enabled)
* Sorting+filtering: top N rows by <col> where <filter>
* Evidence first: every answer shows the plan, method, and a small preview.
* Optional LLM: add “Insights (caveated)” bullets after results.



### **Requirements**



* Python 3.10+
* Dependencies listed in requirements.txt (notably pandas, pyarrow, and optionally scikit-learn, openai/anthropic if you use those features)



### **System Setup**



#### Windows (PowerShell)



\# Create a virtual environment

python -m venv venv



\# Activate it

.\\venv\\Scripts\\Activate.ps1



\# Upgrade pip (recommended)

python -m pip install --upgrade pip



\# Install dependencies

pip install -r requirements.txt



#### macOS/Linux (bash/zsh)



\# Create a virtual environment

python3 -m venv venv



\# Activate it

source venv/bin/activate



\# Upgrade pip (recommended)

python -m pip install --upgrade pip



\# Install dependencies

pip install -r requirements.txt





### **Running the Agent**



#### Local file (CSV/XLSX/Parquet)



\# CSV

python -m src.agent --data-path "C:\\path\\to\\file.csv"



\# Excel (specify sheet if needed)

python -m src.agent --data-path "C:\\path\\to\\file.xlsx" --sheet "Sheet1"



\# Parquet (recommended)

python -m src.agent --data-path "C:\\path\\to\\file.parquet"



#### From a URL (HTTP/HTTPS or Google Drive sharable link)



python -m src.agent --from-url "https://example.com/data.csv"



#### Set a default path for convenience



$env:DATA\_PATH = "C:\\path\\to\\file.parquet"

python -m src.agent





##### **Note:If your dataset has a date/timestamp column, you can derive a year column by telling the agent which column to use:**



python -m src.agent --data-path "C:\\path\\to\\file.parquet" --date-col eff\_gas\_day





#### **Runtime Flags**



* --data-path <path>: Local dataset file (csv/xlsx/parquet)
* --from-url <url>: Download dataset at runtime (saved to ./data/)
* --sheet <name>: Excel sheet name
* --sep <char>: CSV delimiter override (e.g., ; or |)
* --date-col <column>: Source datetime to derive year
* --insights: Append brief “Insights (caveated)” (requires OpenAI/Anthropic key and SDK)





#### **Optional: LLM Insights**



Enable short, caveated bullets after tables (e.g., trends, possible drivers, and correlation).



##### **OpenAI (recommended)**



\# Install SDK once

python -m pip install openai

"openai>=1.40.0" | Out-File -Encoding utf8 -Append requirements.txt



\# Set your key for this shell

$env:OPENAI\_API\_KEY = "sk-..."



\# Run with insights on

python -m src.agent --data-path "C:\\path\\to\\file.parquet" --date-col eff\_gas\_day --insights



##### **Anthropic (alternative)**



python -m pip install anthropic

"anthropic>=0.34.0" | Out-File -Encoding utf8 -Append requirements.txt



$env:ANTHROPIC\_API\_KEY = "sk-ant-..."

python -m src.agent --data-path "C:\\path\\to\\file.parquet" --date-col eff\_gas\_day --insights





##### **Troubleshooting**



ImportError: parquet engine (pyarrow/fastparquet) not found

→ Install pyarrow:

python -m pip install pyarrow

"pyarrow>=15.0.0" | Out-File -Encoding utf8 -Append requirements.txt





UnicodeDecodeError on import (e.g., byte 0x95)

→ A source file was saved with CP-1252 characters. Re-save affected .py files as UTF-8. Use PowerShell to rewrite:

Set-Content -Encoding utf8 src\\answer.py -Value (Get-Content src\\answer.py)





##### **Performance Tips**



* Keep previews small (default shows up to 10 rows).
* Prefer Parquet; CSVs with millions of rows will read slower.
* Group by low-cardinality columns (e.g., state\_abb) for faster aggregations.
* Drop fully null columns (the agent does this automatically in preprocessing).





#### **Project Structure**





synmax-agent/

├─ src/

│  ├─ agent.py           # CLI entrypoint, REPL, planning/execution wiring

│  ├─ dataset.py         # load from path/url; CSV/XLSX/Parquet

│  ├─ preprocess.py      # schema inference, coercions, missing/cleanup

│  ├─ planner.py         # rule-based NL → plan (group\_by, ops, filters, top-N, etc.)

│  ├─ analysis.py        # deterministic ops (aggregates, correlations, anomalies, sort\_top)

│  ├─ answer.py          # formats answers with Plan/Method/Preview

│  ├─ utils.py           # banner, helpers

│  └─ insights.py        # optional LLM “Insights (caveated)”

├─ requirements.txt

├─ README.md

└─ .gitignore





#### **Assumptions \& Limitations**



* Causality: We do not claim causation; insights highlight correlation ≠ causation and note confounders/missingness.
* Year derivation: --date-col chooses the datetime column to derive year; without it we guess a likely candidate.
* Missing geo columns: Fully null columns (e.g., latitude/longitude in some exports) are dropped for speed.
* Anomalies ≠ errors: Outliers may be legitimate spikes; review in domain context.
* LLM mode (optional): Only column names/types + a tiny preview are used for insights; the agent works without any keys.





























