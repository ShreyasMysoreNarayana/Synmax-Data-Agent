import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Allow runtime override via env var fallback to dummy for dev
DEFAULT_DATA_PATH = os.getenv("DATA_PATH", "data/dummy_dataset.csv")
