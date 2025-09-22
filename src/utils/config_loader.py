import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent.parent / "config.json"

def load_config() -> dict:
    """Load config.json into a Python dictionary."""
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

config = load_config()
