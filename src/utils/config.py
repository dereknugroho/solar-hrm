"""Load and expose configuration settings for the project."""
import json

from src.utils.paths import from_root

def load_config(filename: str = "config.json") -> dict:
    """Load a JSON configuration file from the project root."""
    path = from_root(filename)
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}")

CONFIG = load_config()
PREPROCESSING = CONFIG.get("preprocessing", {})
