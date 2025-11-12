import logging
import os

from datetime import datetime

from core.utils.config import FILEPATHS
from core.utils.paths import from_root

# Define log directory
LOG_DIR = from_root(FILEPATHS['dir_log'])

def info(msg: str, log=False, severity=None) -> None:
    """Print and log messages about events in the pipeline."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

    if log:
        pass
