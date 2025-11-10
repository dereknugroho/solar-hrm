"""Utility for constructing absolute filepaths relative to the project root."""

import sys

from pathlib import Path

def _find_project_root() -> Path:
    """Find project root by walking up until a known marker is found."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "config.json").exists() or (parent / ".git").exists():
            return parent

    raise RuntimeError("Could not find project root.")

    # # Fallback: assume src/ is directly under root
    # return current.parents[2]

PROJECT_ROOT = _find_project_root()

def from_root(*parts) -> Path:
    """Return an absolute path relative to the project root."""
    return PROJECT_ROOT.joinpath(*parts)
