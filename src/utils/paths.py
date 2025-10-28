"""Utility for constructing absolute filepaths relative to the project root."""

import sys

from pathlib import Path

def _find_project_root() -> Path:
    """Find project root by walking up until a known marker is found."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "config.json").exists() or (parent / ".git").exists():
            return parent
    # fallback: assume src/ is directly under root
    return current.parents[2]

PROJECT_ROOT = _find_project_root()

def from_root(*parts) -> Path:
    """Return an absolute path relative to the project root."""
    return PROJECT_ROOT.joinpath(*parts)

def ensure_src_importable():
    """Ensure src/ is on sys.path for consistent imports."""
    src_dir = PROJECT_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.append(str(src_dir))

ensure_src_importable()
