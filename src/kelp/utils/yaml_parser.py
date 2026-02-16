from pathlib import Path
import yaml


def load_yaml(file_path: str | Path) -> dict:
    """Load a YAML File from the given path."""
    if isinstance(file_path, str):
        file_path = Path(file_path)
    text = file_path.read_text(encoding="utf-8")
    return yaml.safe_load(text)
