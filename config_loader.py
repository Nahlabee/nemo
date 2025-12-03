import toml
from pathlib import Path

def load_config():
    """Load config.toml located in the project root."""
    # Resolve path of the project root dynamically
    root = Path(__file__).resolve().parent
    config_path = root / "config.toml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return toml.load(config_path)