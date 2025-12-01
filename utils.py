import json
import os


def load_config(config_file):
    """Load arguments from a JSON config file."""
    if not os.path.exists(config_file):
        return {}
    with open(config_file, "r") as f:
        return json.load(f)


