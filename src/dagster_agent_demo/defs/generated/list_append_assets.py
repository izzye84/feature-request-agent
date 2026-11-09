import importlib.util
from pathlib import Path


def _load_generated_assets():
    module_path = Path(__file__).resolve().parents[4] / "generated" / "list_append_assets.py"
    spec = importlib.util.spec_from_file_location(
        "generated_list_append_assets", module_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.asset_a, module.asset_b


asset_a, asset_b = _load_generated_assets()
