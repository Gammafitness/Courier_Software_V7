from importlib import import_module

_registry = {}

def get_engine(courier_name: str):
    key = (courier_name or "").lower().strip().replace(" ", "_")
    if key in _registry:
        return _registry[key]
    # try known modules first
    candidates = [key]
    if key == "blue_dart":
        candidates.append("bluedart")
    for modname in candidates:
        try:
            module = import_module(f".{modname}", __name__)
            _registry[key] = module
            return module
        except ModuleNotFoundError:
            continue
    # default generic
    module = import_module(".generic", __name__)
    _registry[key] = module
    return module
