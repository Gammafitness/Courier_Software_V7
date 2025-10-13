# Modular Pricing Engines

This refactor adds a plug-in architecture so each courier's pricing logic lives in its own Python file under `pricing_engines/`.

## Files
- `pricing_engines/base.py` – helpers for components and taxes
- `pricing_engines/generic.py` – default engine: fuel on freight, ODA fixed when status has ODA/EDL
- `pricing_engines/bluedart.py` – special ODA & fuel on (freight + docket + insurance + ODA)
- `pricing_engines/__init__.py` – dynamic registry; picks engine by courier name

## How it works
The Flask app calls `get_engine(cfg["name"])` and delegates the price computation to `engine.quote(...)`.
If a courier does not have a dedicated engine file, the generic engine is used automatically.

To add a new courier (e.g., `Delhivery`):
1. Create `pricing_engines/delhivery.py`
2. Implement `quote(cfg, pincode, row, used_weight, declared_value, shared)`
3. The engine will be picked when courier name matches "delhivery".

No database schema changes are required.
