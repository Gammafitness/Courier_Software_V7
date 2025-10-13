"""
Pricing engine plugin interface.
Each engine exposes a `quote(cfg, pincode, row, used_weight, declared_value, shared)` function
that returns a dict with fields: freight, fuel, insurance, oda, docket, subtotal, gst, total, reason
"""
from typing import Dict, Any

def common_components(cfg: Dict[str, Any], perkg: float, used_weight: float, declared_value: float, status: str) -> Dict[str,float]:
    freight = perkg * used_weight
    pct_amt  = (cfg.get("insurance_pct",0)/100.0) * float(declared_value or 0)
    flat_amt = float(cfg.get("insurance_flat",0) or 0)
    insurance = max(pct_amt, flat_amt)
    docket = float(cfg.get("docket", 0))
    return {"freight":freight, "insurance":insurance, "docket":docket}

def apply_min_and_tax(cfg, subtotal_no_gst: float):
    subtotal = max(subtotal_no_gst, float(cfg.get("min_charge",0) or 0))
    gst = subtotal * (cfg.get("gst_pct", 0)/100.0)
    total = subtotal + gst
    return subtotal, gst, total
