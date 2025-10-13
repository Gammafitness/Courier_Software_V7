from .base import common_components, apply_min_and_tax

def quote(cfg, pincode, row, used_weight, declared_value, shared):
    status = str(row.get("status","")).upper()
    perkg  = cfg["rates"].get(str(row.get("zone","")).upper())
    if not perkg:
        return {"reason": f"Rate missing for zone {row.get('zone')}"}
    parts = common_components(cfg, perkg, used_weight, declared_value, status)
    oda = 0.0
    if "ODA" in status or "EDL" in status:
        oda = float(cfg.get("oda_fixed", 0))
    # fuel on freight only (generic)
    fuel = parts["freight"] * (cfg.get("fuel_pct",0)/100.0)
    subtotal_no_gst = parts["freight"] + parts["docket"] + parts["insurance"] + oda + fuel
    subtotal, gst, total = apply_min_and_tax(cfg, subtotal_no_gst)
    return {
        **parts, "oda": oda, "fuel": fuel,
        "subtotal": subtotal, "gst": gst, "total": total, "reason":"OK"
    }
