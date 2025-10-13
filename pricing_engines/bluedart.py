# pricing_engines/bluedart.py
from .base import common_components, apply_min_and_tax

# ----------------------------------------------------------------------
# Bluedart ODA Matrix: (min_km, max_km, [ (max_weight, charge) ... ])
# ----------------------------------------------------------------------
ODA_MATRIX = [
    (20, 50,  [(100, 550),  (250, 990),  (500, 1100), (1000, 1375)]),
    (51, 100, [(100, 825),  (250, 1210), (500, 1375), (1000, 1650)]),
    (101,150, [(100,1100),  (250,1650),  (500,1925),  (1000,2200)]),
    (151,200, [(100,1375),  (250,1925),  (500,2200),  (1000,2475)]),
    (201,250, [(100,1650),  (250,2200),  (500,2750),  (1000,3300)]),
    (251,300, [(100,1925),  (250,2500),  (500,3150),  (1000,3800)]),
    (301,350, [(100,2200),  (250,2800),  (500,3550),  (1000,4300)]),
    (351,400, [(100,2475),  (250,3100),  (500,3950),  (1000,4800)]),
]

def get_oda_charge(distance_km: float, weight_kg: float) -> float:
    """
    Compute ODA charge from internal matrix.
    Uses inclusive upper bound on both distance and weight.
    """
    distance_km = float(distance_km)
    weight_kg = float(weight_kg)

    for (min_d, max_d, weight_brackets) in ODA_MATRIX:
        # correct inclusive logic
        if min_d <= distance_km <= max_d:
            for (max_w, charge) in weight_brackets:
                if weight_kg <= max_w:
                    return charge
            # heavier than last bracket
            return weight_brackets[-1][1]
    # above max distance → use highest bracket in last range
    return ODA_MATRIX[-1][2][-1][1]


def quote(cfg, pincode, row, used_weight, declared_value, shared):
    """
    Bluedart pricing with embedded ODA logic (distance-weight matrix).
    """
    status = str(row.get("status", "")).upper()
    zone = str(row.get("zone", "")).upper()
    perkg = cfg["rates"].get(zone)

    if not perkg:
        return {"reason": f"Rate missing for zone {zone}"}

    parts = common_components(cfg, perkg, used_weight, declared_value, status)

    oda = 0.0
    if any(k in status for k in ("ODA", "EDL", "SPECIAL")):
        # ---- get distance safely ----
        df = shared.get("df")
        dist = 0.0
        if df is not None and not df.empty:
            match = df.loc[df["pincode"] == pincode]
            if not match.empty:
                val = match.iloc[0].get("distance_km", 0)
                try:
                    dist = float(val)
                except ValueError:
                    dist = 0.0
        if dist <= 0:
            dist = 50.0  # safe default
        oda = get_oda_charge(dist, used_weight)

    subtotal_before_fuel = (
        parts["freight"] + parts["docket"] + parts["insurance"] + oda
    )
    fuel = subtotal_before_fuel * (cfg.get("fuel_pct", 0) / 100.0)

    subtotal_no_gst = subtotal_before_fuel + fuel
    subtotal, gst, total = apply_min_and_tax(cfg, subtotal_no_gst)

    return {
        **parts,
        "oda": round(oda, 2),
        "fuel": round(fuel, 2),
        "subtotal": round(subtotal, 2),
        "gst": round(gst, 2),
        "total": round(total, 2),
        "reason": "OK",
    }
