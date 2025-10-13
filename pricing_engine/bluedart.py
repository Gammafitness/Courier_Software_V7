import pandas as pd
import os

ODA_CHARGES_FILE = os.path.join("uploads", "Bluedart ODA Charges copy.xlsx")

def load_oda_metrics():
    """Load the ODA charge metrics Excel for Bluedart."""
    if not os.path.exists(ODA_CHARGES_FILE):
        raise FileNotFoundError(f"ODA Charges file not found: {ODA_CHARGES_FILE}")
    df = pd.read_excel(ODA_CHARGES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def get_oda_charge(distance_km, weight, oda_metrics):
    """Fetch ODA charge using distance range and weight slab."""
    matched_row = None
    for _, row in oda_metrics.iterrows():
        if row.get('min_km', 0) <= distance_km <= row.get('max_km', 999999):
            matched_row = row
            break

    if matched_row is None:
        return 0.0

    # Dynamically find correct weight column
    weight_cols = [c for c in oda_metrics.columns if 'kg' in c or 'weight' in c]
    rate = 0.0
    for col in weight_cols:
        try:
            limit = float(col.replace('upto_', '').replace('kg', '').replace('weight_', '').strip())
            if weight <= limit:
                rate = float(matched_row[col])
                break
        except Exception:
            continue

    if not rate and weight_cols:
        try:
            rate = float(matched_row[weight_cols[-1]])
        except Exception:
            rate = 0.0

    return rate

def calculate_price(data, metrics):
    """Bluedart pricing rule with ODA (Special) fetched from Excel."""
    weight = float(data.get("weight", 0))
    zone_rate = float(metrics.get("zone_rate", 0))
    docket = float(metrics.get("docket_charge", 0))
    insurance = float(metrics.get("insurance", 0))
    fuel_pct = float(metrics.get("fuel_surcharge", 0)) / 100
    gst_pct = float(metrics.get("gst", 18)) / 100
    min_charge = float(metrics.get("minimum_charge", 0))
    status = (data.get("status") or "").lower()
    oda_type = (data.get("oda_type") or metrics.get("oda_type") or "").lower()
    distance_km = float(data.get("distance_km", 0))

    oda_charge = 0.0
    # FIX: allow partial match like "special (bluedart)"
    if status == "oda" and "special" in oda_type:
        oda_metrics = load_oda_metrics()
        oda_charge = get_oda_charge(distance_km, weight, oda_metrics)

    base_freight = zone_rate * weight
    subtotal = base_freight + docket + insurance + oda_charge
    fuel = subtotal * fuel_pct
    pre_gst = subtotal + fuel

    if pre_gst < min_charge:
        pre_gst = min_charge

    gst = pre_gst * gst_pct
    total = pre_gst + gst
    return round(total, 2)
