from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_file, abort
import pandas as pd, sqlite3, os, json, datetime, re
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = "gamma_secret_key_v421"



# --- Bluedart ODA coded metrics (distance-weight matrix) ---
def get_bluedart_oda_charge(distance_km: float, weight_kg: float) -> float:
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
    if distance_km is None or distance_km <= 0:
        return 0.0
    # find distance band
    for dmin, dmax, tiers in ODA_MATRIX:
        if dmin <= distance_km <= dmax:
            # pick first tier with max_weight >= weight_kg
            for max_wt, charge in tiers:
                if weight_kg <= max_wt:
                    return float(charge)
            # if heavier than all, use last tier
            return float(tiers[-1][1])
    # if beyond last band, extrapolate with last band tiers
    tiers = ODA_MATRIX[-1][2]
    for max_wt, charge in tiers:
        if weight_kg <= max_wt:
            return float(charge)
    return float(tiers[-1][1])

# Paths
DB_PATH    = os.path.join(app.root_path, "couriers.db")
UPLOAD_DIR = os.path.join(app.root_path, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Default login
DEFAULT_USER = {"username": "admin", "password": "admin123"}

# In-memory caches
couriers = {}             # name -> cfg dict (df, rates, etc.)
last_results_df = None    # pandas df for export (optional)
bluedart_oda_table = None # pandas df for special ODA matrix

# ====================== DB ======================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS couriers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        file_path TEXT,
        rates TEXT,
        docket FLOAT,
        fuel_pct FLOAT,
        insurance_pct FLOAT,
        insurance_flat FLOAT,
        oda_type TEXT DEFAULT 'Fixed',
        oda_fixed FLOAT,
        gst_pct FLOAT,
        min_charge FLOAT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS recent_searches(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        checked_at TEXT,
        pincode TEXT,
        courier TEXT,
        weight REAL,
        total REAL
    )""")
    conn.commit(); conn.close()

def save_courier_to_db(cfg):
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("""INSERT OR REPLACE INTO couriers
        (name,file_path,rates,docket,fuel_pct,insurance_pct,insurance_flat,oda_type,oda_fixed,gst_pct,min_charge)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (cfg["name"], cfg["file_path"], json.dumps(cfg["rates"]), cfg["docket"], cfg["fuel_pct"],
         cfg["insurance_pct"], cfg["insurance_flat"], cfg["oda_type"], cfg["oda_fixed"],
         cfg["gst_pct"], cfg["min_charge"]))
    conn.commit(); conn.close()

# ====================== Helpers ======================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c.strip().lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in lower: return lower[n]
        return None
    mapping = {
        "pincode": pick("pincode","pin","pin code"),
        "status": pick("status","service status"),
        "zone": pick("zone","region"),
        "state": pick("state"),
        "location": pick("location","city","district","area"),
        "oda_distance": pick("oda distance (optional)","oda distance","distance","dist (km)","km")
    }
    rename = {mapping[k]: k for k in mapping if mapping[k]}
    df = df.rename(columns=rename)

    if "pincode" in df.columns:
        df["pincode"] = df["pincode"].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper().str.strip()
    if "zone" in df.columns:
        df["zone"] = df["zone"].astype(str).str.upper().str.strip()
    if "oda_distance" in df.columns:
        df["oda_distance"] = pd.to_numeric(df["oda_distance"], errors="coerce")
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip()
    if "location" in df.columns:
        df["location"] = df["location"].astype(str).str.strip()
    return df

def parse_range(text):
    m = re.search(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)", str(text))
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))
    return float(m.group(1)), float(m.group(2))

def select_band(value, bands):
    for band in bands:
        rng = parse_range(band)
        if rng and rng[0] <= value <= rng[1]:
            return band
    if bands:
        first = parse_range(bands[0]); last = parse_range(bands[-1])
        if first and value < first[0]: return bands[0]
        if last and value > last[1]:  return bands[-1]
    return None

def load_bluedart_oda_table():
    # Try to read Bluedart ODA matrix from uploads. Accept multiple filenames and flexible headers.
    candidates = [
        "Bluedart ODA Charges copy.xlsx",
        "Bluedart ODA Charges.xlsx",
        "Bluedart_ODA_Charges.xlsx",
        "Bluedart_ODA.xlsx"
    ]
    for fname in candidates:
        path = os.path.join(UPLOAD_DIR, fname)
        if not os.path.exists(path):
            continue
        try:
            raw = pd.read_excel(path, header=0)
        except Exception as e:
            print(f"[ERROR] Reading ODA matrix {fname}: {e}")
            continue

        df = raw.copy()

        # If headers are "Unnamed", assume first row holds weight bands. Promote first row to column headers.
        if all(str(c).startswith("Unnamed") for c in df.columns[1:]) and df.shape[0] >= 1:
            header_row = df.iloc[0].tolist()
            # Keep the first column as is (distance label header)
            new_cols = [str(df.columns[0])] + [str(x) for x in header_row[1:]]
            df.columns = new_cols
            df = df.iloc[1:].reset_index(drop=True)

        # Standardize first column to "distance_band"
        df = df.rename(columns={df.columns[0]: "distance_band"})

        # Keep only rows that look like "20-50", "51 - 100", etc in the distance cell.
        mask = df["distance_band"].astype(str).str.contains(r"\d+\s*-\s*\d+")
        df = df[mask].copy()

        # Normalize weight band column names, e.g., "0-100 Kgs" -> "0-100"
        newcols = {}
        for c in df.columns[1:]:
            m = re.search(r"(\d+\s*-\s*\d+)", str(c))
            newcols[c] = (m.group(1).replace(" ", "") if m else str(c).strip())
        df = df.rename(columns=newcols)

        # Coerce values to numeric
        for c in df.columns[1:]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Also normalize distance_band to keep the label text intact; selection uses parse_range.
        print(f"[INFO] Loaded Bluedart ODA matrix from {fname}: rows={len(df)}, weight_bands={list(df.columns[1:])}")
        return df.reset_index(drop=True)

    print("[WARN] Bluedart ODA matrix not found in /uploads")
    return None

def load_all_couriers_from_db():
    couriers.clear()
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("""SELECT name,file_path,rates,docket,fuel_pct,insurance_pct,insurance_flat,
                          oda_type,oda_fixed,gst_pct,min_charge FROM couriers""")
    rows = cur.fetchall(); conn.close()
    for row in rows:
        name, file_path, rates_json, docket, fuel, ins_pct, ins_flat, oda_type, oda_fixed, gst, min_charge = row
        if not file_path or not os.path.exists(file_path):
            print(f"[WARN] Missing Excel for {name}: {file_path}"); continue
        try:
            df = normalize_columns(pd.read_excel(file_path))
        except Exception as e:
            print(f"[ERROR] Failed reading {name} ({file_path}): {e}"); continue
        couriers[name] = {
            "name": name, "df": df, "file_path": file_path,
            "rates": json.loads(rates_json or "{}"),
            "docket": float(docket or 0),
            "fuel_pct": float(fuel or 0),
            "insurance_pct": float(ins_pct or 0),
            "insurance_flat": float(ins_flat or 0),
            "oda_type": oda_type or "Fixed",
            "oda_fixed": float(oda_fixed or 0),
            "gst_pct": float(gst or 18),
            "min_charge": float(min_charge or 0)
        }
        print(f"[INFO] Loaded courier: {name} rows={len(df)} oda_type={couriers[name]['oda_type']} rates={couriers[name]['rates']}")

def auto_seed_from_uploads():
    defaults = {
        "Bluedart.xlsx": {
            "name": "Bluedart", "rates": {"A":25,"B":30,"C":35}, "docket":50, "fuel_pct":15,
            "insurance_pct":2, "insurance_flat":100, "oda_type":"Special", "oda_fixed":0, "gst_pct":18, "min_charge":0
        },
        "XP_India_Pincodes_With_Correct_Zones.xlsx": {
            "name":"XP India", "rates":{"A":22,"B":28,"C":33}, "docket":45, "fuel_pct":12,
            "insurance_pct":1.5, "insurance_flat":50, "oda_type":"Fixed", "oda_fixed":150, "gst_pct":18, "min_charge":0
        }
    }
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM couriers"); n = cur.fetchone()[0]
    if n == 0:
        for fname, cfg in defaults.items():
            path = os.path.join(UPLOAD_DIR, fname)
            if os.path.exists(path):
                try:
                    _ = normalize_columns(pd.read_excel(path))
                    cfg_write = cfg.copy(); cfg_write["file_path"] = path
                    save_courier_to_db(cfg_write)
                    print(f"[INFO] Seeded courier from uploads: {cfg_write['name']}")
                except Exception as e:
                    print(f"[ERROR] Failed seeding {fname}: {e}")
    conn.close()

def bluedart_special_oda(pincode: str, weight: float, df: pd.DataFrame) -> float:
    global bluedart_oda_table
    if bluedart_oda_table is None: return 0.0
    row = df[df.get("pincode")==str(pincode)]
    if row.empty: return 0.0
    r = row.iloc[0]
    status = str(r.get("status","")).upper()
    if "ODA" not in status and "EDL" not in status:
        return 0.0
    dist = r.get("oda_distance")
    if pd.isna(dist): return 0.0
    dist_bands = list(bluedart_oda_table["distance_band"])
    weight_bands = [c for c in bluedart_oda_table.columns if c!="distance_band"]
    dband = select_band(float(dist), dist_bands)
    wband = select_band(float(weight), weight_bands)
    if not dband or not wband: return 0.0
    val = bluedart_oda_table.loc[bluedart_oda_table["distance_band"]==dband, wband]
    try:
        v = float(val.values[0])
        return v if v == v else 0.0
    except Exception:
        return 0.0


from pricing_engines import get_engine

def calc_cost(cfg, pincode, actual_weight, vol_weight=None, declared_value=0.0):
    df = cfg["df"]
    row = df[df.get("pincode")==str(pincode)]
    result = {
        "courier": cfg["name"], "pincode": str(pincode), "weight": float(actual_weight),
        "status": "NOT FOUND", "zone": "", "state": "", "location": "",
        "freight": 0, "fuel": 0, "insurance": 0, "oda": 0, "docket": 0, "subtotal": 0, "gst": 0, "total": 0
    }
    if row.empty:
        result["reason"] = "Pincode not found"
        return result

    r = row.iloc[0]
    result["state"]    = str(r.get("state",""))
    result["location"] = str(r.get("location",""))
    status = str(r.get("status","")).upper()
    zone   = str(r.get("zone","")).upper()
    result["status"] = status
    result["zone"]   = zone

    used_weight = max(float(actual_weight), float(vol_weight or 0))

    # Delegate to plug-in engine
    engine = get_engine(cfg["name"])
    shared = {"bluedart_special_oda": bluedart_special_oda if "bluedart" in cfg["name"].lower() else None,
              "df": df}
    comp = engine.quote(cfg, str(pincode), r, used_weight, float(declared_value or 0), shared)

    if comp.get("reason","OK") != "OK":
        result["reason"] = comp.get("reason","Unknown")
        return result

    result.update({
        "freight": round(comp["freight"],2),
        "fuel": round(comp["fuel"],2),
        "insurance": round(comp["insurance"],2),
        "oda": round(comp["oda"],2),
        "docket": round(comp["docket"],2),
        "subtotal": round(comp["subtotal"],2),
        "gst": round(comp["gst"],2),
        "total": round(comp["total"],2),
        "reason": "OK"
    })
    return result

    r = row.iloc[0]
    result["state"]    = str(r.get("state",""))
    result["location"] = str(r.get("location",""))
    status = str(r.get("status","")).upper()
    zone   = str(r.get("zone","")).upper()
    perkg  = cfg["rates"].get(zone)
    result["status"] = status
    result["zone"]   = zone
    if not perkg:
        result["reason"] = f"Rate missing for zone {zone}"
        return result

    used_weight = max(float(actual_weight), float(vol_weight or 0))
    freight = perkg * used_weight

    pct_amt  = (cfg.get("insurance_pct",0)/100.0) * float(declared_value or 0)
    flat_amt = float(cfg.get("insurance_flat",0) or 0)
    insurance = max(pct_amt, flat_amt)
    docket = float(cfg.get("docket", 0))

    oda = 0.0
    if (cfg.get("oda_type","Fixed").lower().startswith("special")) and cfg["name"].lower() == "bluedart":
        oda = bluedart_special_oda(str(pincode), used_weight, df)
    else:
        if "ODA" in status or "EDL" in status:
            oda = float(cfg.get("oda_fixed", 0))

    if cfg["name"].lower() == "bluedart":
        pre_fuel = freight + docket + insurance + oda
        fuel = pre_fuel * (cfg.get("fuel_pct",0)/100.0)
        subtotal_before_min = pre_fuel + fuel
    else:
        fuel = freight * (cfg.get("fuel_pct",0)/100.0)
        subtotal_before_min = freight + fuel + insurance + oda + docket

    subtotal = max(subtotal_before_min, float(cfg.get("min_charge",0)))
    gst = subtotal * (cfg.get("gst_pct",18)/100.0)
    total = subtotal + gst

    result.update({
        "freight": round(freight,2),
        "fuel": round(fuel,2),
        "insurance": round(insurance,2),
        "oda": round(oda,2),
        "docket": round(docket,2),
        "subtotal": round(subtotal,2),
        "gst": round(gst,2),
        "total": round(total,2),
        "reason": "OK"
    })
    return result

# ====================== Auth & Pages ======================
@app.route('/', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        if request.form.get('username') == DEFAULT_USER["username"] and request.form.get('password') == DEFAULT_USER["password"]:
            session['user'] = DEFAULT_USER["username"]; return redirect(url_for('dashboard'))
        return render_template('login.html', error="Invalid credentials")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user',None); return redirect(url_for('login'))

@app.route('/dashboard')
def dashboard():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('dashboard.html')

@app.route('/recent')
def recent_page():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('recent.html')

@app.route('/manage')
def manage_page():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('manage.html')

@app.route('/add_courier')
def add_courier_page():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('add_courier.html')

@app.route('/edit_courier/<name>')
def edit_courier_page(name):
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('edit_courier.html')

# ====================== APIs ======================

@app.route("/api/recommend", methods=["POST"])
def api_recommend():
    if 'user' not in session:
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        data = request.get_json(force=True)
        pincodes = data.get("pincodes", [])
        weights = data.get("weights", [])
        volweights = data.get("volumetric_weights", [])
        declared_value = float(data.get("declared_value", 0) or 0)
    except Exception as e:
        return jsonify({"success": False, "error": f"Invalid payload: {e}"}), 400

    results = []

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name, rates, docket, fuel_pct, insurance_pct, insurance_flat, oda_type, oda_fixed, gst_pct, min_charge, file_path FROM couriers")
    couriers = cur.fetchall()
    conn.close()

    for courier in couriers:
        (
            name, rates_json, docket, fuel_pct, insurance_pct, insurance_flat,
            oda_type, oda_fixed, gst_pct, min_charge, file_path
        ) = courier

        rates = json.loads(rates_json or "{}")
        excel_path = file_path or os.path.join(UPLOAD_DIR, f"{name}.xlsx")

        df = None
        if os.path.exists(excel_path):
            try:
                df = pd.read_excel(excel_path)
                df = normalize_columns(df)
            except Exception as e:
                print(f"Error reading {excel_path}: {e}")
                df = None

        for i, pin in enumerate(pincodes):
            w = float(weights[i]) if i < len(weights) else 0.0
            vol = float(volweights[i]) if i < len(volweights) else 0.0
            eff_weight = max(w, vol)

            zone = status = state = location = None
            oda_distance = None
            zone_rate = 0.0

            if df is not None:
                match = df[df["pincode"].astype(str) == str(pin)]
                if not match.empty:
                    row = match.iloc[0]
                    zone = row.get("zone")
                    status = str(row.get("status", "")).upper()
                    state = row.get("state")
                    location = row.get("location")
                    if status == "ODA":
                        oda_distance = float(row.get("oda_distance", 0) or 0)
                    if zone and isinstance(rates, dict):
                        zone_rate = float(rates.get(zone, 0) or 0)

            # Cost calculation
            base = zone_rate * eff_weight
            fuel = base * (float(fuel_pct) / 100)
            insurance = (base * (float(insurance_pct) / 100)) + float(insurance_flat)
            docket_chg = float(docket)
            oda_chg = (get_bluedart_oda_charge(oda_distance, eff_weight) if name.lower()=="bluedart" and status=="ODA" else (float(oda_fixed) if status=="ODA" else 0))
            subtotal = base + fuel + insurance + docket_chg + oda_chg
            gst = subtotal * (float(gst_pct) / 100)
            total = subtotal + gst

            results.append({
                "pincode": pin,
                "courier": name,
                "status": status or "OK",
                "zone": zone,
                "state": state,
                "location": location,
                "zone_rate": zone_rate,
                "oda_distance": oda_distance,
                "freight": base,
                "fuel": fuel,
                "insurance": insurance,
                "oda": oda_chg,
                "docket": docket_chg,
                "subtotal": subtotal,
                "gst": gst,
                "total": total,
                "weight": eff_weight
            })

    return jsonify({"success": True, "results": results})

@app.route('/api/recent')
def api_recent():
    if 'user' not in session: return jsonify([]), 401
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("SELECT checked_at,pincode,courier,weight,total FROM recent_searches ORDER BY id DESC LIMIT 20")
    out = [{"checked_at":r[0],"pincode":r[1],"courier":r[2],"weight":r[3],"total":r[4]} for r in cur.fetchall()]
    conn.close()
    return jsonify(out)

@app.route('/api/recent/clear', methods=['POST'])
def api_clear_recent():
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("DELETE FROM recent_searches")
    conn.commit(); conn.close()
    return jsonify({"message":"Recent history cleared successfully"})

@app.route('/api/couriers', methods=['GET'])
def api_list_couriers():
    if 'user' not in session: return jsonify([]), 401
    out=[]
    for name, cfg in couriers.items():
        out.append({
            "name": name,
            "rates": cfg.get("rates", {}),
            "fuel_pct": cfg.get("fuel_pct", 0),
            "insurance_pct": cfg.get("insurance_pct", 0),
            "insurance_flat": cfg.get("insurance_flat", 0),
            "docket": cfg.get("docket", 0),
            "oda_type": cfg.get("oda_type", "Fixed"),
            "oda_fixed": cfg.get("oda_fixed", 0),
            "gst_pct": cfg.get("gst_pct", 18),
            "min_charge": cfg.get("min_charge", 0),
            "file_path": cfg.get("file_path", "")
        })
    return jsonify(out)

@app.route('/api/courier/<name>', methods=['GET'])
def api_get_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    cfg = couriers.get(name)
    if not cfg: return jsonify({"error":"Not found"}), 404
    out = {
        "name": name,
        "rates": cfg.get("rates", {}),
        "fuel_pct": cfg.get("fuel_pct", 0),
        "insurance_pct": cfg.get("insurance_pct", 0),
        "insurance_flat": cfg.get("insurance_flat", 0),
        "docket": cfg.get("docket", 0),
        "oda_type": cfg.get("oda_type", "Fixed"),
        "oda_fixed": cfg.get("oda_fixed", 0),
        "gst_pct": cfg.get("gst_pct", 18),
        "min_charge": cfg.get("min_charge", 0),
        "file_path": cfg.get("file_path","")
    }
    return jsonify(out)

@app.route('/api/couriers/add', methods=['POST'])
def api_add_courier_fullpage():
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    name = (request.form.get('name') or '').strip()
    file = request.files.get('file')
    rates_raw = (request.form.get('rates') or '').strip()
    if not name or not file or not rates_raw:
        return jsonify({"error":"Name, Excel and Rates are required"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_DIR, filename); file.save(filepath)

    try:
        df = normalize_columns(pd.read_excel(filepath))
    except Exception as e:
        return jsonify({"error": f"Failed to read Excel: {e}"}), 400

    try:
        rates = json.loads(rates_raw)
        if not isinstance(rates, dict): raise ValueError("Rates must be a JSON object")
        rates = {str(k).strip().upper(): float(v) for k,v in rates.items()}
    except Exception as e:
        return jsonify({"error": f"Invalid rates JSON: {e}"}), 400

    cfg = {
        "name": name,
        "df": df,
        "file_path": filepath,
        "rates": rates,
        "docket": float(request.form.get('docket') or 0),
        "fuel_pct": float(request.form.get('fuel_pct') or 0),
        "insurance_pct": float(request.form.get('insurance_pct') or 0),
        "insurance_flat": float(request.form.get('insurance_flat') or 0),
        "oda_type": request.form.get('oda_type','Fixed'),
        "oda_fixed": float(request.form.get('oda_fixed') or 0),
        "gst_pct": float(request.form.get('gst_pct') or 18),
        "min_charge": float(request.form.get('min_charge') or 0),
    }
    save_courier_to_db(cfg)
    load_all_couriers_from_db()
    return jsonify({"message": f"Courier {name} added successfully"})

@app.route('/api/couriers/update/<name>', methods=['POST'])
def api_update_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    old = couriers.get(name)
    if not old: return jsonify({"error":"Courier not found"}), 404

    file = request.files.get('file')
    filepath = old.get("file_path")
    if file and file.filename:
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_DIR, filename)
        file.save(filepath)

    try:
        df = normalize_columns(pd.read_excel(filepath))
    except Exception as e:
        return jsonify({"error": f"Failed to read Excel: {e}"}), 400

    rates_raw = (request.form.get('rates') or '').strip() or json.dumps(old.get("rates", {}))
    try:
        rates = json.loads(rates_raw)
        if not isinstance(rates, dict): raise ValueError("Rates must be a JSON object")
        rates = {str(k).strip().upper(): float(v) for k,v in rates.items()}
    except Exception as e:
        return jsonify({"error": f"Invalid rates JSON: {e}"}), 400

    cfg = {
        "name": name,
        "df": df,
        "file_path": filepath,
        "rates": rates,
        "docket": float(request.form.get('docket') or old.get('docket') or 0),
        "fuel_pct": float(request.form.get('fuel_pct') or old.get('fuel_pct') or 0),
        "insurance_pct": float(request.form.get('insurance_pct') or old.get('insurance_pct') or 0),
        "insurance_flat": float(request.form.get('insurance_flat') or old.get('insurance_flat') or 0),
        "oda_type": request.form.get('oda_type') or old.get('oda_type') or 'Fixed',
        "oda_fixed": float(request.form.get('oda_fixed') or old.get('oda_fixed') or 0),
        "gst_pct": float(request.form.get('gst_pct') or old.get('gst_pct') or 18),
        "min_charge": float(request.form.get('min_charge') or old.get('min_charge') or 0),
    }

    save_courier_to_db(cfg)
    load_all_couriers_from_db()
    return jsonify({"message": f"Courier {name} updated successfully"})

@app.route('/api/couriers/delete/<name>', methods=['POST'])
def api_delete_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("SELECT file_path FROM couriers WHERE name=?", (name,))
    row = cur.fetchone()
    cur.execute("DELETE FROM couriers WHERE name=?", (name,))
    conn.commit(); conn.close()
    if row and row[0] and os.path.exists(row[0]):
        try: os.remove(row[0])
        except Exception: pass
    load_all_couriers_from_db()
    return jsonify({"message": "Deleted"})

@app.route('/api/courier/download/<name>')
def api_download_courier(name):
    if 'user' not in session: return redirect(url_for('login'))
    cfg = couriers.get(name)
    if not cfg: abort(404)
    fpath = cfg.get("file_path")
    if not fpath or not os.path.exists(fpath): abort(404)
    return send_file(fpath, as_attachment=True, download_name=os.path.basename(fpath))

# ====================== Startup ======================
if __name__ == '__main__':
    init_db()

    # Ensure legacy DBs have oda_type
    try:
        conn=sqlite3.connect(DB_PATH); cur=conn.cursor()
        cur.execute("PRAGMA table_info(couriers)"); cols=[r[1] for r in cur.fetchall()]
        if "oda_type" not in cols:
            cur.execute("ALTER TABLE couriers ADD COLUMN oda_type TEXT DEFAULT 'Fixed'")
            conn.commit()
        conn.close()
    except Exception:
        pass

    bluedart_oda_table = load_bluedart_oda_table()
    auto_seed_from_uploads()
    load_all_couriers_from_db()

    print(f"[READY] Couriers loaded: {list(couriers.keys())}")
    app.run(debug=True, port=5050)
