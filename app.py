
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_file, abort
import sqlite3, os, json, datetime, logging, textwrap
from werkzeug.utils import secure_filename
import pandas as pd

app = Flask(__name__)
app.secret_key = "gamma_secret_key_v421"

# ---------- Logging ----------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gamma")

APP_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(APP_DIR, "couriers.db")
UPLOAD_DIR= os.path.join(APP_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DEFAULT_USER = {"username": "admin", "password": "admin123"}

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _colset(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}

def db_init_migrate_and_report():
    conn = db_connect(); cur = conn.cursor()
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS couriers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            file_path TEXT,
            rates TEXT,
            docket REAL,
            fuel_pct REAL,
            fuel_basis TEXT DEFAULT 'freight',
            insurance_pct REAL,
            insurance_flat REAL,
            oda_type TEXT DEFAULT 'Fixed',
            oda_fixed REAL,
            gst_pct REAL,
            min_charge REAL,
            updated_at TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recent_searches(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT,
            pincode TEXT,
            courier TEXT,
            weight REAL,
            total REAL
        );
    """)
    cols = _colset(cur, "couriers")
    if "fuel_basis" not in cols:
        log.warning("Migrating: adding 'fuel_basis' column (default 'freight')")
        cur.execute("ALTER TABLE couriers ADD COLUMN fuel_basis TEXT DEFAULT 'freight'")
    if "updated_at" not in cols:
        log.warning("Migrating: adding 'updated_at' column")
        cur.execute("ALTER TABLE couriers ADD COLUMN updated_at TEXT")
    conn.commit()
    # Backfill
    cur.execute("UPDATE couriers SET fuel_basis='freight' WHERE fuel_basis IS NULL OR TRIM(fuel_basis)=''")
    conn.commit()
    # Report
    rows = cur.execute("""SELECT name, fuel_basis, fuel_pct, docket, gst_pct, min_charge, LENGTH(rates) AS rlen, file_path 
                          FROM couriers ORDER BY name""").fetchall()
    if not rows:
        log.error("DB READY but EMPTY: No couriers found in SQLite. Use /manage or /add_courier to add at least one.")
    else:
        log.info("DB READY: %d couriers loaded: %s", len(rows), ", ".join(r["name"] for r in rows))
        for r in rows:
            log.debug("Courier %-16s basis=%-8s fuel%%=%.2f docket=%.2f gst%%=%.2f min=%.2f rates_len=%s file=%s",
                      r["name"], r["fuel_basis"], r["fuel_pct"] or 0, r["docket"] or 0,
                      r["gst_pct"] or 0, r["min_charge"] or 0, r["rlen"], r["file_path"] or "-")
    conn.close()

def now_iso():
    return datetime.datetime.now().isoformat(timespec="seconds")

@app.route('/', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        if request.form.get('username') == DEFAULT_USER["username"] and request.form.get('password') == DEFAULT_USER["password"]:
            session['user'] = DEFAULT_USER["username"]
            return redirect(url_for('dashboard'))
        return render_template('login.html', error="Invalid credentials")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

@app.route('/dashboard')
def dashboard():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('dashboard.html')

@app.route('/manage')
def manage():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('manage.html')

@app.route('/add_courier')
def add_courier():
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('add_courier.html')

@app.route('/edit_courier/<name>')
def edit_courier(name):
    if 'user' not in session: return redirect(url_for('login'))
    return render_template('edit_courier.html', name=name)

ALLOWED_EXTS = {"xlsx","xls","csv","json"}
def allowed_file(fn: str) -> bool:
    return "." in fn and fn.rsplit(".",1)[1].lower() in ALLOWED_EXTS

# ---------- Courier APIs ----------
@app.route('/api/couriers', methods=['GET'])
def api_list_couriers():
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = db_connect(); cur = conn.cursor()
    rows = cur.execute("""
        SELECT name, file_path, docket, fuel_pct, fuel_basis, insurance_pct, insurance_flat,
               oda_type, oda_fixed, gst_pct, min_charge, updated_at, rates
        FROM couriers ORDER BY name
    """).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["rates"] = json.loads(d.get("rates") or "{}")
        except Exception:
            d["rates"] = {}
        out.append(d)
    return jsonify(out)

@app.route('/api/courier/<name>', methods=['GET'])
def api_get_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = db_connect(); cur = conn.cursor()
    row = cur.execute("SELECT * FROM couriers WHERE name=?", (name,)).fetchone()
    conn.close()
    if not row: return jsonify({"error":"Not found"}), 404
    d = dict(row)
    try:
        d["rates"] = json.loads(d.get("rates") or "{}")
    except Exception:
        d["rates"] = {}
    return jsonify(d)

@app.route('/api/couriers/add', methods=['POST'])
def api_add_courier():
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    name = (request.form.get('name') or "").strip()
    if not name: return jsonify({"error": "Missing name"}), 400
    file = request.files.get('file')

    rates_json = {}
    saved_path = None
    if file and allowed_file(file.filename):
        fname = secure_filename(file.filename)
        saved_path = os.path.join(UPLOAD_DIR, fname)
        file.save(saved_path)
        try:
            if fname.lower().endswith((".xlsx",".xls")):
                df = pd.read_excel(saved_path)
            elif fname.lower().endswith(".csv"):
                df = pd.read_csv(saved_path)
            else:
                df = None
            if df is not None:
                rates_json = json.loads(df.to_json(orient="records"))
        except Exception as e:
            log.exception("Failed to parse uploaded rates file for %s: %s", name, e)
            rates_json = {}
    else:
        try:
            rates_json = json.loads(request.form.get('rates') or "{}")
        except Exception as e:
            log.warning("Bad 'rates' JSON in form for %s: %s", name, e)
            rates_json = {}

    docket         = float(request.form.get('docket') or 0)
    fuel_pct       = float(request.form.get('fuel_pct') or 0)
    fuel_basis     = (request.form.get('fuel_basis') or "freight").strip().lower()
    insurance_pct  = float(request.form.get('insurance_pct') or 0)
    insurance_flat = float(request.form.get('insurance_flat') or 0)
    oda_type       = (request.form.get('oda_type') or "Fixed").strip()
    oda_fixed      = float(request.form.get('oda_fixed') or 0)
    gst_pct        = float(request.form.get('gst_pct') or 18)
    min_charge     = float(request.form.get('min_charge') or 0)

    conn = db_connect(); cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO couriers
        (name, file_path, rates, docket, fuel_pct, fuel_basis, insurance_pct, insurance_flat, oda_type, oda_fixed, gst_pct, min_charge, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, saved_path, json.dumps(rates_json), docket, fuel_pct, fuel_basis, insurance_pct, insurance_flat, oda_type, oda_fixed, gst_pct, min_charge, now_iso()))
    conn.commit(); conn.close()
    log.info("Courier added/updated: %s fuel_basis=%s fuel_pct=%.2f min_charge=%.2f rates_preview=%s file=%s",
             name, fuel_basis, fuel_pct, min_charge,
             textwrap.shorten(json.dumps(rates_json) or "{}", width=120), saved_path or "-")
    return jsonify({"message": f"Courier {name} added/updated."})

@app.route('/api/couriers/update/<name>', methods=['POST'])
def api_update_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = db_connect(); cur = conn.cursor()
    row = cur.execute("SELECT * FROM couriers WHERE name=?", (name,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error":"Courier not found"}), 404

    file = request.files.get('file')
    updates, values = [], []

    if file and allowed_file(file.filename):
        fname = secure_filename(file.filename)
        saved_path = os.path.join(UPLOAD_DIR, fname)
        file.save(saved_path)
        updates.append("file_path=?"); values.append(saved_path)
        try:
            if fname.lower().endswith((".xlsx",".xls")):
                df = pd.read_excel(saved_path)
            elif fname.lower().endswith(".csv"):
                df = pd.read_csv(saved_path)
            else:
                df = None
            if df is not None:
                rates_json = json.loads(df.to_json(orient="records"))
                updates.append("rates=?"); values.append(json.dumps(rates_json))
        except Exception as e:
            log.exception("Failed to parse updated rates file for %s: %s", name, e)

    field_specs = {
        "docket": float, "fuel_pct": float, "insurance_pct": float, "insurance_flat": float,
        "oda_type": str, "oda_fixed": float, "gst_pct": float, "min_charge": float,
        "fuel_basis": str
    }
    for k, caster in field_specs.items():
        if k in request.form:
            v = request.form.get(k)
            if caster is float:
                try: v = float(v or 0)
                except Exception: v = 0.0
            else:
                v = (v or "").strip()
            updates.append(f"{k}=?"); values.append(v)

    if 'rates' in request.form:
        try:
            rjson = json.loads(request.form.get('rates') or "{}")
        except Exception as e:
            log.warning("Bad 'rates' JSON on update for %s: %s", name, e); rjson = {}
        updates.append("rates=?"); values.append(json.dumps(rjson))

    if updates:
        updates.append("updated_at=?"); values.append(now_iso())
        values.append(name)
        cur.execute(f"UPDATE couriers SET {', '.join(updates)} WHERE name=?", values)
        conn.commit()

    conn.close()
    log.info("Courier updated: %s", name)
    return jsonify({"message": f"Courier {name} updated."})

@app.route('/api/couriers/delete/<name>', methods=['POST'])
def api_delete_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = db_connect(); cur = conn.cursor()
    cur.execute("DELETE FROM couriers WHERE name=?", (name,))
    conn.commit(); conn.close()
    log.info("Courier deleted: %s", name)
    return jsonify({"message": f"Courier {name} deleted."})

@app.route('/api/courier/download/<name>', methods=['GET'])
def api_download_courier(name):
    if 'user' not in session: return jsonify({"error":"Unauthorized"}), 401
    conn = db_connect(); cur = conn.cursor()
    row = cur.execute("SELECT file_path FROM couriers WHERE name=?", (name,)).fetchone()
    conn.close()
    if not row or not row["file_path"] or not os.path.exists(row["file_path"]):
        return abort(404)
    return send_file(row["file_path"], as_attachment=True)

# ---------- Helpers for Excel pincode fetch ----------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common column header variations to standard names."""
    if df is None or df.empty:
        return df
    # lower-case headers
    df = df.rename(columns={c: str(c).strip().lower() for c in df.columns})
    # common aliases
    aliases = {
        "pin": "pincode", "pin code": "pincode", "pincode": "pincode", "postal": "pincode", "zip": "pincode",
        "zone name": "zone", "zonename": "zone",
        "statename": "state",
        "loc": "location", "city": "location", "area": "location",
        "distance": "oda_distance", "dist": "oda_distance", "distance_km": "oda_distance", "oda_km": "oda_distance",
        "oda": "status"  # sometimes sheet has a column 'oda' with 'yes/no' - map to status
    }
    for old, new in list(aliases.items()):
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    # ensure expected columns exist
    for need in ["pincode","zone","state","location","status","oda_distance"]:
        if need not in df.columns:
            df[need] = None
    # normalize content
    if "pincode" in df.columns:
        df["pincode"] = df["pincode"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper().str.strip()
        df["status"] = df["status"].replace({"YES":"ODA", "Y":"ODA"})
    # numeric distance
    if "oda_distance" in df.columns:
        def _to_float(x):
            try: return float(str(x).strip())
            except: return 0.0
        df["oda_distance"] = df["oda_distance"].map(_to_float)
    return df

def fetch_pincode_row_from_excel(excel_path: str, pin: str):
    """Read Excel/CSV, normalize, and return first matching row dict (or None)."""
    if not excel_path or not os.path.exists(excel_path):
        log.warning("Excel not found for pincode fetch: %s", excel_path)
        return None
    try:
        if excel_path.lower().endswith((".xlsx",".xls")):
            df = pd.read_excel(excel_path)
        elif excel_path.lower().endswith(".csv"):
            df = pd.read_csv(excel_path)
        else:
            log.warning("Unsupported file extension for %s", excel_path); return None
        df = normalize_columns(df)
        pin_s = str(pin).strip()
        match = df[df["pincode"].astype(str) == pin_s]
        if match.empty:
            log.info("No pincode %s in file %s", pin_s, os.path.basename(excel_path))
            return None
        row = match.iloc[0].to_dict()
        log.info("Matched pin=%s in %s → zone=%s state=%s loc=%s status=%s dist=%.2f",
                 pin_s, os.path.basename(excel_path),
                 row.get("zone"), row.get("state"), row.get("location"),
                 row.get("status"), float(row.get("oda_distance") or 0))
        return row
    except Exception as e:
        log.exception("Failed reading excel for pin %s: %s", pin, e)
        return None

# ---------- ODA helper ----------
def get_bluedart_oda_charge(distance_km: float, weight_kg: float) -> float:
    ODA_MATRIX = [
        (20, 50,  [(100, 550),  (250, 990),  (500, 1100), (1000, 1375)]),
        (51, 100, [(100, 825),  (250, 1210), (500, 1375), (1000, 1650)]),
        (101,150, [(100,1100),  (250,1650),  (500,1925),  (1000,2200)]),
        (151,200, [(100,1375),  (250,1925),  (500,2200),  (1000,2475)]),
        (201,250, [(100,1650),  (250,2200),  (500,2475),  (1000,2750)]),
        (251,300, [(100,1925),  (250,2475),  (500,2750),  (1000,3025)]),
        (301,350, [(100,2200),  (250,2750),  (500,3025),  (1000,3300)]),
        (351,400, [(100,2475),  (250,3025),  (500,3300),  (1000,3575)]),
        (401,450, [(100,2750),  (250,3300),  (500,3575),  (1000,3850)]),
        (451,500, [(100,3025),  (250,3575),  (500,3850),  (1000,4125)]),
    ]
    row = next((r for r in ODA_MATRIX if distance_km <= r[0]), ODA_MATRIX[-1])
    tiers = row[2]
    for max_wt, charge in tiers:
        if weight_kg <= max_wt:
            return float(charge)
    return float(tiers[-1][1])

# ---------- Recommend ----------
@app.route('/api/recommend', methods=['POST'])
def api_recommend():
    if 'user' not in session: return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        data = request.get_json(force=True)
        pincodes = data.get("pincodes", [])
        weights = data.get("weights", [])
        volweights = data.get("volumetric_weights", [])
        declared_value = float(data.get("declared_value", 0) or 0)
    except Exception as e:
        log.exception("Bad payload to /api/recommend: %s", e)
        return jsonify({"success": False, "error": f"Invalid payload: {e}"}), 400

    # Load couriers from DB
    conn = db_connect(); cur = conn.cursor()
    rows = cur.execute("""
        SELECT name, file_path, rates, docket, fuel_pct, fuel_basis, insurance_pct, insurance_flat,
               oda_type, oda_fixed, gst_pct, min_charge
        FROM couriers
    """).fetchall()
    couriers = [dict(r) for r in rows]
    conn.close()

    if not couriers:
        log.error("No couriers in DB; returning empty results")
        return jsonify({"success": True, "results": [], "message": "No couriers configured"}), 200

    results = []
    for idx, pin in enumerate(pincodes):
        for c in couriers:
            name = c["name"]
            excel_path = c.get("file_path")

            wt = float(weights[idx] if idx < len(weights) else (weights[-1] if weights else 0))
            vol = float(volweights[idx] if idx < len(volweights) else 0)
            eff_weight = max(wt, vol) if vol else wt

            # Validate/preview rates JSON
            raw_rates = c.get("rates")
            try:
                rates = json.loads(raw_rates or "{}")
                log.debug("Rates for %s: %s", name, (raw_rates[:200] + ("..." if raw_rates and len(raw_rates)>200 else "")) if isinstance(raw_rates, str) else str(rates)[:200])
            except Exception as e:
                log.warning("Bad rates JSON for %s: %s; using {}", name, e)
                rates = {}

            # Default lookups
            state = ""; location = ""; zone = ""; zone_rate = 0.0; oda_distance = 0.0; status = "OK"

            # ---- Dynamic Excel fetch & normalization per courier
            row = fetch_pincode_row_from_excel(excel_path, pin) if excel_path else None
            if row:
                state = row.get("state") or ""
                location = row.get("location") or ""
                zone = row.get("zone") or ""
                status = (row.get("status") or "").upper()
                if status in ("YES","Y"): status = "ODA"
                oda_distance = float(row.get("oda_distance") or 0.0)
                # zone rate from dict-style rates
                if isinstance(rates, dict) and zone:
                    try:
                        zone_rate = float(rates.get(zone) or 0)
                    except Exception:
                        zone_rate = 0.0

            # Compute base freight (zone rate or generic)
            base = 0.0
            if zone and zone_rate:
                base = max(base, zone_rate * eff_weight)
            else:
                if isinstance(rates, dict) and "rate_per_kg" in rates:
                    try:
                        base = max(base, float(rates.get("rate_per_kg") or 0) * eff_weight)
                    except Exception:
                        base = base
                elif isinstance(rates, list) and rates:
                    rec0 = rates[0]
                    if isinstance(rec0, dict):
                        for key in ["rate","rate_per_kg","z_rate","price"]:
                            if key in rec0:
                                try:
                                    base = max(base, float(rec0.get(key) or 0) * eff_weight)
                                except Exception:
                                    pass
                                break

            docket = float(c["docket"] or 0)
            insurance = (declared_value * (float(c["insurance_pct"] or 0)/100.0)) + float(c["insurance_flat"] or 0)

            # ODA charge
            if c["oda_type"] == "Special" and status == "ODA":
                oda = get_bluedart_oda_charge(oda_distance, eff_weight)
            elif c["oda_type"] == "Fixed":
                oda = float(c["oda_fixed"] or 0)
            else:
                oda = 0.0

            # ---- Min-charge fallback as SUBTOTAL baseline
            min_charge = float(c["min_charge"] or 0)
            subtotal_pre_fuel = max(base + docket + insurance + oda, min_charge)

            # Fuel basis
            fuel_pct = float(c["fuel_pct"] or 0)
            basis = (c.get("fuel_basis") or "freight").lower()
            fuel_base = subtotal_pre_fuel if basis == "subtotal" else (base if base>0 else min_charge)
            fuel = fuel_base * (fuel_pct/100.0)

            subtotal_for_tax = subtotal_pre_fuel + fuel
            gst = subtotal_for_tax * (float(c["gst_pct"] or 0)/100.0)
            total = subtotal_for_tax + gst

            log.debug(("RECO pin=%s courier=%s weight=%.2f zone=%s zone_rate=%.2f base=%.2f docket=%.2f "
                       "insurance=%.2f oda=%.2f min=%.2f basis=%s fuel=%.2f gst=%.2f total=%.2f"),
                      pin, name, eff_weight, zone, zone_rate, base, docket, insurance, oda, min_charge, basis, fuel, gst, total)

            results.append({
                "pincode": str(pin),
                "weight": eff_weight,
                "courier": name,
                "status": status,
                "zone": zone,
                "zone_rate": zone_rate,
                "oda_distance": oda_distance,
                "state": state,
                "location": location,
                "freight": base if base>0 else min_charge,  # show min when used
                "fuel": fuel,
                "insurance": insurance,
                "oda": oda,
                "docket": docket,
                "subtotal": subtotal_for_tax - gst,  # before GST (includes fuel)
                "gst": gst,
                "total": total
            })

            # record recent
            conn = db_connect(); cur = conn.cursor()
            cur.execute("INSERT INTO recent_searches(checked_at, pincode, courier, weight, total) VALUES (?,?,?,?,?)",
                        (now_iso(), str(pin), name, eff_weight, total))
            conn.commit(); conn.close()

    return jsonify({"success": True, "results": results})

if __name__ == "__main__":
    db_init_migrate_and_report()
    log.info("Gamma Courier Suite v4 (SQLite + Fuel Basis + Excel Pincode Fetch) http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
