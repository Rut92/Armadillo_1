# armadillo_app.py — Clean full script (syntax-checked)
# ---------------------------------------------------------------
# Features
# - Landing page (name + tagline), tasteful backgrounds
# - Auth (login / signup / forgot), role-based routing (client/admin)
# - Client dashboards (Procurement, Inventory, Logistics) with KPI cards,
#   inline slicers under the title, charts, and print button
# - Admin: Dashboards (view any client) & Backend with:
#     1) Create/Edit Clients (+ assign users to client)
#     2) Add/Edit/Remove Data (upload CSV/XLSX, clean, inline edit, save, load existing)
#     3) Select KPIs (predefined + custom builder per domain)
# - SQLite persistence; bcrypt password hashing
# - Client view hides tables (toggle to view); Admin sees table by default
# - Uses only st.query_params (no experimental APIs)
# ---------------------------------------------------------------
# Run: pip install streamlit pandas plotly bcrypt SQLAlchemy openpyxl xlsxwriter
# Start: streamlit run armadillo_app.py

import os
import json
from datetime import datetime, date
import importlib
import smtplib
from email.message import EmailMessage
import secrets
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.lib.units import cm

import bcrypt
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text

# ----------------------------- Dependency doctor (optional) -----------------------------
REQUIRED = [
    ("streamlit", None),
    ("pandas", None),
    ("plotly", "express"),
    ("sqlalchemy", None),
    ("bcrypt", None),
    ("openpyxl", None),
    ("xlsxwriter", None),
]
_missing = []
for pkg, sub in REQUIRED:
    try:
        importlib.import_module(f"{pkg}.{sub}" if sub else pkg)
    except Exception as e:  # pragma: no cover
        _missing.append((pkg, str(e)))
if _missing:
    st.error("🚨 Missing dependencies detected:")
    for pkg, err in _missing:
        st.write(f"- **{pkg}** → `{err}`")
    st.stop()

# ----------------------------- App Config -----------------------------
st.set_page_config(page_title="Armadillo", page_icon="🦔", layout="wide")

APP_NAME = "Armadillo"
TAGLINE = "Strategic Insights. Operational Clarity."
DB_PATH = os.environ.get("ARMADILLO_DB", "armadillo.db")
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

DEFAULT_KPIS = {
    "procurement": ["Supplier OTD %", "PPV $", "PO Cycle Time (days)"],
    "inventory": ["Inventory Turns", "DOH", "Obsolete %"],
    "logistics": ["Perfect Order %", "Freight/Unit", "On-Time Ship %"],
}

# ----------------------------- DB Helpers -----------------------------

def init_db() -> None:
    with engine.begin() as con:
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash BLOB NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','client')),
                client_id INTEGER,
                created_at TEXT
            )
            """
        ))
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                background_notes TEXT,
                created_at TEXT
            )
            """
        ))
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                domain TEXT NOT NULL,           -- procurement|inventory|logistics
                data_json TEXT,                 -- records as JSON
                updated_at TEXT
            )
            """
        ))
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kpi_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                domain TEXT NOT NULL,
                kpis_json TEXT,                 -- list[str|dict]
                updated_at TEXT
            )
            """
        ))
    with engine.begin() as con:
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS pw_otps (
                email TEXT NOT NULL,
                code TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
            """
        ))
    with engine.begin() as con:
        admin = con.execute(text("SELECT 1 FROM users WHERE role='admin' LIMIT 1")).fetchone()
        if not admin:
            pw = bcrypt.hashpw(b"admin123", bcrypt.gensalt())
            con.execute(text(
                "INSERT INTO users(email,password_hash,role,created_at) VALUES(:e,:p,'admin',:c)"
            ), {"e": "admin@armadillo.io", "p": pw, "c": datetime.utcnow().isoformat()})

# ----------------------------- Auth Helpers -----------------------------

def hash_pw(pw: str) -> bytes:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt())

def check_pw(pw: str, hashed: bytes) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed)
    except Exception:
        return False

def get_user_by_email(email: str):
    with engine.begin() as con:
        row = con.execute(text("SELECT * FROM users WHERE email=:e"), {"e": email}).mappings().fetchone()
        return dict(row) if row else None

def create_user(email: str, pw: str, role: str = "client", client_id: int | None = None) -> None:
    with engine.begin() as con:
        con.execute(text(
            "INSERT INTO users(email,password_hash,role,client_id,created_at) VALUES(:e,:p,:r,:cid,:c)"
        ), {"e": email, "p": hash_pw(pw), "r": role, "cid": client_id, "c": datetime.utcnow().isoformat()})

def upsert_client(name: str, background_notes: str = "") -> int | None:
    if not name:
        return None
    with engine.begin() as con:
        row = con.execute(text("SELECT id FROM clients WHERE name=:n"), {"n": name}).fetchone()
        if row:
            con.execute(text("UPDATE clients SET background_notes=:b WHERE id=:i"), {"b": background_notes, "i": row[0]})
            return row[0]
        con.execute(text("INSERT INTO clients(name,background_notes,created_at) VALUES(:n,:b,:c)"),
                    {"n": name, "b": background_notes, "c": datetime.utcnow().isoformat()})
        rid = con.execute(text("SELECT id FROM clients WHERE name=:n"), {"n": name}).fetchone()[0]
        return rid

def list_clients():
    with engine.begin() as con:
        rows = con.execute(text("SELECT id,name FROM clients ORDER BY name")).fetchall()
        return [(r[0], r[1]) for r in rows]

def save_dataset(client_id: int, domain: str, df: pd.DataFrame) -> None:
    # Convert datetime columns to ISO strings for JSON
    for c in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[c] = df[c].astype(str)
    recs = df.to_dict(orient="records")
    with engine.begin() as con:
        row = con.execute(text("SELECT id FROM datasets WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if row:
            con.execute(text("UPDATE datasets SET data_json=:j, updated_at=:u WHERE id=:i"),
                        {"j": json.dumps(recs), "u": datetime.utcnow().isoformat(), "i": row[0]})
        else:
            con.execute(text("INSERT INTO datasets(client_id,domain,data_json,updated_at) VALUES(:c,:d,:j,:u)"),
                        {"c": client_id, "d": domain, "j": json.dumps(recs), "u": datetime.utcnow().isoformat()})

def load_dataset(client_id: int, domain: str) -> pd.DataFrame | None:
    with engine.begin() as con:
        row = con.execute(text("SELECT data_json FROM datasets WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if not row or not row[0]:
            return None
        return pd.DataFrame(json.loads(row[0]))

def save_kpis(client_id: int, domain: str, kpis: list) -> None:
    with engine.begin() as con:
        row = con.execute(text("SELECT id FROM kpi_configs WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if row:
            con.execute(text("UPDATE kpi_configs SET kpis_json=:j, updated_at=:u WHERE id=:i"),
                        {"j": json.dumps(kpis), "u": datetime.utcnow().isoformat(), "i": row[0]})
        else:
            con.execute(text("INSERT INTO kpi_configs(client_id,domain,kpis_json,updated_at) VALUES(:c,:d,:j,:u)"),
                        {"c": client_id, "d": domain, "j": json.dumps(kpis), "u": datetime.utcnow().isoformat()})

def load_kpis(client_id: int, domain: str) -> list:
    with engine.begin() as con:
        row = con.execute(text("SELECT kpis_json FROM kpi_configs WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if not row or not row[0]:
            return DEFAULT_KPIS.get(domain, [])
        return list(json.loads(row[0]))

# ----------------------------- OTP Helpers -----------------------------

def send_email_otp(to_email: str, code: str) -> bool:
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pw   = os.environ.get("SMTP_PASS")
    sender = os.environ.get("SMTP_FROM", user or "no-reply@armadillo.local")
    if not host or not user or not pw:
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = "Armadillo Password Reset Code"
        msg["From"] = sender
        msg["To"] = to_email
        msg.set_content(f"Your Armadillo reset code is: {code}
This code expires in 10 minutes.")
        with smtplib.SMTP(host, port) as s:
            s.starttls()
            s.login(user, pw)
            s.send_message(msg)
        return True
    except Exception:
        return False

def create_otp(email: str) -> str:
    code = f"{secrets.randbelow(1000000):06d}"
    expires = datetime.utcnow().timestamp() + 10 * 60
    with engine.begin() as con:
        con.execute(text("DELETE FROM pw_otps WHERE email=:e"), {"e": email})
        con.execute(text("INSERT INTO pw_otps(email, code, expires_at) VALUES(:e,:c,:x)"),
                    {"e": email, "c": code, "x": str(expires)})
    return code

def verify_otp(email: str, code: str) -> bool:
    with engine.begin() as con:
        row = con.execute(text("SELECT code, expires_at FROM pw_otps WHERE email=:e"), {"e": email}).fetchone()
    if not row:
        return False
    saved_code, exp = row
    if saved_code != code:
        return False
    try:
        return float(exp) >= datetime.utcnow().timestamp()
    except Exception:
        return False

# ----------------------------- UI Utilities -----------------------------

def set_bg(style_key: str) -> None:
    styles = {
        "landing": "linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0b1020 100%)",
        "login":   "linear-gradient(135deg, #0b1020 0%, #1b2a41 50%, #0b1020 100%)",
        "client":  "linear-gradient(135deg, #0b1f2a, #0d3b66)",
        "admin":   "linear-gradient(135deg, #1f2937, #111827)",
    }
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: {styles.get(style_key, styles['landing'])};
            color: #e5e7eb;
        }}
        .glass {{
            background: rgba(17, 24, 39, 0.5);
            backdrop-filter: blur(8px);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 16px;
            padding: 24px;
        }}
        .hero-title {{ font-size: 64px; font-weight: 800; letter-spacing: 0.5px; }}
        .hero-sub {{ font-size: 22px; opacity: 0.9; }}
        @media print {{
            header, footer, .stSidebar {{ display: none !important; }}
            .stApp {{ background: white !important; color: #000 !important; }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

def logout_button() -> None:
    if st.session_state.get("auth", {}).get("logged_in"):
        col = st.columns([6, 1])[1]
        with col:
            if st.button("🚪 Log out", key="logout_btn"):
                st.session_state.clear()
                st.query_params["page"] = "landing"
                st.rerun()

def nav(page: str) -> None:
    st.query_params["page"] = page
    st.session_state["page"] = page

# ----------------------------- Filters / KPIs -----------------------------

def reset_filters(key_prefix: str):
    keys = [f"{key_prefix}_date", f"{key_prefix}_supplier", f"{key_prefix}_sku", f"{key_prefix}_mode"]
    for k in keys:
        if k in st.session_state:
            del st.session_state[k]
    st.rerun()

def slicers_inline(df: pd.DataFrame, key_prefix: str = "global") -> dict:
    st.markdown("#### 🔎 Filters")
    row = st.columns([2, 2, 2, 2])
    filters: dict = {}
    i = 0
    if "received_date" in df.columns:
        dmin = pd.to_datetime(df["received_date"], errors="coerce").min()
        dmax = pd.to_datetime(df["received_date"], errors="coerce").max()
        with row[i]:
            d_from, d_to = st.date_input(
                "Date range",
                value=(dmin.date() if pd.notna(dmin) else date.today(),
                       dmax.date() if pd.notna(dmax) else date.today()),
                key=f"{key_prefix}_date",
            )
        filters["date"] = (pd.to_datetime(d_from), pd.to_datetime(d_to))
        i += 1
    if "supplier" in df.columns:
        sups = sorted([s for s in df["supplier"].dropna().unique().tolist()])
        with row[i % 4]:
            sel = st.multiselect("Supplier", sups, default=sups, key=f"{key_prefix}_supplier")
        filters["supplier"] = sel
        i += 1
    if "sku" in df.columns:
        skus = sorted([s for s in df["sku"].dropna().unique().tolist()])
        with row[i % 4]:
            sel2 = st.multiselect("SKU", skus, default=skus, key=f"{key_prefix}_sku")
        filters["sku"] = sel2
        i += 1
    if "mode" in df.columns:
        modes = sorted(df["mode"].dropna().unique().tolist())
        with row[i % 4]:
            sel3 = st.multiselect("Mode", modes, default=modes, key=f"{key_prefix}_mode")
        filters["mode"] = sel3

    rr = st.columns([3,1,1,1])
    with rr[1]:
        if st.button("↺ Reset Filters", key=f"{key_prefix}_reset"):
            reset_filters(key_prefix)
    return filters

def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    ctx = df.copy()
    if "date" in filters and "received_date" in ctx.columns:
        d_from, d_to = filters["date"]
        ctx = ctx[(pd.to_datetime(ctx["received_date"], errors="coerce") >= d_from) &
                  (pd.to_datetime(ctx["received_date"], errors="coerce") <= d_to)]
    for col in ["supplier", "sku", "mode"]:
        if col in filters and col in ctx.columns and filters.get(col):
            ctx = ctx[ctx[col].isin(filters[col])]
    return ctx

def kpi_cards(domain: str, df: pd.DataFrame, kpis: list) -> None:
    c1, c2, c3 = st.columns(3)

    def cycle_time_days(_df: pd.DataFrame):
        if {"order_date", "received_date"} <= set(_df.columns):
            d = pd.to_datetime(_df["received_date"]) - pd.to_datetime(_df["order_date"])
            return float(d.dt.days.mean()) if len(_df) else 0
        if {"promised_date", "received_date"} <= set(_df.columns):
            d = pd.to_datetime(_df["received_date"]) - pd.to_datetime(_df["promised_date"])
            return float(d.dt.days.mean()) if len(_df) else 0
        return None

    vals: list[tuple[str, str]] = []
    for k in (kpis or [])[:3]:
        if isinstance(k, dict):
            col = k.get("column"); agg = k.get("agg", "sum"); name = k.get("name", f"{agg} {col}")
            if col in df.columns:
                series = pd.to_numeric(df[col], errors="coerce") if pd.api.types.is_numeric_dtype(df[col]) else df[col]
                val = getattr(series, agg)() if hasattr(series, agg) else None
                vals.append((name, f"{val:,.2f}" if isinstance(val, (int, float)) else str(val)))
            else:
                vals.append((name, "–"))
        else:
            name = k
            low = k.lower()
            if "cycle time" in low:
                ct = cycle_time_days(df)
                vals.append((name, f"{ct:.1f} days" if ct is not None else "–"))
            elif "ppv" in low and "ppv_amt" in df.columns:
                vals.append((name, f"${df['ppv_amt'].sum():,.0f}"))
            elif "otd" in low and "on_time" in df.columns:
                vals.append((name, f"{df['on_time'].mean():.1%}"))
            elif "freight" in low and {"freight_cost", "weight_kg"} <= set(df.columns):
                per = df["freight_cost"].sum() / df["weight_kg"].sum() if df["weight_kg"].sum() else 0
                vals.append((name, f"${per:,.2f}/kg"))
            elif "closing" in low and "closing_qty" in df.columns:
                vals.append((name, f"{df['closing_qty'].sum():,.0f}"))
            else:
                vals.append((name, "–"))

    for i, (n, v) in enumerate(vals + [("", "")] * (3 - len(vals))):
        [c1, c2, c3][i].metric(n if n else "", v if v else "–")

# ----------------------------- Pages -----------------------------

def page_landing() -> None:
    set_bg("landing")
    logout_button()

    top = st.columns([4, 1])
    with top[0]:
        st.markdown(f"<div class='hero-title'>{APP_NAME}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='hero-sub'>{TAGLINE}</div>", unsafe_allow_html=True)
        st.markdown(
            """
            <div class='glass'>
            <p>Welcome to <strong>Armadillo</strong> — a strategic consulting platform for SMBs.
            Build Power BI–style dashboards for <em>Procurement</em>, <em>Inventory</em>, and <em>Logistics</em>,
            complete with slicers, printable layouts, and an admin backend to onboard clients and clean data.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with top[1]:
        st.write("")
        st.write("")
        if st.button("🔐 Login", key="landing_login", use_container_width=True):
            nav("login"); st.rerun()


def page_login() -> None:
    set_bg("login")
    logout_button()

    st.markdown("<h2>Login to Armadillo</h2>", unsafe_allow_html=True)
    tab_login, tab_signup, tab_forgot = st.tabs(["Login", "Sign up", "Forgot password"])

    with tab_login:
        email = st.text_input("Email", key="login_email")
        pw = st.text_input("Password", type="password", key="login_pw")
        if st.button("Login", key="login_btn"):
            user = get_user_by_email(email)
            if not user:
                st.error("No user found with that email.")
            elif check_pw(pw, user["password_hash"]):
                st.session_state["auth"] = {"logged_in": True, "user": user}
                nav("admin_home" if user["role"] == "admin" else "client_home")
                st.rerun()
            else:
                st.error("Incorrect password.")

    with tab_signup:
        st.info("If the email does not exist, create an account (client by default). Admins can later upgrade roles.")
        email_s = st.text_input("Email (new)", key="signup_email")
        pw1 = st.text_input("Create password", type="password", key="signup_pw1")
        pw2 = st.text_input("Confirm password", type="password", key="signup_pw2")
        client_name = st.text_input("Client/Company Name (optional for new client)", key="signup_client")
        if st.button("Create account", key="signup_btn"):
            if pw1 != pw2 or not pw1:
                st.error("Passwords do not match or are empty.")
            elif get_user_by_email(email_s):
                st.error("Email already exists. Try login.")
            else:
                cid = upsert_client(client_name) if client_name else None
                create_user(email_s, pw1, role="client", client_id=cid)
                st.success("Account created. You can now login.")

    with tab_forgot:
        st.info("Enter your email to receive a one-time code. In development, the code will be shown if SMTP isn't set.")
        f_email = st.text_input("Registered Email", key="forgot_email")
        col_a, col_b = st.columns([1,1])
        with col_a:
            if st.button("Send Code", key="forgot_send"):
                user = get_user_by_email(f_email)
                if not user:
                    st.error("No user with this email.")
                else:
                    code = create_otp(f_email)
                    sent = send_email_otp(f_email, code)
                    if sent:
                        st.success("Code sent to your email.")
                    else:
                        st.warning(f"SMTP not configured. Use this code (dev): **{code}**")
        st.divider()
        st.write("Reset using the code:")
        otp_code = st.text_input("6-digit code", key="forgot_code")
        new_pw1 = st.text_input("New password", type="password", key="forgot_pw1")
        new_pw2 = st.text_input("Confirm new password", type="password", key="forgot_pw2")
        if st.button("Reset password", key="forgot_btn"):
            user = get_user_by_email(f_email)
            if not user:
                st.error("No user with this email.")
            elif not otp_code or not verify_otp(f_email, otp_code):
                st.error("Invalid or expired code.")
            elif new_pw1 != new_pw2 or not new_pw1:
                st.error("Passwords don't match or are empty.")
            else:
                with engine.begin() as con:
                    con.execute(text("UPDATE users SET password_hash=:p WHERE id=:i"), {"p": hash_pw(new_pw1), "i": user["id"]})
                    con.execute(text("DELETE FROM pw_otps WHERE email=:e"), {"e": f_email})
                st.success("Password updated. Please login.")

# ----------------------------- PDF Export Helper -----------------------------

def export_pdf_for_dashboard(client_id: int, domain: str, ctx: pd.DataFrame, kpis: list) -> str:
    fig = None
    img_path = None
    try:
        if domain == "procurement" and {"received_date","on_time"} <= set(ctx.columns):
            tmp = ctx.copy()
            tmp["month"] = pd.to_datetime(tmp["received_date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
            df_plot = tmp.groupby("month", as_index=False)["on_time"].mean()
            fig = px.line(df_plot, x="month", y="on_time", title="OTD by Month", markers=True)
        elif domain == "inventory" and {"month","closing_qty"} <= set(ctx.columns):
            df_plot = ctx.groupby("month", as_index=False)["closing_qty"].sum()
            fig = px.line(df_plot, x="month", y="closing_qty", title="Closing Qty Trend", markers=True)
        elif domain == "logistics" and {"dispatch_date","freight_cost"} <= set(ctx.columns):
            tmp = ctx.copy()
            tmp["month"] = pd.to_datetime(tmp["dispatch_date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
            df_plot = tmp.groupby("month", as_index=False)["freight_cost"].mean()
            fig = px.line(df_plot, x="month", y="freight_cost", title="Freight Cost Trend", markers=True)
        if fig is not None:
            img_path = f"/tmp/{domain}_{client_id}_chart.png"
            fig.write_image(img_path, scale=2)  # needs kaleido
    except Exception:
        img_path = None

    pdf_path = f"/tmp/armadillo_{domain}_{client_id}.pdf"
    c = pdfcanvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    y = height - 2*cm

    c.setFont("Helvetica-Bold", 16)
    c.drawString(2*cm, y, f"Armadillo — {domain.title()} Dashboard")
    y -= 1*cm

    c.setFont("Helvetica", 12)
    kpi_lines = []
    if "on_time" in ctx.columns and any("otd" in str(k).lower() for k in kpis):
        kpi_lines.append(f"Supplier OTD: {ctx['on_time'].mean():.1%}")
    if "ppv_amt" in ctx.columns and any("ppv" in str(k).lower() for k in kpis):
        kpi_lines.append(f"PPV: ${ctx['ppv_amt'].sum():,.0f}")
    if {"freight_cost","weight_kg"} <= set(ctx.columns) and any("freight" in str(k).lower() for k in kpis):
        per = ctx["freight_cost"].sum() / ctx["weight_kg"].sum() if ctx["weight_kg"].sum() else 0
        kpi_lines.append(f"Freight/Unit: ${per:,.2f}/kg")
    if "closing_qty" in ctx.columns and any("closing" in str(k).lower() for k in kpis):
        kpi_lines.append(f"Total Closing Qty: {ctx['closing_qty'].sum():,.0f}")

    for line in (kpi_lines[:3] or [""]):
        c.drawString(2*cm, y, line)
        y -= 0.7*cm

    if img_path and os.path.exists(img_path):
        img_w = width - 4*cm
        img_h = 9*cm
        c.drawImage(img_path, 2*cm, y - img_h, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
        y -= img_h + 1*cm
    c.showPage()
    c.save()
    return pdf_path

# ----------------------------- Dashboards -----------------------------

def dashboard_section(title: str, client_id: int, domain: str) -> None:
    st.subheader(title)
    df = load_dataset(client_id, domain)
    if df is None or df.empty:
        st.info("Dashboard is being prepared. Data not available yet.")
        return

    # helper cols for procurement
    if "ppv_amt" not in df.columns and {"std_cost", "act_cost", "qty"} <= set(df.columns):
        df["ppv_amt"] = (
            pd.to_numeric(df["act_cost"], errors="coerce") - pd.to_numeric(df["std_cost"], errors="coerce")
        ) * pd.to_numeric(df["qty"], errors="coerce")
    if "on_time" not in df.columns and {"received_date", "promised_date"} <= set(df.columns):
        rd = pd.to_datetime(df["received_date"], errors="coerce")
        pdm = pd.to_datetime(df["promised_date"], errors="coerce")
        df["on_time"] = (rd <= pdm).astype(int)

    kpis = load_kpis(client_id, domain)

    # Print button
    if st.button("🖨️ Print Dashboard", key=f"print_{domain}_{client_id}"):
        components.html("<script>window.print()</script>", height=0)

    # Inline filters
    filters = slicers_inline(df, key_prefix=f"{domain}_{client_id}")
    ctx = apply_filters(df, filters)

    kpi_cards(domain, ctx, kpis)

    # Export PDF button
    pdf_col = st.columns([1,5])[0]
    with pdf_col:
        if st.button("⬇️ Export to PDF", key=f"export_pdf_{domain}_{client_id}"):
            try:
                pdf_path = export_pdf_for_dashboard(client_id, domain, ctx, kpis)
                with open(pdf_path, "rb") as f:
                    st.download_button("Download PDF", f, file_name=os.path.basename(pdf_path), mime="application/pdf", key=f"dl_pdf_{domain}_{client_id}")
            except Exception as e:
                st.error(f"Could not create PDF: {e}")

    c1, c2 = st.columns(2)

    if domain == "procurement":
        with c1:
            if {"received_date", "on_time"} <= set(ctx.columns):
                ctx["month"] = pd.to_datetime(ctx["received_date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
                chart_df = ctx.groupby("month", as_index=False).agg(otd=("on_time", "mean"))
                st.plotly_chart(px.line(chart_df, x="month", y="otd", markers=True, title="On-Time Delivery by Month"), use_container_width=True)
            else:
                st.info("Add received_date & on_time columns for OTD chart.")
        with c2:
            if {"supplier", "ppv_amt"} <= set(ctx.columns):
                top = ctx.groupby("supplier", as_index=False)["ppv_amt"].sum().sort_values("ppv_amt", ascending=False)
                st.plotly_chart(px.bar(top.head(10), x="supplier", y="ppv_amt", title="Top Suppliers by PPV"), use_container_width=True)
            else:
                st.info("Add supplier & ppv_amt columns for PPV chart.")

    elif domain == "inventory":
        with c1:
            if "month" in ctx.columns and "closing_qty" in ctx.columns:
                df_plot = ctx.groupby("month", as_index=False)["closing_qty"].sum()
                st.plotly_chart(px.line(df_plot, x="month", y="closing_qty", title="Closing Quantity Trend", markers=True), use_container_width=True)
            else:
                st.info("Add month & closing_qty columns for trend chart.")
        with c2:
            if "category" in ctx.columns and "closing_qty" in ctx.columns:
                st.plotly_chart(px.pie(ctx, names="category", values="closing_qty", title="Category Mix"), use_container_width=True)
            elif "warehouse" in ctx.columns and "closing_qty" in ctx.columns:
                wh = ctx.groupby("warehouse", as_index=False)["closing_qty"].sum()
                st.plotly_chart(px.bar(wh, x="warehouse", y="closing_qty", title="Warehouse Stock"), use_container_width=True)
            else:
                st.info("Add category or warehouse columns for mix chart.")

    elif domain == "logistics":
        with c1:
            if {"dispatch_date", "freight_cost"} <= set(ctx.columns):
                ctx["month"] = pd.to_datetime(ctx["dispatch_date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
                df_plot = ctx.groupby("month", as_index=False)["freight_cost"].mean()
                st.plotly_chart(px.line(df_plot, x="month", y="freight_cost", title="Freight Cost Trend", markers=True), use_container_width=True)
            else:
                st.info("Add dispatch_date & freight_cost columns for trend chart.")
        with c2:
            if "mode" in ctx.columns:
                st.plotly_chart(px.pie(ctx, names="mode", title="Mode Split (Air/Sea/Ground)"), use_container_width=True)
            elif "carrier" in ctx.columns:
                perf = ctx.groupby("carrier", as_index=False)[["damage_flag", "complete_flag"]].mean(numeric_only=True)
                st.plotly_chart(px.bar(perf, x="carrier", y="complete_flag", title="Carrier Perfect Delivery %"), use_container_width=True)
            else:
                st.info("Add mode or carrier columns for logistics chart.")

    # Data table visibility
    user = st.session_state.get("auth", {}).get("user")
    is_admin = user and user.get("role") == "admin"
    if is_admin:
        st.markdown("### Detail Table (Filtered)")
        st.dataframe(ctx, use_container_width=True)
    else:
        if st.button("View Data Table", key=f"view_table_{domain}_{client_id}"):
            st.session_state[f"show_table_{domain}_{client_id}"] = True
        if st.session_state.get(f"show_table_{domain}_{client_id}"):
            with st.expander("Data Table", expanded=False):
                st.dataframe(ctx, use_container_width=True)

# ----------------------------- Client/Admin -----------------------------

def page_client_home() -> None:
    set_bg("client")
    logout_button()

    user = st.session_state.get("auth", {}).get("user")
    if not user:
        st.warning("Please login.")
        nav("login"); st.rerun()
    cid = user.get("client_id")
    if not cid:
        st.info("No client linked to this account yet.")
        return

    st.markdown("## Client Dashboard")
    tabs = st.tabs(["Procurement", "Inventory", "Logistics"])
    with tabs[0]:
        dashboard_section("Procurement Dashboard", cid, "procurement")
    with tabs[1]:
        dashboard_section("Inventory Dashboard", cid, "inventory")
    with tabs[2]:
        dashboard_section("Logistics Dashboard", cid, "logistics")


def admin_dashboards() -> None:
    st.markdown("### View Client Dashboards")
    clients = list_clients()
    if not clients:
        st.info("No clients yet.")
        return
    cid = st.selectbox("Select Client", options=[c[0] for c in clients], format_func=lambda x: dict(clients).get(x), key="admin_dash_client_select")
    tabs = st.tabs(["Procurement", "Inventory", "Logistics"])
    with tabs[0]:
        dashboard_section("Procurement Dashboard", cid, "procurement")
    with tabs[1]:
        dashboard_section("Inventory Dashboard", cid, "inventory")
    with tabs[2]:
        dashboard_section("Logistics Dashboard", cid, "logistics")


def admin_backend() -> None:
    st.markdown("### Backend")
    t1, t2, t3 = st.tabs(["1) Create/Edit Clients", "2) Add/Edit/Remove Data", "3) Select KPIs"])

    # --- Step 1: Create/Edit Clients ---
    with t1:
        st.subheader("Create or Edit Client")
        c_left, c_right = st.columns(2)
        with c_left:
            cname = st.text_input("Client Name", key="bk_client_name")
            cnotes = st.text_area("Background notes (optional)", key="bk_client_notes")
            if st.button("Save Client", key="save_client_main"):
                cid = upsert_client(cname, cnotes)
                if cid:
                    st.success(f"Saved client '{cname}' (id={cid}). Redirecting to Step 2…")
                    st.session_state["last_client_id"] = cid
                    st.query_params.update({"page": "admin_home", "step": "data", "cid": str(cid)})
                    st.rerun()
                else:
                    st.error("Please enter a valid client name.")
        with c_right:
            st.markdown("**Existing Clients**")
            cl = list_clients()
            if cl:
                st.table(pd.DataFrame(cl, columns=["ID", "Name"]))
            else:
                st.info("No clients yet.")

        st.markdown("#### Assign Users to Client")
        clients_list = list_clients()
        with engine.begin() as con:
            user_rows = con.execute(text("SELECT id, email, role, client_id FROM users ORDER BY email")).fetchall()
        a1, a2, a3 = st.columns([2, 2, 1])
        with a1:
            sel_user = st.selectbox("User", options=[u[0] for u in user_rows],
                                    format_func=lambda x: next((r[1] for r in user_rows if r[0] == x), "—"), key="assign_user")
        with a2:
            sel_client = st.selectbox("Client", options=[c[0] for c in clients_list],
                                      format_func=lambda x: dict(clients_list).get(x, "—"), key="assign_client")
        with a3:
            if st.button("Assign", key="assign_user_to_client"):
                with engine.begin() as con:
                    con.execute(text("UPDATE users SET client_id=:cid WHERE id=:uid"), {"cid": sel_client, "uid": sel_user})
                st.success("User assigned to client.")

    # --- Step 2: Add/Edit/Remove Data ---
    with t2:
        st.subheader("Upload / Clean / Edit Data")
        clients = list_clients()
        cid2 = st.selectbox("Client", options=[c[0] for c in clients] if clients else [None],
                            format_func=lambda x: dict(clients).get(x, "—") if x else "—",
                            index=0 if clients else 0, key="bk_data_client")
        domain = st.selectbox("Domain", ["procurement", "inventory", "logistics"], key="bk_data_domain")
        up = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx", "xls"], key="bk_data_uploader")

        if up:
            df = pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)
            df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
            for c in ["received_date", "promised_date", "eta", "date", "dispatch_date", "delivery_date", "month"]:
                if c in df.columns and c != "month":
                    df[c] = pd.to_datetime(df[c], errors='coerce')
            for c in ["qty", "quantity", "act_cost", "std_cost", "price", "closing_qty", "opening_qty", "receipts", "issues", "freight_cost", "weight_kg"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            issues = df[df.isna().any(axis=1)]
            if not issues.empty:
                st.warning("⚠️ Some rows have missing/invalid values. You can edit below.")
                st.dataframe(issues, use_container_width=True)

            st.markdown("#### Review / Edit Data")
            edited = st.data_editor(df, use_container_width=True, num_rows="dynamic", key=f"bk_editor_{cid2}_{domain}")

            if st.button("💾 Save Cleaned Data", key=f"save_data_{cid2}_{domain}"):
                if not cid2:
                    st.error("Select a client first.")
                else:
                    save_dataset(cid2, domain, edited)
                    st.success("Data saved.")
                    st.info("Proceed to Step 3 to select KPIs.")
                    st.query_params.update({"page": "admin_home", "step": "kpis", "cid": str(cid2), "domain": domain})
        else:
            st.info("Upload a CSV/Excel to begin cleaning.")

        if st.button("Load existing data", key=f"load_existing_{cid2}_{domain}"):
            existing = load_dataset(cid2, domain)
            if existing is None or existing.empty:
                st.info("No saved data for this client/domain yet.")
            else:
                st.markdown("#### Edit Saved Data")
                edited2 = st.data_editor(existing, use_container_width=True, num_rows="dynamic", key=f"bk_editor_existing_{cid2}_{domain}")
                if st.button("💾 Save Edited Data", key=f"save_existing_{cid2}_{domain}"):
                    save_dataset(cid2, domain, edited2)
                    st.success("Existing data updated.")

    # --- Step 3: Select KPIs ---
    with t3:
        st.subheader("Select KPIs per Tab")
        clients = list_clients()
        cid3 = st.selectbox("Client", options=[c[0] for c in clients] if clients else [None],
                            format_func=lambda x: dict(clients).get(x, "—") if x else "—",
                            key="kpi_client")
        for dmn in ["procurement", "inventory", "logistics"]:
            with st.expander(f"KPIs for {dmn.title()}"):
                defaults = load_kpis(cid3, dmn) if cid3 else DEFAULT_KPIS[dmn]
                base = [k for k in defaults if isinstance(k, str)]
                custom_existing = [k for k in defaults if isinstance(k, dict)]
                chosen = st.multiselect("Choose KPIs", options=DEFAULT_KPIS[dmn], default=base, key=f"kpi_{dmn}")

                st.markdown("**Add Custom KPI**")
                data = load_dataset(cid3, dmn) if cid3 else None
                numeric_cols = [c for c in (data.columns.tolist() if data is not None else []) if (data is not None and pd.api.types.is_numeric_dtype(data[c]))]
                c1, c2, c3cols = st.columns([2, 1, 1])
                with c1:
                    kpi_name = st.text_input("KPI Name", key=f"custom_name_{dmn}")
                with c2:
                    kpi_col = st.selectbox("Column", options=numeric_cols, key=f"custom_col_{dmn}")
                with c3cols:
                    kpi_agg = st.selectbox("Aggregation", options=["sum", "mean", "min", "max", "count"], key=f"custom_agg_{dmn}")

                if st.button(f"➕ Add Custom KPI", key=f"add_custom_{dmn}"):
                    if kpi_name and kpi_col:
                        custom_existing.append({"name": kpi_name, "column": kpi_col, "agg": kpi_agg})
                        st.success("Custom KPI added (remember to Save KPIs).")
                    else:
                        st.error("Provide a name and select a column.")

                if st.button(f"Save {dmn.title()} KPIs", key=f"save_{dmn}_kpis"):
                    if cid3:
                        final_kpis = chosen + custom_existing
                        save_kpis(cid3, dmn, final_kpis)
                        st.success(f"Saved KPIs for {dmn}.")
                    else:
                        st.error("Select a client first.")


def page_admin_home() -> None:
    set_bg("admin")
    logout_button()

    user = st.session_state.get("auth", {}).get("user")
    if not user or user.get("role") != "admin":
        st.warning("Admin access only.")
        nav("login"); st.rerun()

    st.markdown("# Admin")
    opt = st.radio("Go to:", ["Dashboards", "Backend"], horizontal=True, key="admin_nav")
    if opt == "Dashboards":
        admin_dashboards()
    else:
        admin_backend()

# ----------------------------- Router -----------------------------

def router() -> None:
    init_db()
    page = st.query_params.get("page", st.session_state.get("page", "landing"))
    st.session_state["page"] = page

    if page == "landing":
        page_landing()
    elif page == "login":
        page_login()
    elif page == "client_home":
        page_client_home()
    elif page == "admin_home":
        page_admin_home()
    else:
        page_landing()

if __name__ == "__main__":
    router()
