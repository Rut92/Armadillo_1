# Armadillo — Streamlit Single‑File MVP
# ---------------------------------------------------------------
# Features
# - Landing page with name + tagline, aesthetic backgrounds
# - Auth (login / sign up / forgot password), role-based routing (client/admin)
# - Client dashboard: 3 tabs (Procurement, Inventory, Logistics) with KPI cards,
#   charts, slicers, print-friendly layout
# - Admin area: Dashboards (view all clients) & Backend tabs
#   1) Create/Edit Clients (assign client email)
#   2) Add/Edit/Remove Data (CSV/Excel upload → clean → inline edit)
#   3) Select KPIs per tab → used by client dashboards
# - Persistent storage in SQLite; passwords hashed (bcrypt)
# - Save-as-you-go: partial progress persists; incomplete dashboards show messages
# - Logout button only when logged in, redirects to Landing
# ---------------------------------------------------------------
# Run: pip install streamlit pandas plotly bcrypt SQLAlchemy openpyxl xlsxwriter
# Start: streamlit run armadillo_app.py

import os
import io
import json
from datetime import datetime, date

import bcrypt
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ----------------------------- App Config -----------------------------
st.set_page_config(page_title="Armadillo", page_icon="🦔", layout="wide")

# ----------------------------- Constants -----------------------------
APP_NAME = "Armadillo"
TAGLINE = "Strategic Insights. Operational Clarity."
DB_PATH = os.environ.get("ARMADILLO_DB", "armadillo.db")
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

# Default KPI options per tab (you can extend later)
DEFAULT_KPIS = {
    "procurement": ["Supplier OTD %", "PPV $", "PO Cycle Time (days)"],
    "inventory": ["Inventory Turns", "DOH", "Obsolete %"],
    "logistics": ["Perfect Order %", "Freight/Unit", "On-Time Ship %"],
}

# ----------------------------- DB Helpers -----------------------------
def init_db():
    with engine.begin() as con:
        # users table
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
        # clients table
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
        # datasets (per client, by domain)
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                domain TEXT NOT NULL,           -- procurement|inventory|logistics
                data_json TEXT,                 -- stored as JSON (records)
                updated_at TEXT
            )
            """
        ))
        # kpi configs (per client, by domain)
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kpi_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                domain TEXT NOT NULL,
                kpis_json TEXT,                 -- list of KPI names
                updated_at TEXT
            )
            """
        ))

    # seed an admin if none exists
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


def create_user(email: str, pw: str, role: str = "client", client_id: int | None = None):
    with engine.begin() as con:
        con.execute(text(
            "INSERT INTO users(email,password_hash,role,client_id,created_at) VALUES(:e,:p,:r,:cid,:c)"
        ), {"e": email, "p": hash_pw(pw), "r": role, "cid": client_id, "c": datetime.utcnow().isoformat()})


def upsert_client(name: str, background_notes: str = "") -> int:
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


def save_dataset(client_id: int, domain: str, df: pd.DataFrame):
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


def save_kpis(client_id: int, domain: str, kpis: list[str]):
    with engine.begin() as con:
        row = con.execute(text("SELECT id FROM kpi_configs WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if row:
            con.execute(text("UPDATE kpi_configs SET kpis_json=:j, updated_at=:u WHERE id=:i"),
                        {"j": json.dumps(kpis), "u": datetime.utcnow().isoformat(), "i": row[0]})
        else:
            con.execute(text("INSERT INTO kpi_configs(client_id,domain,kpis_json,updated_at) VALUES(:c,:d,:j,:u)"),
                        {"c": client_id, "d": domain, "j": json.dumps(kpis), "u": datetime.utcnow().isoformat()})


def load_kpis(client_id: int, domain: str) -> list[str]:
    with engine.begin() as con:
        row = con.execute(text("SELECT kpis_json FROM kpi_configs WHERE client_id=:c AND domain=:d"), {"c": client_id, "d": domain}).fetchone()
        if not row or not row[0]:
            return DEFAULT_KPIS.get(domain, [])
        return list(json.loads(row[0]))


# ----------------------------- UI Utilities -----------------------------

def set_bg(style_key: str):
    """Apply background CSS by page key."""
    styles = {
        "landing": "linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0b1020 100%)",
        "login": "linear-gradient(135deg, #0b1020 0%, #1b2a41 50%, #0b1020 100%)",
        "client": "linear-gradient(135deg, #0b1f2a, #0d3b66)",
        "admin": "linear-gradient(135deg, #1f2937, #111827)",
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
            header, footer, .stSidebar, .st-emotion-cache-uhkwx6, .st-emotion-cache-1dp5vir {{ display: none !important; }}
            .stApp {{ background: white !important; color: #000 !important; }}
            .print-card {{ border: 1px solid #eee; padding: 12px; border-radius: 8px; }}
        }}
        .print-button button {{
            border-radius: 10px; padding: 8px 14px; border: 1px solid rgba(255,255,255,0.2);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def logout_button():
    if st.session_state.get("auth", {}).get("logged_in"):
        col = st.columns([6,1])[1]
        with col:
            if st.button("🚪 Log out"):
                st.session_state.clear()
                st.experimental_set_query_params(page="landing")
                st.success("Logged out.")
                st.rerun()


def nav(page: str):
    st.experimental_set_query_params(page=page)
    st.session_state["page"] = page


# ----------------------------- Pages -----------------------------

def page_landing():
    set_bg("landing")
    logout_button()  # renders only if logged in

    top = st.columns([4,1])
    with top[0]:
        st.markdown(f"<div class='hero-title'>{APP_NAME}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='hero-sub'>{TAGLINE}</div>", unsafe_allow_html=True)
        st.write("")
        st.markdown("""
        <div class='glass'>
            <p>Welcome to <strong>Armadillo</strong> — a strategic consulting platform for SMBs.
            Build Power BI–style dashboards for <em>Procurement</em>, <em>Inventory</em>, and <em>Logistics</em>,
            complete with slicers, printable layouts, and an admin backend to onboard clients and clean data.</p>
        </div>
        """, unsafe_allow_html=True)
    with top[1]:
        st.write("")
        st.write("")
        if st.button("🔐 Login", use_container_width=True):
            nav("login")
            st.rerun()


def page_login():
    set_bg("login")
    logout_button()

    st.markdown("<h2>Login to Armadillo</h2>", unsafe_allow_html=True)

    tab_login, tab_signup, tab_forgot = st.tabs(["Login", "Sign up", "Forgot password"])

    with tab_login:
        email = st.text_input("Email")
        pw = st.text_input("Password", type="password")
        if st.button("Login"):
            user = get_user_by_email(email)
            if not user:
                st.error("No user found with that email.")
            else:
                if check_pw(pw, user["password_hash"]):
                    st.session_state["auth"] = {"logged_in": True, "user": user}
                    # route based on role
                    if user["role"] == "admin":
                        nav("admin_home")
                    else:
                        nav("client_home")
                    st.rerun()
                else:
                    st.error("Incorrect password.")

    with tab_signup:
        st.info("If the email does not exist, create an account (client by default). Admins can later upgrade roles.")
        email_s = st.text_input("Email (new)")
        pw1 = st.text_input("Create password", type="password")
        pw2 = st.text_input("Confirm password", type="password")
        client_name = st.text_input("Client/Company Name (optional for new client)")
        if st.button("Create account"):
            if pw1 != pw2 or not pw1:
                st.error("Passwords do not match or are empty.")
            elif get_user_by_email(email_s):
                st.error("Email already exists. Try login.")
            else:
                cid = None
                if client_name:
                    cid = upsert_client(client_name)
                create_user(email_s, pw1, role="client", client_id=cid)
                st.success("Account created. You can now login.")

    with tab_forgot:
        st.warning("Demo flow: reset without email token (for PoC). Replace with real email OTP later.")
        f_email = st.text_input("Registered Email")
        new_pw1 = st.text_input("New password", type="password")
        new_pw2 = st.text_input("Confirm new password", type="password")
        if st.button("Reset password"):
            user = get_user_by_email(f_email)
            if not user:
                st.error("No user with this email.")
            elif new_pw1 != new_pw2 or not new_pw1:
                st.error("Passwords don't match or empty.")
            else:
                with engine.begin() as con:
                    con.execute(text("UPDATE users SET password_hash=:p WHERE id=:i"),
                                {"p": hash_pw(new_pw1), "i": user["id"]})
                st.success("Password updated. Please login.")


# ----------------------------- Client Area -----------------------------

def slicers(df: pd.DataFrame):
    st.sidebar.header("🔎 Filters")
    filters = {}
    if "received_date" in df.columns:
        dmin, dmax = pd.to_datetime(df["received_date"]).min(), pd.to_datetime(df["received_date"]).max()
        d_from, d_to = st.sidebar.date_input("Date range", value=(dmin.date() if pd.notna(dmin) else date.today(),
                                                                   dmax.date() if pd.notna(dmax) else date.today()))
        filters["date"] = (pd.to_datetime(d_from), pd.to_datetime(d_to))
    if "supplier" in df.columns:
        sups = sorted([s for s in df["supplier"].dropna().unique().tolist()])
        sel = st.sidebar.multiselect("Supplier", sups, default=sups)
        filters["supplier"] = sel
    if "sku" in df.columns:
        skus = sorted([s for s in df["sku"].dropna().unique().tolist()])
        sel = st.sidebar.multiselect("SKU", skus, default=skus)
        filters["sku"] = sel
    return filters


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    ctx = df.copy()
    if "date" in filters and "received_date" in ctx.columns:
        d_from, d_to = filters["date"]
        ctx = ctx[(pd.to_datetime(ctx["received_date"]) >= d_from) & (pd.to_datetime(ctx["received_date"]) <= d_to)]
    if "supplier" in filters and "supplier" in ctx.columns:
        ctx = ctx[ctx["supplier"].isin(filters["supplier"])]
    if "sku" in filters and "sku" in ctx.columns:
        ctx = ctx[ctx["sku"].isin(filters["sku"])]
    return ctx


def kpi_cards(domain: str, df: pd.DataFrame, kpis: list[str]):
    c1,c2,c3 = st.columns(3)
    # Simple demo measures; extend with your domain logic
    if len(kpis) >= 1:
        if "on_time" in df.columns:
            v = float(df["on_time"].mean()) if not df.empty else 0
            c1.metric(kpis[0], f"{v:.1%}")
        elif "ppv_amt" in df.columns:
            c1.metric(kpis[0], f"${df['ppv_amt'].sum():,.0f}")
        else:
            c1.metric(kpis[0], "–")
    if len(kpis) >= 2:
        if "ppv_amt" in df.columns:
            c2.metric(kpis[1], f"${df['ppv_amt'].sum():,.0f}")
        elif "qty" in df.columns:
            c2.metric(kpis[1], f"{df['qty'].sum():,.0f}")
        else:
            c2.metric(kpis[1], "–")
    if len(kpis) >= 3:
        if "act_cost" in df.columns and "qty" in df.columns:
            total = (df["act_cost"]*df["qty"]).sum()
            qty = df["qty"].sum() or 1
            c3.metric(kpis[2], f"${total/qty:,.2f}")
        else:
            c3.metric(kpis[2], "–")


def dashboard_section(title: str, client_id: int, domain: str):
    st.subheader(title)
    df = load_dataset(client_id, domain)
    if df is None or df.empty:
        st.info("Dashboard is being prepared. Data not available yet.")
        return

    # ensure a few helper columns for demo visuals
    if "ppv_amt" not in df.columns and set(["std_cost","act_cost","qty"]).issubset(df.columns):
        df["ppv_amt"] = (pd.to_numeric(df["act_cost"], errors='coerce') - pd.to_numeric(df["std_cost"], errors='coerce')) * pd.to_numeric(df["qty"], errors='coerce')
    if "on_time" not in df.columns and set(["received_date","promised_date"]).issubset(df.columns):
        rd = pd.to_datetime(df["received_date"], errors='coerce')
        pdm = pd.to_datetime(df["promised_date"], errors='coerce')
        df["on_time"] = (rd <= pdm).astype(int)

    kpis = load_kpis(client_id, domain)

    # Print button (JS window.print)
    st.markdown("<div class='print-button'>", unsafe_allow_html=True)
    st.button("🖨️ Print Dashboard", on_click=lambda: st.write(""))
    st.markdown(
        """
        <script>
        const btns = window.parent.document.querySelectorAll('button');
        btns.forEach(b => { if (b.innerText.includes('Print Dashboard')) { b.onclick = () => window.print(); }});
        </script>
        """, unsafe_allow_html=True
    )

    filters = slicers(df)
    ctx = apply_filters(df, filters)

    kpi_cards(domain, ctx, kpis)

    # Chart row
    c1, c2 = st.columns(2)
    with c1:
        if "received_date" in ctx.columns and "on_time" in ctx.columns:
            ctx["month"] = pd.to_datetime(ctx["received_date"], errors='coerce').dt.to_period("M").dt.to_timestamp()
            chart_df = ctx.groupby("month", as_index=False).agg(otd=("on_time","mean"))
            fig = px.line(chart_df, x="month", y="otd", markers=True, title="On-Time Delivery by Month")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Add received_date & on_time columns for OTD chart.")
    with c2:
        if "supplier" in ctx.columns and "ppv_amt" in ctx.columns:
            top = ctx.groupby("supplier", as_index=False)["ppv_amt"].sum().sort_values("ppv_amt", ascending=False)
            fig2 = px.bar(top.head(10), x="supplier", y="ppv_amt", title="Top Suppliers by PPV")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Add supplier & ppv_amt columns for PPV chart.")

    st.markdown("### Detail Table (Filtered)")
    st.dataframe(ctx, use_container_width=True)


def page_client_home():
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


# ----------------------------- Admin Area -----------------------------

def page_admin_home():
    set_bg("admin")
    logout_button()

    user = st.session_state.get("auth", {}).get("user")
    if not user or user.get("role") != "admin":
        st.warning("Admin access only.")
        nav("login"); st.rerun()

    st.markdown("# Admin")
    opt = st.radio("Go to:", ["Dashboards", "Backend"], horizontal=True)
    if opt == "Dashboards":
        admin_dashboards()
    else:
        admin_backend()


def admin_dashboards():
    st.markdown("### View Client Dashboards")
    clients = list_clients()
    if not clients:
        st.info("No clients yet.")
        return
    cid = st.selectbox("Select Client", options=[c[0] for c in clients], format_func=lambda x: dict(clients).get(x))
    tabs = st.tabs(["Procurement", "Inventory", "Logistics"])
    with tabs[0]:
        dashboard_section("Procurement Dashboard", cid, "procurement")
    with tabs[1]:
        dashboard_section("Inventory Dashboard", cid, "inventory")
    with tabs[2]:
        dashboard_section("Logistics Dashboard", cid, "logistics")


def admin_backend():
    st.markdown("### Backend")
    t1, t2, t3 = st.tabs(["1) Create/Edit Clients", "2) Add/Edit/Remove Data", "3) Select KPIs"])

    # --- Step 1: Create/Edit Clients ---
    with t1:
        st.subheader("Create or Edit Client")
        c_left, c_right = st.columns(2)
        with c_left:
            cname = st.text_input("Client Name")
            cnotes = st.text_area("Background notes (optional)")
            if st.button("Save Client"):
                cid = upsert_client(cname, cnotes)
                st.success(f"Saved client '{cname}' (id={cid}). Redirecting to Step 2…")
                st.session_state["last_client_id"] = cid
                st.experimental_set_query_params(page="admin_home", step="data", cid=str(cid))
                st.rerun()
        with c_right:
            st.markdown("**Existing Clients**")
            cl = list_clients()
            if cl:
                st.table(pd.DataFrame(cl, columns=["ID","Name"]))
            else:
                st.info("No clients yet.")

    # --- Step 2: Add/Edit/Remove Data ---
    with t2:
        st.subheader("Upload / Clean / Edit Data")
        clients = list_clients()
        cid = st.selectbox("Client", options=[c[0] for c in clients] if clients else [None],
                           format_func=lambda x: dict(clients).get(x, "—") if x else "—",
                           index=0 if clients else 0)
        domain = st.selectbox("Domain", ["procurement","inventory","logistics"])
        up = st.file_uploader("Upload CSV/Excel", type=["csv","xlsx","xls"])

        if up:
            if up.name.endswith(".csv"):
                df = pd.read_csv(up)
            else:
                df = pd.read_excel(up)
            # Basic cleaning: trim cols, lower snake case
            df.columns = (df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_"))
            # coerce common fields
            for c in ["received_date","promised_date","eta","date"]:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors='coerce')
            for c in ["qty","quantity","act_cost","std_cost","price"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            # Highlight problematic rows
            issues = df[df.isna().any(axis=1)]
            if not issues.empty:
                st.warning("⚠️ Some rows have missing/invalid values. You can edit below.")
                st.dataframe(issues, use_container_width=True)

            st.markdown("#### Review / Edit Data")
            edited = st.data_editor(df, use_container_width=True, num_rows="dynamic")

            if st.button("💾 Save Cleaned Data"):
                if not cid:
                    st.error("Select a client first.")
                else:
                    save_dataset(cid, domain, edited)
                    st.success("Data saved.")
                    st.info("Proceed to Step 3 to select KPIs.")
                    st.experimental_set_query_params(page="admin_home", step="kpis", cid=str(cid), domain=domain)
        else:
            st.info("Upload a CSV/Excel to begin cleaning.")

    # --- Step 3: Select KPIs ---
    with t3:
        st.subheader("Select KPIs per Tab")
        clients = list_clients()
        cid = st.selectbox("Client", options=[c[0] for c in clients] if clients else [None],
                           format_func=lambda x: dict(clients).get(x, "—") if x else "—",
                           key="kpi_client")
        for domain in ["procurement","inventory","logistics"]:
            with st.expander(f"KPIs for {domain.title()}"):
                chosen = st.multiselect("Choose KPIs", options=DEFAULT_KPIS[domain], default=load_kpis(cid, domain) if cid else DEFAULT_KPIS[domain], key=f"kpi_{domain}")
                if st.button(f"Save {domain.title()} KPIs", key=f"save_{domain}"):
                    if cid:
                        save_kpis(cid, domain, chosen)
                        st.success(f"Saved KPIs for {domain}.")
                    else:
                        st.error("Select a client first.")


# ----------------------------- Router -----------------------------

def router():
    init_db()
    q = st.experimental_get_query_params()
    page = q.get("page", [None])[0] or st.session_state.get("page") or "landing"
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
