
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------------
# App configuration
# -----------------------------------------------------------------------------

# Streamlit-specific: page config controls the browser tab title and responsive
# layout. "wide" gives more horizontal room for dashboard tables.
st.set_page_config(
    page_title="TitleIQ Agent — NPL Title Review",
    page_icon="📘",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "titleiq_reviews.db"
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

for folder in [UPLOAD_DIR, OUTPUT_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Database helpers
# -----------------------------------------------------------------------------

def get_db_connection() -> sqlite3.Connection:
    """
    Returns a SQLite connection for persistent review history.

    Why:
    Streamlit Cloud apps are stateless between runs at the script level, so the
    review table should be stored in a small local database file for search and
    reopen flows during the app session lifetime.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Creates the reviews table if it does not already exist.

    Why:
    The user requested a searchable history of past packet reviews with notes,
    recommendation, risk score, and file locations.
    """
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT UNIQUE,
            property_address TEXT,
            analyst_name TEXT,
            review_date TEXT,
            risk_score REAL,
            recommendation TEXT,
            analyst_notes TEXT,
            dashboard_pdf_path TEXT,
            analysis_excel_path TEXT,
            processing_log_path TEXT,
            dashboard_json_path TEXT,
            source_pdf_path TEXT,
            status TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# Orchestrator loading
# -----------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def load_orchestrator():
    """
    Loads the existing master orchestrator module from the app folder.

    Why:
    The interface layer should change from Flask to Streamlit, but the analysis
    logic itself must remain unchanged.
    """
    orchestrator_path = BASE_DIR / "titleiq_master_orchestrator.py"
    if not orchestrator_path.exists():
        raise FileNotFoundError(
            "titleiq_master_orchestrator.py was not found in the Streamlit app folder."
        )

    spec = importlib.util.spec_from_file_location(
        "titleiq_master_orchestrator", str(orchestrator_path)
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

def safe_float(value, default=0.0) -> float:
    """
    Converts values to float safely.

    Why:
    SQLite and JSON payloads can hold null-like or string-like values that need
    a safe numeric conversion for filtering and metrics.
    """
    try:
        return float(value)
    except Exception:
        return float(default)


def money(value: float) -> str:
    """
    Formats numeric values as USD strings.

    Why:
    Waterfall outputs and projected recovery are easier to review in standard
    financial notation than raw floating point values.
    """
    return "${:,.2f}".format(float(value))


def save_uploaded_pdf(uploaded_file, job_id: str) -> Path:
    """
    Saves the uploaded PDF to a persistent local path.

    Why:
    The orchestrator expects a filesystem path, so the uploaded file must be
    written to disk before the integrated pipeline can run.
    """
    filename = uploaded_file.name or f"{job_id}.pdf"
    safe_name = filename.replace("/", "_").replace("\\", "_")
    target_path = UPLOAD_DIR / f"{job_id}_{safe_name}"
    target_path.write_bytes(uploaded_file.getvalue())
    return target_path


def get_summary_metrics() -> Dict[str, float]:
    """
    Computes sidebar metrics from review history.

    Why:
    The sidebar should show a quick operational summary of completed reviews.
    """
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_reviews,
            AVG(risk_score) AS avg_risk_score
        FROM reviews
        WHERE status IN ('completed', 'reviewed')
        """
    ).fetchone()
    conn.close()

    return {
        "total_reviews": int(row["total_reviews"] or 0),
        "avg_risk_score": round(float(row["avg_risk_score"] or 0.0), 2),
    }


def save_review_record(
    job_id: str,
    property_address: str,
    analyst_name: str,
    dashboard: Dict,
    dashboard_pdf_path: str,
    analysis_excel_path: str,
    processing_log_path: str,
    dashboard_json_path: str,
    source_pdf_path: str,
    analyst_notes: str = "",
    status: str = "completed",
) -> None:
    """
    Inserts or updates a review record in SQLite.

    Why:
    Each run should be persistent so analysts can revisit prior work and add
    manual observations later.
    """
    conn = get_db_connection()
    conn.execute(
        """
        INSERT OR REPLACE INTO reviews (
            job_id, property_address, analyst_name, review_date, risk_score,
            recommendation, analyst_notes, dashboard_pdf_path, analysis_excel_path,
            processing_log_path, dashboard_json_path, source_pdf_path, status, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            property_address,
            analyst_name,
            dashboard.get("header", {}).get("review_date"),
            dashboard.get("header", {}).get("overall_risk_score"),
            dashboard.get("acquisition_recommendation", {}).get("recommendation"),
            analyst_notes,
            dashboard_pdf_path,
            analysis_excel_path,
            processing_log_path,
            dashboard_json_path,
            source_pdf_path,
            status,
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
        ),
    )
    conn.commit()
    conn.close()


def load_review_by_job_id(job_id: str) -> Optional[sqlite3.Row]:
    """
    Loads a review record by job ID.

    Why:
    Review history must allow any prior packet to be reopened in the browser.
    """
    conn = get_db_connection()
    row = conn.execute("SELECT * FROM reviews WHERE job_id = ?", (job_id,)).fetchone()
    conn.close()
    return row


def load_dashboard_json(path_str: str) -> Dict:
    """
    Loads a dashboard JSON payload from disk.

    Why:
    The results page and history reopen flow render from the saved JSON rather
    than recomputing the full analysis every time.
    """
    if not path_str:
        return {}
    path = Path(path_str)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_binary(path_str: str) -> bytes:
    """
    Reads an output file as bytes for Streamlit download buttons.

    Why:
    st.download_button requires raw bytes or text content.
    """
    if not path_str:
        return b""
    path = Path(path_str)
    if not path.exists():
        return b""
    return path.read_bytes()


# -----------------------------------------------------------------------------
# Result rendering
# -----------------------------------------------------------------------------

def show_risk_banner(risk_score: float) -> None:
    """
    Displays the overall risk score with Streamlit color coding.

    Why:
    The user explicitly requested success / warning / error treatment mapped to
    risk score bands.
    """
    label = f"Overall Risk Score: {risk_score:.1f} / 10"
    if risk_score <= 3:
        st.success(label)
    elif risk_score <= 6:
        st.warning(label)
    else:
        st.error(label)


def render_results(dashboard: Dict, review_row: Optional[sqlite3.Row] = None) -> None:
    """
    Renders the complete browser-based dashboard.

    Why:
    This is the main analyst-facing output after a successful run or when a
    historical review is reopened.
    """
    if not dashboard:
        st.info("No dashboard data is available for this review.")
        return

    risk_score = safe_float(dashboard.get("header", {}).get("overall_risk_score"), 0.0)
    recommendation = dashboard.get("acquisition_recommendation", {})
    review_id = review_row["job_id"] if review_row else None

    col1, col2, col3 = st.columns([2.2, 1.2, 1.2])
    with col1:
        st.subheader(dashboard.get("header", {}).get("property_address", "Property"))
        st.caption(
            f"Review Date: {dashboard.get('header', {}).get('review_date', 'N/A')} | "
            f"Analyst: {dashboard.get('header', {}).get('analyst_name') or 'N/A'} | "
            f"Packet Quality: {dashboard.get('header', {}).get('packet_quality_score', 'N/A')}%"
        )
    with col2:
        show_risk_banner(risk_score)
    with col3:
        st.metric("Projected Recovery", money(dashboard.get("projected_net_recovery", 0.0)))

    st.markdown("### Acquisition Recommendation")
    rec_text = recommendation.get("recommendation", "N/A")
    rationale = recommendation.get("rationale", "No rationale available.")
    st.markdown(
        f"""
        <div style="border:1px solid #d8e3f0;border-radius:12px;padding:16px;background:#f8fbff;">
            <div style="font-size:1.1rem;font-weight:700;color:#12345b;">{rec_text}</div>
            <div style="margin-top:8px;color:#334155;">{rationale}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Waterfall Calculation")
    waterfall_df = pd.DataFrame(dashboard.get("waterfall_summary_table", []))
    if not waterfall_df.empty:
        investor_position = dashboard.get("investor_position")
        def highlight_investor_row(row):
            if row.get("step") == investor_position:
                return ["background-color: #fff3cd; font-weight: 600;" for _ in row]
            return ["" for _ in row]

        styled_wf = waterfall_df.style.format(
            {"balance": "${:,.2f}", "remaining_after": "${:,.2f}"}
        ).apply(highlight_investor_row, axis=1)
        st.dataframe(styled_wf, use_container_width=True, hide_index=True)
    else:
        st.info("No waterfall rows available.")

    st.markdown("### Chain of Title Timeline")
    chain_df = pd.DataFrame(dashboard.get("chain_of_title_timeline", []))
    if not chain_df.empty:
        def highlight_flag_row(row):
            if row.get("status") == "FLAG":
                return ["background-color: #fde2e1;" for _ in row]
            return ["" for _ in row]

        styled_chain = chain_df.style.apply(highlight_flag_row, axis=1)
        st.dataframe(styled_chain, use_container_width=True, hide_index=True)
    else:
        st.info("No chain timeline is available.")

    st.markdown("### Critical Alerts")
    alerts = dashboard.get("critical_alerts", [])
    if alerts:
        for idx, alert in enumerate(alerts, start=1):
            st.warning(
                f"{idx}. [{alert.get('severity', 'ALERT')}] {alert.get('message', '')} "
                f"({alert.get('page_reference', 'No page reference')})"
            )
    else:
        st.success("No critical alerts were generated.")

    st.markdown("### Recommended Next Actions")
    actions = dashboard.get("recommended_next_actions", [])
    if actions:
        for idx, action in enumerate(actions, start=1):
            st.markdown(f"{idx}. {action}")
    else:
        st.markdown("No follow-up actions were generated.")

    if review_row:
        st.markdown("### Downloads")
        d1, d2, d3 = st.columns(3)
        with d1:
            st.download_button(
                label="Download Dashboard PDF",
                data=read_binary(review_row["dashboard_pdf_path"]),
                file_name=f"{review_row['job_id']}_dashboard.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        with d2:
            st.download_button(
                label="Download Analysis Excel",
                data=read_binary(review_row["analysis_excel_path"]),
                file_name=f"{review_row['job_id']}_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with d3:
            st.download_button(
                label="Download Processing Log",
                data=read_binary(review_row["processing_log_path"]),
                file_name=f"{review_row['job_id']}_processing_log.json",
                mime="application/json",
                use_container_width=True,
            )

        st.markdown("### Review Notes")
        existing_notes = review_row["analyst_notes"] or ""
        notes_key = f"notes_{review_row['job_id']}"
        notes = st.text_area(
            "Add manual analyst observations",
            value=existing_notes,
            height=160,
            key=notes_key,
        )

        if st.button("Mark As Reviewed", key=f"review_{review_row['job_id']}"):
            conn = get_db_connection()
            conn.execute(
                "UPDATE reviews SET analyst_notes = ?, status = ? WHERE job_id = ?",
                (notes, "reviewed", review_row["job_id"]),
            )
            conn.commit()
            conn.close()
            st.success("Review notes saved and packet marked as reviewed.")
            st.rerun()


# -----------------------------------------------------------------------------
# Analysis execution
# -----------------------------------------------------------------------------

def run_analysis(uploaded_pdf, analyst_name: str, property_address: str) -> Optional[str]:
    """
    Runs the complete pipeline and persists the result.

    Why:
    The upload page should trigger the same integrated TitleIQ workflow that
    was already built in prior phases.
    """
    orchestrator = load_orchestrator()
    job_id = uuid.uuid4().hex[:12]
    saved_pdf = save_uploaded_pdf(uploaded_pdf, job_id)

    job_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    dashboard_pdf = job_dir / "dashboard.pdf"
    analysis_excel = job_dir / "analysis.xlsx"
    processing_log = job_dir / "processing_log.json"
    dashboard_json = job_dir / "dashboard.json"

    # Streamlit-specific: progress and status placeholders give live feedback
    # during a long-running synchronous pipeline.
    progress = st.progress(0)
    status = st.empty()

    phases = [
        (0.15, "Phase 1: Processing document quality..."),
        (0.40, "Phase 2: Extracting chain of title..."),
        (0.70, "Phase 3: Calculating lien waterfall..."),
        (0.90, "Phase 4: Generating analyst dashboard..."),
    ]

    for pct, msg in phases[:-1]:
        progress.progress(int(pct * 100))
        status.info(msg)
        time.sleep(0.15)

    status.info(phases[-1][1])

    result = orchestrator.run_titleiq_agent(
        pdf_path=str(saved_pdf),
        property_estimated_value=280000,
        investor_lien_type="SECOND_MORTGAGE",
        property_address=property_address,
        analyst_name=analyst_name,
        state="FL",
        output_pdf_path=str(dashboard_pdf),
        output_xlsx_path=str(analysis_excel),
        output_log_path=str(processing_log),
    )

    dashboard = result.get("dashboard_output", {})
    dashboard_json.write_text(json.dumps(dashboard, indent=2), encoding="utf-8")

    save_review_record(
        job_id=job_id,
        property_address=property_address,
        analyst_name=analyst_name,
        dashboard=dashboard,
        dashboard_pdf_path=str(dashboard_pdf),
        analysis_excel_path=str(analysis_excel),
        processing_log_path=str(processing_log),
        dashboard_json_path=str(dashboard_json),
        source_pdf_path=str(saved_pdf),
        analyst_notes="",
        status="completed",
    )

    progress.progress(100)
    status.success("Full analysis completed successfully.")
    return job_id


# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------

def page_upload_and_analysis():
    """
    Renders Page 1: upload and analysis.

    Why:
    This is the primary operational entry point for analysts uploading new title packets.
    """
    st.title("TitleIQ Agent — NPL Title Review")
    st.caption("Upload a title packet PDF and run the full four-phase analysis pipeline.")

    with st.container(border=True):
        uploaded_pdf = st.file_uploader("Upload Title Packet PDF", type=["pdf"])
        col1, col2 = st.columns(2)
        with col1:
            analyst_name = st.text_input("Analyst Name")
        with col2:
            property_address = st.text_input("Property Address")

        run_clicked = st.button("Run Full Analysis", type="primary", use_container_width=True)

        if run_clicked:
            if uploaded_pdf is None:
                st.error("Please upload a PDF file.")
                return
            if not property_address.strip():
                st.error("Please enter the property address.")
                return

            try:
                job_id = run_analysis(uploaded_pdf, analyst_name.strip(), property_address.strip())
                if job_id:
                    st.session_state["selected_job_id"] = job_id
                    st.success("Analysis completed. View the results below or reopen it later from Review History.")
                    review_row = load_review_by_job_id(job_id)
                    dashboard = load_dashboard_json(review_row["dashboard_json_path"]) if review_row else {}
                    render_results(dashboard, review_row)
            except Exception as exc:
                st.exception(exc)

    existing_job = st.session_state.get("selected_job_id")
    if existing_job:
        st.markdown("---")
        st.subheader("Latest Results")
        review_row = load_review_by_job_id(existing_job)
        dashboard = load_dashboard_json(review_row["dashboard_json_path"]) if review_row else {}
        render_results(dashboard, review_row)


def page_review_history():
    """
    Renders Page 3: review history.

    Why:
    Analysts need a searchable archive of prior packets, including the ability
    to reopen any completed review.
    """
    st.title("Review History")
    st.caption("Search and reopen previously reviewed title packets.")

    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM reviews ORDER BY review_date DESC, id DESC", conn)
    conn.close()

    if df.empty:
        st.info("No reviews have been stored yet.")
        return

    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    with col1:
        min_risk = st.number_input("Min Risk", min_value=0.0, max_value=10.0, value=0.0, step=0.1)
    with col2:
        max_risk = st.number_input("Max Risk", min_value=0.0, max_value=10.0, value=10.0, step=0.1)
    with col3:
        recommendation = st.selectbox(
            "Recommendation",
            ["All", "PROCEED WITH CAUTION", "REQUIRES FURTHER INVESTIGATION", "DO NOT PROCEED"],
            index=0,
        )
    with col4:
        start_date = st.date_input("From", value=None)
    with col5:
        end_date = st.date_input("To", value=None)

    filtered = df.copy()
    filtered = filtered[(filtered["risk_score"].fillna(-1) >= min_risk) & (filtered["risk_score"].fillna(-1) <= max_risk)]
    if recommendation != "All":
        filtered = filtered[filtered["recommendation"] == recommendation]
    if start_date:
        filtered = filtered[filtered["review_date"].fillna("") >= str(start_date)]
    if end_date:
        filtered = filtered[filtered["review_date"].fillna("") <= str(end_date)]

    display_cols = [
        "job_id",
        "property_address",
        "review_date",
        "risk_score",
        "recommendation",
        "status",
        "analyst_name",
    ]
    st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

    # Streamlit-specific: dataframe row-click selection using on_select. If a
    # single row is selected, the review is reopened below.
    st.markdown("#### Open a Review")
    selection_df = filtered[display_cols].copy()
    event = st.dataframe(
        selection_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="history_selector",
    )

    selected_job_id = None
    try:
        selected_rows = event.selection.rows
        if selected_rows:
            selected_job_id = selection_df.iloc[selected_rows[0]]["job_id"]
    except Exception:
        selected_job_id = None

    # Fallback selector if row-click selection is not available in the current runtime.
    if not selected_job_id:
        options = [""] + filtered["job_id"].tolist()
        selected_job_id = st.selectbox(
            "Or choose a review ID",
            options=options,
            index=0,
        )

    if selected_job_id:
        review_row = load_review_by_job_id(selected_job_id)
        dashboard = load_dashboard_json(review_row["dashboard_json_path"]) if review_row else {}
        st.markdown("---")
        render_results(dashboard, review_row)


# -----------------------------------------------------------------------------
# Main app
# -----------------------------------------------------------------------------

def main():
    """
    Runs the Streamlit app.

    Why:
    A single main entry point keeps navigation and initialization predictable.
    """
    init_db()

    metrics = get_summary_metrics()

    with st.sidebar:
        st.header("Navigation")
        page = st.radio(
            "Go to",
            ["Upload and Analysis", "Review History"],
            index=0,
        )

        st.markdown("---")
        st.subheader("Portfolio Summary")
        st.metric("Total Reviews", metrics["total_reviews"])
        st.metric("Average Risk Score", metrics["avg_risk_score"])

        st.markdown("---")
        st.caption(
            "This Streamlit interface reuses the existing TitleIQ Agent analysis "
            "logic without changing the underlying phases."
        )

    if page == "Upload and Analysis":
        page_upload_and_analysis()
    else:
        page_review_history()


if __name__ == "__main__":
    main()
