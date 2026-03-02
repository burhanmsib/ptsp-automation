# =========================
# MODULE 1 – GOOGLE SHEET VIA SERVICE ACCOUNT
# =========================

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

# =========================
# REQUIRED COLUMNS
# =========================
REQUIRED_COLUMNS = [
    "Id",
    "Requester",
    "Timestamp",
    "Nama Perusahaan",
    "Alamat Perusahaan",
    "Nomor Surat",
    "Informasi",
    "Tanggal Koordinat",
    "Koordinat",
    "Koordinat Awal",
    "Koordinat Akhir",
    "Koordinat Awal (Desimal)",
    "Koordinat Akhir (Desimal)",
    "Water Checker Awal",
    "Water Checker Akhir"
]

# =========================
# VALIDATOR
# =========================
def validate_request_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom wajib tidak ditemukan: {missing}")
    return df.reset_index(drop=True)


# =========================
# LOAD GOOGLE SHEET
# =========================
@st.cache_data(show_spinner=False)
def load_google_sheet():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # ======================================
    # MODE 1: LOCALHOST (pakai JSON file)
    # ======================================
    if Path("service_account.json").exists():

        creds = Credentials.from_service_account_file(
            "service_account.json",
            scopes=scopes,
        )

        spreadsheet_id = "18SMCEU0t9tDub0wpwFsvnyM92z_HOe4w7nju8iRSFeY"
        worksheet_name = "Surat"

    # ======================================
    # MODE 2: STREAMLIT CLOUD (pakai secrets)
    # ======================================
    else:

        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=scopes,
        )

        spreadsheet_id = st.secrets["google_sheet"]["spreadsheet_id"]
        worksheet_name = st.secrets["google_sheet"]["worksheet_name"]

    client = gspread.authorize(creds)

    sheet = client.open_by_key(spreadsheet_id).worksheet(worksheet_name)

    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    return validate_request_dataframe(df)


# =========================
# STREAMLIT WRAPPER
# =========================
def load_request_sheet_streamlit():
    st.header("📄 Data Permintaan PTSP (Google Sheet)")

    try:
        df = load_google_sheet()
        st.success("✅ Data Google Sheet berhasil dimuat")
        st.write(f"Total permintaan: **{len(df)}**")
        return df

    except Exception as e:
        st.error("❌ Gagal memuat Google Sheet")
        st.exception(e)
        return None