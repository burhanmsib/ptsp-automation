import streamlit as st
import pandas as pd
from pathlib import Path

# =========================
# IMPORT MODULE
# =========================
from modules.module1_request import load_request_sheet_streamlit  # ← UPDATED
from modules.module2_route import process_route_segment_module2_streamlit
from modules.module34_data import process_module34
from modules.module5_analysis import process_module5
from modules.module6_report import generate_final_docx_streamlit

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="PTSP Marine Meteorological Report",
    page_icon="🌊",
    layout="wide"
)

st.title("🌊 PTSP Marine Meteorological Report Automation")
st.caption("BMKG – Otomatisasi Analisis Cuaca Maritim (WW3 + FVCOM + GSMaP)")

# =========================
# SESSION STATE INIT
# =========================
for key in [
    "df_requests",
    "selected_id",
    "results_module2",
    "results_module34",
    "results_module5",
    "doc_buffer",
]:
    if key not in st.session_state:
        st.session_state[key] = None

# =========================
# MODULE 1 – GOOGLE SHEET
# =========================
st.header("🟦 Module 1 – Data Permintaan PTSP (Google Sheet)")

df_requests = load_request_sheet_streamlit()
if df_requests is None:
    st.stop()

st.session_state.df_requests = df_requests

# =========================
# PILIH ID
# =========================
st.header("🆔 Pilih ID Surat")

id_list = sorted(df_requests["Id"].astype(str).unique())

mode = st.radio(
    "Metode Pemilihan ID",
    ["Pilih dari daftar", "Input manual"],
    horizontal=True
)

if mode == "Pilih dari daftar":
    selected_id = st.selectbox(
        "Pilih ID Surat",
        id_list
    )

else:
    selected_id = st.text_input(
        "Masukkan ID Surat secara manual"
    )

    if selected_id and selected_id not in id_list:
        st.warning("⚠️ ID tidak ditemukan dalam database")

# =========================
# FILTER DATA BERDASARKAN ID
# =========================
if selected_id:
    df_id = (
        df_requests[df_requests["Id"].astype(str) == selected_id]
        .reset_index(drop=True)
    )

    if df_id.empty:
        st.error("❌ Data untuk ID tersebut tidak ditemukan.")
        st.stop()

    st.success(f"📄 Total {len(df_id)} permintaan untuk ID {selected_id}")

    st.dataframe(
        df_id[["Tanggal Koordinat", "Koordinat Awal", "Koordinat Akhir"]],
        use_container_width=True
    )
else:
    st.stop()

# =========================
# MODULE 2 – ROUTE PER TANGGAL
# =========================
st.header("🟩 Module 2 – Gambar Rute (Per Tanggal)")

results_module2 = []

for idx, row in df_id.iterrows():
    st.markdown("---")
    st.markdown(f"### 📍 Tanggal {row['Tanggal Koordinat']}")

    st.caption(
        f"Koordinat: {row['Koordinat Awal']} → {row['Koordinat Akhir']}"
    )

    hasil = process_route_segment_module2_streamlit(row, idx)

    if hasil is None:
        st.warning("❌ Rute belum valid. Silakan gambar ulang.")
        st.stop()

    results_module2.append(hasil)

st.session_state.results_module2 = results_module2
st.success("✅ Semua rute per tanggal berhasil ditentukan")

# =========================
# MODULE 3 & 4 – WEATHER SAMPLING
# =========================
st.header("🟨 Module 3 & 4 – Pengambilan Data Cuaca")

if not st.session_state.get("results_module2"):
    st.info("Selesaikan dan simpan semua rute terlebih dahulu.")
else:

    tz = st.selectbox("Zona Waktu Analisis", ["WIB", "WITA", "WIT"], index=0)

    if st.button("🌐 Ambil Data Cuaca", type="primary"):

        results_module34 = []
        gagal = False

        with st.spinner("Mengambil data cuaca (WW3 + FVCOM + GSMaP FTP)..."):

            for i, item in enumerate(st.session_state.results_module2):

                result = process_module34(
                    row=df_id.iloc[i],
                    polyline=item["titik5"],
                    tz=tz
                )

                if result is None:
                    gagal = True
                    break

                results_module34.append(result)

        if gagal:
            st.session_state.results_module34 = None
            st.error("❌ Gagal mengambil data cuaca. Periksa koneksi atau dataset.")
        else:
            st.session_state.results_module34 = results_module34
            st.success("✅ Data cuaca berhasil diambil")
            
# =========================
# MODULE 5 – WEATHER ANALYSIS
# =========================
st.header("🟧 Module 5 – Analisis Cuaca (Berbasis Rainfall)")

if not st.session_state.get("results_module34"):
    st.info("Ambil data cuaca terlebih dahulu.")
else:

    with st.spinner("📊 Analisis cuaca 6-jaman..."):
        results_module5 = process_module5(
            st.session_state.results_module34,
            tz=tz
        )

    st.session_state.results_module5 = results_module5
    st.success("✅ Analisis selesai")

# =========================
# MODULE 6 – GENERATE REPORT
# =========================
st.header("🟥 Module 6 – Generate Laporan Word")

template_path = Path("templates/Template PTSP.docx")

if not template_path.exists():
    st.error("Template Word tidak ditemukan.")
    st.stop()

if st.button("📄 Generate Laporan Word", type="primary"):

    with st.spinner("📝 Menyusun laporan..."):

        doc_buffer = generate_final_docx_streamlit(
            module1_rows=df_id.to_dict(orient="records"),
            module5_rows=st.session_state.results_module5,
            template_path=str(template_path)
        )

        st.session_state.doc_buffer = doc_buffer

    st.success("✅ Laporan berhasil dibuat")

# =========================
# DOWNLOAD BUTTON
# =========================
if st.session_state.doc_buffer:

    st.download_button(
        "⬇️ Download Laporan PTSP",
        data=st.session_state.doc_buffer,
        file_name=f"PTSP_Report_ID_{selected_id}.docx",
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
