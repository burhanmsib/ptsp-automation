# =========================
# MODULE 2 – ROUTE ENGINE (UPDATED & STABLE)
# =========================

import streamlit as st
from streamlit_folium import st_folium
from shapely.geometry import LineString
import folium
from folium.plugins import Draw

REQUIRED_POINTS = 5


# =========================
# HELPER – PARSE KOORDINAT
# =========================
def parse_decimal_coordinate(value):
    """
    Aman untuk data Google Sheet (kadang ada spasi)
    Format: "lat, lon"
    """
    try:
        parts = str(value).replace(" ", "").split(",")
        return float(parts[0]), float(parts[1])
    except Exception:
        return None, None


# =========================
# INTERPOLASI 5 TITIK
# =========================
def split_route_into_5(points_latlon):

    if len(points_latlon) < 2:
        return None

    line = LineString([(lon, lat) for lat, lon in points_latlon])

    fractions = [0.0, 0.25, 0.50, 0.75, 1.0]
    result = []

    for f in fractions:
        p = line.interpolate(f, normalized=True)
        result.append((p.y, p.x))  # lat, lon

    return result


# =========================
# MARKER STYLE
# =========================
def numbered_marker(lat, lon, number):
    html = f"""
    <div style="
        background-color:#0D47A1;
        color:white;
        border-radius:50%;
        width:30px;
        height:30px;
        text-align:center;
        font-weight:bold;
        font-size:13px;
        line-height:30px;
        border:2px solid white;
        box-shadow:0 0 5px rgba(0,0,0,0.6);
    ">
        {number}
    </div>
    """
    return folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(html=html),
        tooltip=f"Titik {number}"
    )


# =========================
# MAIN FUNCTION
# =========================
def process_route_segment_module2_streamlit(row, map_key):

    lat1, lon1 = parse_decimal_coordinate(row.get("Koordinat Awal (Desimal)"))
    lat2, lon2 = parse_decimal_coordinate(row.get("Koordinat Akhir (Desimal)"))

    if None in (lat1, lon1, lat2, lon2):
        st.error("Format koordinat desimal tidak valid.")
        return None

    route_key = f"route_{map_key}"

    # =====================================================
    # JIKA SUDAH DISIMPAN → TAMPILKAN MAP FINAL TERKUNCI
    # =====================================================
    if route_key in st.session_state:

        titik5 = st.session_state[route_key]

        m_final = folium.Map(
            location=[(lat1 + lat2) / 2, (lon1 + lon2) / 2],
            zoom_start=10,
            tiles="OpenStreetMap"
        )

        folium.PolyLine(
            locations=titik5,
            color="#1565C0",
            weight=6,
        ).add_to(m_final)

        for i, (lat, lon) in enumerate(titik5, start=1):
            numbered_marker(lat, lon, i).add_to(m_final)

        st.success("✅ Rute sudah disimpan & terkunci")

        st_folium(
            m_final,
            height=600,
            key=f"final_map_{map_key}"
        )

        if st.button(f"🔄 Reset Rute Tanggal {map_key}", key=f"reset_{map_key}"):
            del st.session_state[route_key]
            st.rerun()

        return {
            "tanggal": row.get("Tanggal Koordinat"),
            "awal": (lat1, lon1),
            "akhir": (lat2, lon2),
            "titik5": titik5
        }

    # =====================================================
    # MODE GAMBAR
    # =====================================================
    m = folium.Map(
        location=[(lat1 + lat2) / 2, (lon1 + lon2) / 2],
        zoom_start=10,
        tiles="OpenStreetMap"
    )

    folium.Marker(
        [lat1, lon1],
        tooltip="Start",
        icon=folium.Icon(color="green")
    ).add_to(m)

    folium.Marker(
        [lat2, lon2],
        tooltip="End",
        icon=folium.Icon(color="red")
    ).add_to(m)

    Draw(
        draw_options={
            "polyline": True,
            "polygon": False,
            "circle": False,
            "rectangle": False,
            "marker": False,
            "circlemarker": False,
        },
        edit_options={"edit": False}
    ).add_to(m)

    output = st_folium(
        m,
        height=600,
        key=f"draw_map_{map_key}",
        returned_objects=["all_drawings"]
    )

    if not output or not output.get("all_drawings"):
        st.info("Gambar rute dengan TEPAT 5 titik.")
        return None

    drawings = output["all_drawings"]

    polyline = None
    for obj in drawings:
        if obj.get("geometry", {}).get("type") == "LineString":
            polyline = obj

    if not polyline:
        st.warning("Objek harus berupa polyline.")
        return None

    coords = polyline["geometry"]["coordinates"]
    jumlah_titik = len(coords)

    st.write(f"Jumlah titik saat ini: **{jumlah_titik}**")

    if jumlah_titik != 5:
        st.error("❌ Rute harus TEPAT 5 titik. Silakan gambar ulang.")
        return None

    # Konversi lon,lat → lat,lon
    titik5 = [(pt[1], pt[0]) for pt in coords]

    if st.button(f"💾 Simpan Rute Tanggal {map_key}", key=f"save_{map_key}"):

        st.session_state[route_key] = titik5
        st.success("✅ Rute berhasil disimpan & dikunci.")
        st.rerun()

    return None
