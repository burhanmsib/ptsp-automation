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

    # =========================
    # PARSE KOORDINAT AWAL & AKHIR
    # =========================
    lat1, lon1 = parse_decimal_coordinate(row.get("Koordinat Awal (Desimal)"))
    lat2, lon2 = parse_decimal_coordinate(row.get("Koordinat Akhir (Desimal)"))

    if None in (lat1, lon1, lat2, lon2):
        st.error("Format koordinat desimal tidak valid.")
        return None

    st.caption(
        f"{row.get('Koordinat Awal')} ➜ {row.get('Koordinat Akhir')}"
    )

    # =========================
    # MAP AWAL (DRAW MODE)
    # =========================
    m = folium.Map(
        location=[(lat1 + lat2) / 2, (lon1 + lon2) / 2],
        zoom_start=7,
        tiles="OpenStreetMap"
    )

    folium.Marker(
        [lat1, lon1],
        tooltip="Start Point",
        icon=folium.Icon(color="green", icon="play")
    ).add_to(m)

    folium.Marker(
        [lat2, lon2],
        tooltip="End Point",
        icon=folium.Icon(color="red", icon="flag")
    ).add_to(m)

    Draw(
        draw_options={
            "polyline": {
                "shapeOptions": {
                    "color": "#1565C0",
                    "weight": 6,
                }
            },
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
        height=800,
        width=None,
        key=f"draw_map_{map_key}",
        returned_objects=["last_active_drawing"]
    )

    drawing = output.get("last_active_drawing")

    if drawing is None:
        st.info("Gambar rute dengan TEPAT 5 titik.")
        return None

    geom = drawing.get("geometry", {})

    if geom.get("type") != "LineString":
        st.warning("Objek harus berupa polyline.")
        return None

    coords = geom.get("coordinates", [])

    if len(coords) != REQUIRED_POINTS:
        st.error(f"Rute harus TEPAT {REQUIRED_POINTS} titik. Sekarang: {len(coords)} titik.")
        return None

    # lon,lat → lat,lon
    points_latlon = [(pt[1], pt[0]) for pt in coords]

    titik5 = split_route_into_5(points_latlon)

    if titik5 is None:
        st.error("Gagal membuat 5 titik.")
        return None

    # =========================
    # MAP FINAL
    # =========================
    m2 = folium.Map(
        location=[(lat1 + lat2) / 2, (lon1 + lon2) / 2],
        zoom_start=7,
        tiles="OpenStreetMap"
    )

    folium.PolyLine(
        locations=titik5,
        color="#1565C0",
        weight=6,
    ).add_to(m2)

    for i, (lat, lon) in enumerate(titik5, start=1):
        numbered_marker(lat, lon, i).add_to(m2)

    st.success("✅ Rute valid & tersimpan")

    st_folium(
        m2,
        height=800,
        width=None,
        key=f"final_map_{map_key}"
    )

    return {
        "tanggal": row.get("Tanggal Koordinat"),
        "awal": (lat1, lon1),
        "akhir": (lat2, lon2),
        "titik5": titik5
    }
