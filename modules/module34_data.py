# =========================
# MODULE 3 + 4
# WEATHER EXTRACTION & SAMPLING ENGINE
# ROBUST TIMEOUT VERSION
# =========================

import re
import numpy as np
import xarray as xr
import streamlit as st
import ftplib
import tempfile
import os
import time

from datetime import datetime, timedelta, timezone
from dateutil import parser


# =========================
# CONSTANTS
# =========================
TZ_OFFSET = {
    "WIB": 7,
    "WITA": 8,
    "WIT": 9
}


# =========================
# LOAD BMKG CREDENTIAL
# =========================
def get_bmkg_credentials():
    return (
        st.secrets["bmkg"]["user"],
        st.secrets["bmkg"]["pass"]
    )


# =========================
# DATE NORMALIZATION
# =========================
def normalize_date(raw):
    if raw is None or str(raw).strip() == "":
        return None

    s = str(raw)
    s = re.sub(r"\d{1,2}[.:]\d{2}(-\d{1,2}[.:]\d{2})?", "", s)
    s = s.replace("/", " ")

    month_map = {
        "Januari": "January", "Februari": "February", "Maret": "March",
        "April": "April", "Mei": "May", "Juni": "June", "Juli": "July",
        "Agustus": "August", "September": "September",
        "Oktober": "October", "November": "November", "Desember": "December"
    }

    for indo, eng in month_map.items():
        s = s.replace(indo, eng)

    s = s.strip()

    formats = [
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %B %Y",
        "%B %d, %Y",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            continue

    try:
        return parser.parse(s, dayfirst=True)
    except:
        return None


# =========================
# URL BUILDERS
# =========================
def ww3_urls(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    return [
        f"https://{user}:{password}@maritim.bmkg.go.id/opendap/ww3gfs/{YYYY}/{MM}/w3g_hires_{YYYY}{MM}{DD}_1200.nc",
        f"https://{user}:{password}@maritim.bmkg.go.id/opendap/ww3gfs/{YYYY}/{MM}/w3g_hires_{YYYY}{MM}{DD}_0000.nc",
    ]


def fvcom_urls(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    return [
        f"https://{user}:{password}@maritim.bmkg.go.id/opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_1200.nc",
        f"https://{user}:{password}@maritim.bmkg.go.id/opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_0000.nc",
    ]


# =========================
# SAFE DATASET OPEN WITH RETRY
# =========================
def open_dataset(url, max_retries=3, sleep_sec=2):
    last_error = None

    for attempt in range(max_retries):
        try:
            ds = xr.open_dataset(
                url,
                engine="netcdf4",
                decode_times=True
            )
            return ds
        except Exception as e:
            last_error = e
            time.sleep(sleep_sec)

    return None


# =========================
# GSMAP FTP
# =========================
def load_gsmap(dt):
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["pass"]

    Y = dt.strftime("%Y")
    M = dt.strftime("%m")
    D = dt.strftime("%d")
    H = dt.strftime("%H")

    remote_path = f"/himawari6/GSMaP/netcdf/{Y}/{M}/{D}/GSMaP_{Y}{M}{D}{H}00.nc"

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".nc")
    tmp_path = tmp.name
    tmp.close()

    ftp = ftplib.FTP(timeout=30)
    ftp.connect(ftp_host, 21)
    ftp.login(ftp_user, ftp_pass)

    with open(tmp_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)

    ftp.quit()

    ds = xr.open_dataset(tmp_path)

    try:
        ds.load()
    except:
        pass

    try:
        os.remove(tmp_path)
    except:
        pass

    return ds


# =========================
# LOAD DATASETS CACHED
# =========================
@st.cache_resource(show_spinner=False)
def load_datasets_cached(dt_utc_str):
    dt_utc = datetime.fromisoformat(dt_utc_str)
    user, password = get_bmkg_credentials()

    # ---------- WW3 ----------
    ds_wave = None
    for url in ww3_urls(dt_utc, user, password):
        ds_wave = open_dataset(url, max_retries=2, sleep_sec=1)
        if ds_wave is not None:
            break

    # ---------- FVCOM ----------
    ds_cur = None
    for url in fvcom_urls(dt_utc, user, password):
        ds_cur = open_dataset(url, max_retries=2, sleep_sec=1)
        if ds_cur is not None:
            break

    # ---------- GSMAP ----------
    ds_rain = None
    try:
        ds_rain = load_gsmap(dt_utc)
    except:
        ds_rain = None

    return ds_wave, ds_cur, ds_rain


def load_datasets(dt_utc):
    try:
        ds_wave, ds_cur, ds_rain = load_datasets_cached(dt_utc.isoformat())
    except Exception as e:
        st.error(f"❌ Gagal load dataset: {e}")
        return None, None, None

    if ds_wave is None:
        st.error("❌ Dataset WW3 tidak ditemukan / timeout")
        return None, None, None

    if ds_cur is None:
        st.error("❌ Dataset FVCOM tidak ditemukan / timeout")
        return None, None, None

    return ds_wave, ds_cur, ds_rain


# =========================
# HELPER
# =========================
def find_coord_name(ds, candidates):
    for name in candidates:
        if name in ds.coords:
            return name
        if name in ds.variables:
            return name
    return None


def select_time_safe(da, t):
    if "time" not in da.dims and "time" not in da.coords:
        return da

    try:
        return da.sel(time=t, method="nearest")
    except:
        try:
            time_vals = da["time"].values
            idx = int(np.argmin(np.abs(time_vals - np.datetime64(t))))
            return da.isel(time=idx)
        except:
            return da


# =========================
# EXTRACTION REGULAR GRID
# =========================
def extract_from_regular_grid(da, ds, lat, lon):
    lat_name = find_coord_name(ds, ["lat", "latitude", "Latitude", "LAT"])
    lon_name = find_coord_name(ds, ["lon", "longitude", "Longitude", "LON"])

    if lat_name is None or lon_name is None:
        return None

    try:
        val = da.sel({lat_name: lat, lon_name: lon}, method="nearest").values
        val = float(np.asarray(val).squeeze())
        if np.isnan(val):
            return None
        return val
    except:
        return None


# =========================
# EXTRACTION UNSTRUCTURED GRID
# =========================
def extract_from_unstructured_grid(da, ds, lat, lon):
    lat_name = find_coord_name(ds, ["lat", "latc", "latitude", "y", "yc"])
    lon_name = find_coord_name(ds, ["lon", "lonc", "longitude", "x", "xc"])

    if lat_name is None or lon_name is None:
        return None

    try:
        lat_vals = np.asarray(ds[lat_name].values).squeeze()
        lon_vals = np.asarray(ds[lon_name].values).squeeze()

        if lat_vals.ndim != 1 or lon_vals.ndim != 1:
            return None

        dist2 = (lat_vals - lat) ** 2 + (lon_vals - lon) ** 2
        nearest_idx = int(np.argmin(dist2))

        node_dim = None
        for d in da.dims:
            if da.sizes.get(d, None) == len(lat_vals):
                node_dim = d
                break

        if node_dim is None:
            for cand in ["node", "nele", "cell"]:
                if cand in da.dims:
                    node_dim = cand
                    break

        if node_dim is None:
            return None

        val = da.isel({node_dim: nearest_idx}).values
        val = float(np.asarray(val).squeeze())

        if np.isnan(val):
            return None

        return val
    except:
        return None


# =========================
# SAFE DATA EXTRACTION
# =========================
def safe_extract(ds, var, t, lat, lon, depth=None):
    if ds is None or var not in ds:
        return 0.0

    try:
        da = ds[var]
        da = select_time_safe(da, t)

        if depth is not None:
            for depth_name in ["depth", "siglay", "siglev"]:
                if depth_name in da.dims or depth_name in da.coords:
                    try:
                        da = da.sel({depth_name: depth}, method="nearest")
                        break
                    except:
                        try:
                            da = da.isel({depth_name: 0})
                            break
                        except:
                            pass

        val = extract_from_regular_grid(da, ds, lat, lon)
        if val is not None:
            return val

        val = extract_from_unstructured_grid(da, ds, lat, lon)
        if val is not None:
            return val

        return 0.0

    except:
        return 0.0


# =========================
# WEATHER CLASSIFICATION
# =========================
def classify_weather_bmkg(rain_mm):
    if rain_mm is None:
        return "Unknown"
    if rain_mm < 1:
        return "Clear"
    if rain_mm < 5:
        return "Slight Rain"
    if rain_mm < 10:
        return "Moderate Rain"
    if rain_mm < 20:
        return "Heavy Rain"
    return "Heavy Rain with Thunderstorm"


# =========================
# WEATHER EXTRACTION
# =========================
def extract_hourly_weather(ds_wave, ds_cur, ds_rain, t, lat, lon):
    rain_val = None

    if ds_rain is not None:
        try:
            var = list(ds_rain.data_vars)[0]
            da = ds_rain[var]

            if "time" in da.dims:
                da = da.sel(time=t, method="nearest")

            lat_name = None
            for name in ["lat", "latitude"]:
                if name in da.coords:
                    lat_name = name
                    break

            lon_name = None
            for name in ["lon", "longitude"]:
                if name in da.coords:
                    lon_name = name
                    break

            if lat_name and lon_name:
                lat_vals = da[lat_name].values
                lon_vals = da[lon_name].values

                lat_idx = np.abs(lat_vals - lat).argmin()
                lon_idx = np.abs(lon_vals - lon).argmin()

                da = da.isel({lat_name: lat_idx, lon_name: lon_idx})
                rain_val = float(np.asarray(da.values).squeeze())

                if np.isnan(rain_val):
                    rain_val = None
        except:
            rain_val = None

    return {
        "wave": {
            "hs": safe_extract(ds_wave, "hs", t, lat, lon),
            "tp": safe_extract(ds_wave, "t01", t, lat, lon),
            "dir": safe_extract(ds_wave, "dir", t, lat, lon)
        },
        "wind": {
            "u": safe_extract(ds_wave, "uwnd", t, lat, lon),
            "v": safe_extract(ds_wave, "vwnd", t, lat, lon)
        },
        "current": {
            "u": safe_extract(ds_cur, "u", t, lat, lon, depth=0.5),
            "v": safe_extract(ds_cur, "v", t, lat, lon, depth=0.5)
        },
        "rain": {
            "precip": rain_val
        }
    }


# =========================
# GENERATE POINTS ALONG ROUTE
# =========================
def generate_points_along_segment(p1, p2, n_points=3):
    points = []

    if n_points < 2:
        return [p1, p2]

    for i in range(n_points):
        frac = i / (n_points - 1)
        lat = p1[0] + (p2[0] - p1[0]) * frac
        lon = p1[1] + (p2[1] - p1[1]) * frac
        points.append((lat, lon))

    return points


# =========================
# MAIN ENTRY
# =========================
def process_module34(row, polyline, tz="WIB"):
    dt_local = normalize_date(row["Tanggal Koordinat"])
    if dt_local is None:
        return None

    tz_offset = TZ_OFFSET.get(tz, 7)

    dt_local = dt_local.replace(hour=0, minute=0, second=0, microsecond=0)

    dt_utc0 = dt_local.replace(
        tzinfo=timezone(timedelta(hours=tz_offset))
    ).astimezone(timezone.utc).replace(tzinfo=None)

    ds_wave, ds_cur, ds_rain = load_datasets(dt_utc0)

    if ds_wave is None or ds_cur is None:
        return None

    route = [(p[0], p[1]) for p in polyline]

    if len(route) < 5:
        return None

    segments = []

    for i in range(4):
        start = route[i]
        end = route[i + 1]

        t0 = dt_utc0 + timedelta(hours=i * 6)
        t3 = t0 + timedelta(hours=3)

        points = generate_points_along_segment(start, end, n_points=3)

        samples = []

        for lat, lon in points:
            try:
                sample0 = extract_hourly_weather(ds_wave, ds_cur, ds_rain, t0, lat, lon)
                sample3 = extract_hourly_weather(ds_wave, ds_cur, ds_rain, t3, lat, lon)

                samples.append(sample0)
                samples.append(sample3)
            except:
                continue

        if not samples:
            samples = [{
                "wave": {"hs": 0.0, "tp": 0.0, "dir": 0.0},
                "wind": {"u": 0.0, "v": 0.0},
                "current": {"u": 0.0, "v": 0.0},
                "rain": {"precip": None}
            }]

        rain_vals = [
            s["rain"]["precip"]
            for s in samples
            if s["rain"]["precip"] is not None
        ]

        rain_mean = float(np.mean(rain_vals)) if rain_vals else None
        weather_class = classify_weather_bmkg(rain_mean)

        segments.append({
            "interval": f"T{i*6}-T{(i+1)*6}",
            "samples": samples,
            "rain_mean": rain_mean,
            "weather": weather_class
        })

    return {
        "tanggal": dt_local,
        "tz": tz,
        "segments": segments
    }
