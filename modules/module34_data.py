# =========================
# MODULE 3 + 4
# WEATHER EXTRACTION & SAMPLING ENGINE
# (FINAL – STABLE VERSION)
# =========================

import re
import math
import numpy as np
import xarray as xr
import streamlit as st
import ftplib
import tempfile
import os

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
    try:
        return (
            st.secrets["bmkg"]["user"],
            st.secrets["bmkg"]["pass"]
        )
    except KeyError:
        raise RuntimeError("BMKG credential belum diset di secrets.")


# =========================
# DATE NORMALIZATION (STABLE)
# =========================

def normalize_date(raw):
    if raw is None or str(raw).strip() == "":
        return None

    s = str(raw)

    # Hapus jam (noise ekstraksi)
    s = re.sub(r"\d{1,2}[.:]\d{2}(-\d{1,2}[.:]\d{2})?", "", s)

    try:
        return parser.parse(s, dayfirst=True, fuzzy=True)
    except:
        return None


# =========================
# DATASET URL BUILDER
# =========================

def ww3_url(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    base = "12" if dt.hour >= 12 else "00"

    return (
        f"https://{user}:{password}@maritim.bmkg.go.id/"
        f"opendap/ww3gfs/{YYYY}/{MM}/w3g_hires_{YYYY}{MM}{DD}_{base}00.nc"
    )


def fvcom_url(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    base_list = ["1200", "0000"]

    for base in base_list:
        url = (
            f"https://{user}:{password}@maritim.bmkg.go.id/"
            f"opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_{base}.nc"
        )
        try:
            xr.open_dataset(url).close()
            return url
        except:
            continue

    raise RuntimeError(f"FVCOM file tidak ditemukan untuk {YYYY}-{MM}-{DD}")


# =========================
# CACHED DATASET LOADER (VERSI LAMA YANG STABIL)
# =========================

@st.cache_resource(show_spinner=False)
def load_datasets_cached(ww3_url_str, fvcom_url_str):
    ds_wave = xr.open_dataset(ww3_url_str)
    ds_cur = xr.open_dataset(fvcom_url_str)
    return ds_wave, ds_cur


# =========================
# GSMaP FTP DOWNLOAD
# =========================

def download_gsmap_ftp(dt):

    try:
        ftp_host = st.secrets["ftp"]["host"]
        ftp_user = st.secrets["ftp"]["user"]
        ftp_pass = st.secrets["ftp"]["pass"]
    except:
        ftp_host = "202.90.199.64"
        ftp_user = "metmar2"
        ftp_pass = "780!a0C=i&"

    Y = dt.strftime("%Y")
    M = dt.strftime("%m")
    D = dt.strftime("%d")
    H = dt.strftime("%H")

    remote_path = f"/himawari6/GSMaP/netcdf/{Y}/{M}/{D}/GSMaP_{Y}{M}{D}{H}00.nc"

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".nc")
    tmp_path = tmp_file.name
    tmp_file.close()

    try:
        ftp = ftplib.FTP(ftp_host)
        ftp.login(ftp_user, ftp_pass)

        with open(tmp_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)

        ftp.quit()
        return tmp_path

    except:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return None


# =========================
# LOAD ALL DATASETS
# =========================

def load_datasets(dt_utc):

    user, password = get_bmkg_credentials()

    ww3 = ww3_url(dt_utc, user, password)
    fvcom = fvcom_url(dt_utc, user, password)

    try:
        ds_wave, ds_cur = load_datasets_cached(ww3, fvcom)
    except:
        st.error("❌ Gagal membuka dataset WW3/FVCOM")
        return None, None, None

    gsmap_file = download_gsmap_ftp(dt_utc)

    if gsmap_file is None:
        return ds_wave, ds_cur, None

    try:
        ds_rain = xr.open_dataset(gsmap_file)
    except:
        ds_rain = None

    if os.path.exists(gsmap_file):
        os.remove(gsmap_file)

    return ds_wave, ds_cur, ds_rain


# =========================
# SAFE GRID EXTRACTION (VERSI LAMA)
# =========================

def safe_extract(ds, var, t, lat, lon, depth=None):

    if ds is None or var not in ds:
        return None

    try:
        da = ds[var]

        if "time" in da.dims:
            da = da.sel(time=t, method="nearest")

        if depth is not None and "depth" in da.dims:
            da = da.sel(depth=depth, method="nearest")

        if "lat" in da.coords:
            da = da.sel(lat=lat, method="nearest")
        elif "latitude" in da.coords:
            da = da.sel(latitude=lat, method="nearest")

        if "lon" in da.coords:
            da = da.sel(lon=lon, method="nearest")
        elif "longitude" in da.coords:
            da = da.sel(longitude=lon, method="nearest")

        return float(da.values)

    except:
        return None


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
# HOURLY WEATHER EXTRACTION
# =========================

def extract_hourly_weather(ds_wave, ds_cur, ds_rain, t, lat, lon):

    rain_val = safe_extract(
        ds_rain,
        list(ds_rain.data_vars)[0],
        t, lat, lon
    ) if ds_rain else None

    return {
        "wave": {
            "hs":  safe_extract(ds_wave, "hs",  t, lat, lon),
            "tp":  safe_extract(ds_wave, "t01", t, lat, lon),
            "dir": safe_extract(ds_wave, "dir", t, lat, lon),
        },
        "wind": {
            "u": safe_extract(ds_wave, "uwnd", t, lat, lon),
            "v": safe_extract(ds_wave, "vwnd", t, lat, lon),
        },
        "current": {
            "u": safe_extract(ds_cur, "u", t, lat, lon, depth=0),
            "v": safe_extract(ds_cur, "v", t, lat, lon, depth=0),
        },
        "rain": {
            "precip": rain_val
        }
    }


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

    segments = []

    for i in range(4):

        lat, lon = route[min(i, len(route) - 1)]

        t0 = dt_utc0 + timedelta(hours=i * 6)
        t3 = t0 + timedelta(hours=3)

        sample0 = extract_hourly_weather(ds_wave, ds_cur, ds_rain, t0, lat, lon)
        sample3 = extract_hourly_weather(ds_wave, ds_cur, ds_rain, t3, lat, lon)

        samples = [sample0, sample3]

        rain_vals = [
            s.get("rain", {}).get("precip")
            for s in samples
            if s.get("rain", {}).get("precip") is not None
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
