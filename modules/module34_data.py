# =========================
# MODULE 3 + 4
# WEATHER EXTRACTION & SAMPLING ENGINE
# (PRODUCTION STABLE VERSION – CLOUD SAFE)
# =========================

import re
import math
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
# DATE NORMALIZATION (SAFE)
# =========================

def normalize_date(raw):
    if raw is None or str(raw).strip() == "":
        return None

    s = str(raw)
    s = re.sub(r"\d{1,2}[.:]\d{2}(-\d{1,2}[.:]\d{2})?", "", s)

    try:
        return parser.parse(s, dayfirst=True, fuzzy=True)
    except:
        return None


# =========================
# URL BUILDER
# =========================

def ww3_url(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    base = "12" if dt.hour >= 12 else "00"

    return (
        f"https://{user}:{password}@maritim.bmkg.go.id/"
        f"opendap/ww3gfs/{YYYY}/{MM}/w3g_hires_{YYYY}{MM}{DD}_{base}00.nc"
    )


def fvcom_urls(dt, user, password):
    YYYY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    return [
        f"https://{user}:{password}@maritim.bmkg.go.id/"
        f"opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_1200.nc",
        f"https://{user}:{password}@maritim.bmkg.go.id/"
        f"opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_0000.nc",
    ]


# =========================
# SAFE OPEN DATASET (RETRY)
# =========================

def open_dataset_with_retry(url, retries=3, delay=2):

    for i in range(retries):
        try:
            return xr.open_dataset(url)
        except Exception as e:
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise e


# =========================
# CACHE DATASET PER URL
# =========================

@st.cache_resource(show_spinner=False)
def load_dataset_cached(url):
    return open_dataset_with_retry(url)


# =========================
# LOAD ALL DATASETS (STABLE)
# =========================

def load_datasets(dt_utc):

    user, password = get_bmkg_credentials()

    ww3 = ww3_url(dt_utc, user, password)
    fv_list = fvcom_urls(dt_utc, user, password)

    time.sleep(1)

    try:
        ds_wave = load_dataset_cached(ww3)
    except Exception as e:
        st.error("❌ Gagal membuka dataset WW3")
        st.exception(e)
        return None, None, None

    ds_cur = None
    for url in fv_list:
        try:
            time.sleep(1)
            ds_cur = load_dataset_cached(url)
            break
        except:
            continue

    if ds_cur is None:
        st.error("❌ FVCOM file tidak ditemukan")
        return None, None, None

    ds_rain = None
    try:
        ds_rain = load_gsmap(dt_utc)
    except:
        ds_rain = None

    return ds_wave, ds_cur, ds_rain


# =========================
# GSMaP FTP
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

    ftp = ftplib.FTP(ftp_host, timeout=20)
    ftp.set_pasv(True)
    ftp.login(ftp_user, ftp_pass)

    with open(tmp_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)

    ftp.quit()

    ds = xr.open_dataset(tmp_path)
    os.remove(tmp_path)

    return ds


# =========================
# SAFE EXTRACT (ASLI)
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

        if "lon" in da.coords:
            da = da.sel(lon=lon, method="nearest")

        return float(da.values)

    except:
        return None


# =========================
# HOURLY WEATHER EXTRACTION (TAMBAHAN)
# =========================

def extract_hourly_weather(ds_wave, ds_cur, ds_rain, t, lat, lon):

    rain_val = None

    if ds_rain is not None:
        try:
            rain_var = list(ds_rain.data_vars)[0]
            rain_val = safe_extract(ds_rain, rain_var, t, lat, lon)
        except:
            rain_val = None

    return {
        "wave": {
            "hs": safe_extract(ds_wave, "hs", t, lat, lon),
            "tp": safe_extract(ds_wave, "t01", t, lat, lon),
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

        # ===== hitung rainfall mean =====
        rain_vals = []

        for s in samples:
            rain_val = s.get("rain", {}).get("precip")
            if rain_val is not None:
                rain_vals.append(rain_val)

        rain_mean = float(np.mean(rain_vals)) if rain_vals else None

        # ===== klasifikasi weather =====
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
