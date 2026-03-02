# =========================
# MODULE 3 + 4
# WEATHER EXTRACTION & SAMPLING ENGINE
# (CLOUD OPTIMIZED – SAFE + FVCOM 1200/0000)
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
from concurrent.futures import ThreadPoolExecutor


# =========================
# CONSTANTS
# =========================

MONTH_ID = {
    "januari": "January", "februari": "February", "maret": "March",
    "april": "April", "mei": "May", "juni": "June", "juli": "July",
    "agustus": "August", "september": "September",
    "oktober": "October", "november": "November", "desember": "December",
}

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
# DATE NORMALIZATION
# =========================

def normalize_date(raw):
    if raw is None or str(raw).strip() == "":
        return None

    txt = str(raw).lower()

    for k in ["sd", "to", "-"]:
        if k in txt:
            txt = txt.split(k)[0]

    for indo, eng in MONTH_ID.items():
        txt = txt.replace(indo, eng.lower())

    txt = re.sub(r"\d{1,2}[:.]\d{2}.*", "", txt).strip()

    try:
        return parser.parse(txt, dayfirst=True, fuzzy=True)
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

    urls = []

    for base in base_list:
        urls.append(
            f"https://{user}:{password}@maritim.bmkg.go.id/"
            f"opendap/fvcom/{YYYY}/{MM}/InaFlows_{YYYY}{MM}{DD}_{base}.nc"
        )

    return urls


# =========================
# CACHED DATASET LOADER (OPTIMIZED)
# =========================

@st.cache_resource(ttl=7200, show_spinner=False)
def load_single_dataset(url):
    return xr.open_dataset(
        url,
        engine="netcdf4",
        decode_times=False
    )


# =========================
# GSMaP FTP DOWNLOAD (OPTIMIZED)
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
        ftp = ftplib.FTP(ftp_host, timeout=15)
        ftp.set_pasv(True)
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
# LOAD ALL DATASETS (OPTIMIZED)
# =========================

def load_datasets(dt_utc):

    user, password = get_bmkg_credentials()

    ww3 = ww3_url(dt_utc, user, password)
    fvcom_urls = fvcom_url(dt_utc, user, password)

    # Load WW3
    try:
        ds_wave = load_single_dataset(ww3)
    except Exception as e:
        st.error("❌ Gagal membuka dataset WW3")
        st.exception(e)
        return None, None, None

    # Load FVCOM (1200 -> 0000 fallback)
    ds_cur = None
    for url in fvcom_urls:
        try:
            ds_cur = load_single_dataset(url)
            break
        except:
            continue

    if ds_cur is None:
        st.error("❌ FVCOM file tidak ditemukan (1200 & 0000 gagal)")
        return None, None, None

    # GSMaP
    gsmap_file = download_gsmap_ftp(dt_utc)

    ds_rain = None
    if gsmap_file:
        try:
            ds_rain = xr.open_dataset(gsmap_file, engine="netcdf4", decode_times=False)
        except:
            ds_rain = None

        if os.path.exists(gsmap_file):
            os.remove(gsmap_file)

    return ds_wave, ds_cur, ds_rain


# =========================
# SAFE GRID EXTRACTION (FASTER, SAME RESULT)
# =========================

def safe_extract(ds, var, t, lat, lon, depth=None):

    if ds is None or var not in ds:
        return None

    try:
        da = ds[var]

        # TIME
        if "time" in da.dims:
            time_vals = ds["time"].values
            t_idx = np.abs(time_vals - np.datetime64(t)).argmin()
            da = da.isel(time=t_idx)

        # DEPTH
        if depth is not None and "depth" in da.dims:
            da = da.isel(depth=0)

        # LAT
        if "lat" in ds.coords:
            lat_vals = ds["lat"].values
            lat_idx = np.abs(lat_vals - lat).argmin()
            da = da.isel(lat=lat_idx)
        elif "latitude" in ds.coords:
            lat_vals = ds["latitude"].values
            lat_idx = np.abs(lat_vals - lat).argmin()
            da = da.isel(latitude=lat_idx)

        # LON
        if "lon" in ds.coords:
            lon_vals = ds["lon"].values
            lon_idx = np.abs(lon_vals - lon).argmin()
            da = da.isel(lon=lon_idx)
        elif "longitude" in ds.coords:
            lon_vals = ds["longitude"].values
            lon_idx = np.abs(lon_vals - lon).argmin()
            da = da.isel(longitude=lon_idx)

        return float(da.values)

    except:
        return None


# ===============================
# WEATHER CLASSIFICATION
# ===============================

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

        rain_vals = []

        for s in samples:
            rain_val = s.get("rain", {}).get("precip")
            if rain_val is not None:
                rain_vals.append(rain_val)

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
