# =========================
# MODULE 5 : WEATHER ANALYSIS ENGINE
# (VERSI IDENTIK IPYNB)
# =========================

import math
from datetime import datetime, timedelta, timezone

# -------------------------
# CONSTANTS
# -------------------------
COMPASS = [
    "North", "Northeast", "East", "Southeast",
    "South", "Southwest", "West", "Northwest"
]

TZ_OFFSET = {
    "WIB": 7,
    "WITA": 8,
    "WIT": 9
}

# -------------------------
# BASIC UTILITIES
# -------------------------
def normalize_deg(d):
    if d is None or (isinstance(d, float) and math.isnan(d)):
        return None
    return d % 360


def deg_to_compass(deg):
    if deg is None or (isinstance(deg, float) and math.isnan(deg)):
        return None
    idx = int((deg + 22.5) // 45) % 8
    return COMPASS[idx]


def ms_to_knots(ms):
    if ms is None or (isinstance(ms, float) and math.isnan(ms)):
        return None
    return ms * 1.94384449

# ===============================
# WEATHER CLASSIFICATION (BMKG)
# ===============================
def classify_weather_bmkg(rain_mm):
    """
    rain_mm dalam mm/hour (GSMaP)
    """
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


def rainfall_range_text(values):
    """
    GSMaP hourly rainfall (mm/hour)
    """
    if not values:
        return "No Data"

    rmin = round(min(values), 1)
    rmax = round(max(values), 1)

    if rmin == rmax:
        return f"{rmin} mm/hour"

    return f"{rmin} - {rmax} mm/hour"

# -------------------------
# VECTOR → DIRECTION
# -------------------------
def uv_to_dir_from(u, v):
    """
    Wind direction FROM (meteorological)
    """
    if (
        u is None or v is None
        or (isinstance(u, float) and math.isnan(u))
        or (isinstance(v, float) and math.isnan(v))
    ):
        return None
    return normalize_deg(math.degrees(math.atan2(-u, -v)))


def uv_to_dir_to(u, v):
    """
    Current direction TO (oceanographic)
    """
    if (
        u is None or v is None
        or (isinstance(u, float) and math.isnan(u))
        or (isinstance(v, float) and math.isnan(v))
    ):
        return None
    return normalize_deg(math.degrees(math.atan2(u, v)))


# -------------------------
# CLOCKWISE DIRECTION RULE
# -------------------------
def clockwise_span(start, end):
    if start is None or end is None:
        return None
    return (end - start) % 360


def limit_direction(start, end, max_span=90):
    if start is None or end is None:
        return None

    span = clockwise_span(start, end)
    if span is None:
        return None

    if span <= max_span:
        return end

    return (start + max_span) % 360


def format_direction_range(start_deg, end_deg):
    if start_deg is None or end_deg is None:
        return "Variable"

    start_label = deg_to_compass(start_deg)
    end_label = deg_to_compass(end_deg)

    if start_label is None or end_label is None:
        return "Variable"

    if start_label == end_label:
        return start_label

    return f"{start_label} - {end_label}"


# -------------------------
# WAVE CATEGORY (BMKG)
# -------------------------
def wave_category(hs):
    if hs is None or (isinstance(hs, float) and math.isnan(hs)):
        return "Unknown"
    if hs < 0.5:
        return "Smooth"
    elif hs < 1.25:
        return "Slight"
    elif hs < 2.5:
        return "Moderate"
    elif hs < 4.0:
        return "Rough"
    elif hs < 6.0:
        return "Very Rough"
    elif hs < 9.0:
        return "High"
    else:
        return "Very High"


def wave_category_range(hs_values):
    """
    Contoh:
    Smooth
    Smooth to Slight
    Moderate to Rough
    """
    if not hs_values:
        return "Unknown"

    hs_min = min(hs_values)
    hs_max = max(hs_values)

    cat_min = wave_category(hs_min)
    cat_max = wave_category(hs_max)

    if cat_min == cat_max:
        return cat_min

    return f"{cat_min} to {cat_max}"

# -------------------------
# BEAUFORT SCALE
# -------------------------
def beaufort_from_knots(k):
    if k is None or (isinstance(k, float) and math.isnan(k)):
        return None

    if k < 1:
        return 0
    elif k <= 3:
        return 1
    elif k <= 6:
        return 2
    elif k <= 10:
        return 3
    elif k <= 16:
        return 4
    elif k <= 21:
        return 5
    elif k <= 27:
        return 6
    elif k <= 33:
        return 7
    elif k <= 40:
        return 8
    elif k <= 47:
        return 9
    elif k <= 55:
        return 10
    elif k <= 63:
        return 11
    else:
        return 12


def beaufort_range_from_knots(min_knot, max_knot):
    if min_knot is None or max_knot is None:
        return "N/A"

    bf_min = beaufort_from_knots(min_knot)
    bf_max = beaufort_from_knots(max_knot)

    if bf_min is None or bf_max is None:
        return "N/A"

    if bf_min == bf_max:
        return str(bf_min)

    return f"{bf_min} - {bf_max}"


# -------------------------
# HELPER RANGE ROUNDING
# -------------------------
def rounded_range_with_padding(min_val, max_val):
    if min_val is None or max_val is None:
        return None, None

    r_min = round(min_val)
    r_max = round(max_val)

    if r_min == r_max:
        r_max = r_min + 1

    return r_min, r_max


# -------------------------
# CORE ANALYSIS (6 JAM)
# -------------------------
def analyze_segment(samples):
    if not samples:
        return {
            "WEATHER": "Unknown",
            "WIND": "Variable",
            "CURRENT": "Variable",
            "WAVE": "Unknown",
            "BEAUFORT": "N/A"
        }

    # ===== RAINFALL (WEATHER SEKARANG BERDASARKAN HUJAN) =====
    rain_vals = [
        s.get("rain", {}).get("precip")
        for s in samples
        if s.get("rain", {}).get("precip") is not None
    ]

    rain_max = max(rain_vals) if rain_vals else None
    weather_txt = classify_weather_bmkg(rain_max)

    # ===== WAVE =====
    hs_vals = [
        s["wave"]["hs"]
        for s in samples
        if s.get("wave")
        and s["wave"].get("hs") is not None
        and not (isinstance(s["wave"]["hs"], float) and math.isnan(s["wave"]["hs"]))
    ]

    wave_txt = wave_category_range(hs_vals)

    # ===== WIND =====
    wind_dirs, wind_spds = [], []

    for s in samples:
        u = s.get("wind", {}).get("u")
        v = s.get("wind", {}).get("v")

        d = uv_to_dir_from(u, v)
        spd = ms_to_knots(math.hypot(u, v)) if u is not None and v is not None else None

        if d is not None and spd is not None:
            wind_dirs.append(d)
            wind_spds.append(spd)

    wind_txt = "Variable"
    beaufort = "N/A"

    if wind_dirs and wind_spds:
        d_start = wind_dirs[0]
        d_end = limit_direction(d_start, wind_dirs[-1])
        dir_txt = format_direction_range(d_start, d_end)

        w_min, w_max = rounded_range_with_padding(
            min(wind_spds), max(wind_spds)
        )

        wind_txt = f"{dir_txt}, {w_min} - {w_max} knots"
        beaufort = beaufort_range_from_knots(w_min, w_max)

    # ===== CURRENT =====
    cur_dirs, cur_spds = [], []

    for s in samples:
        u = s.get("current", {}).get("u")
        v = s.get("current", {}).get("v")

        d = uv_to_dir_to(u, v)
        spd = math.hypot(u, v) if u is not None and v is not None else None

        if d is not None and spd is not None:
            cur_dirs.append(d)
            cur_spds.append(spd)

    cur_txt = "Variable"
    if cur_dirs and cur_spds:
        d_start = cur_dirs[0]
        d_end = limit_direction(d_start, cur_dirs[-1])
        dir_txt = format_direction_range(d_start, d_end)

        c_min, c_max = rounded_range_with_padding(
            min(cur_spds), max(cur_spds)
        )

        cur_txt = f"{dir_txt}, {c_min} - {c_max} cm/s"

    return {
        "WEATHER": weather_txt,
        "WIND": wind_txt,
        "CURRENT": cur_txt,
        "WAVE": wave_txt,
        "BEAUFORT": beaufort
    }


# -------------------------
# LOCAL TIME BUILDER
# -------------------------
def build_local_times(date_utc, tz):
    if tz not in TZ_OFFSET:
        tz = "WIB"

    base_utc = datetime(
        date_utc.year, date_utc.month, date_utc.day,
        18, 0, 0, tzinfo=timezone.utc
    )

    local_start = base_utc + timedelta(hours=TZ_OFFSET[tz])

    labels = []
    for i in range(4):
        s = local_start + timedelta(hours=6 * i)
        e = s + timedelta(hours=6)
        labels.append(f"{s.strftime('%H.%M')} – {e.strftime('%H.%M')}")

    return labels


# -------------------------
# MAIN ENTRY
# -------------------------
def process_module5(results_module34, tz="WIB"):
    output = []

    for item in results_module34:
        if item is None:
            output.append(None)
            continue

        tanggal = item["tanggal"]
        times = build_local_times(tanggal, tz)

        rows = []
        for i, seg in enumerate(item["segments"]):
            result = analyze_segment(seg["samples"])
            rows.append({
                "DATE": tanggal.strftime("%b %d, %Y"),
                "LOCAL TIME": times[i],
                **result
            })

        output.append({
            "tanggal": tanggal,
            "tz": tz,
            "intervals": rows
        })

    return output