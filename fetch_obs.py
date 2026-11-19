#!/usr/bin/env python3
"""
Downloads the Bureau of Meteorology's state observation files (IDx60920.xml:
every automatic weather station in each state, updated every 10 minutes)
and turns them into obs.json for the map page.

GitHub runs this automatically (see .github/workflows/update.yml).
Usage: python3 fetch_obs.py OUTPUT_FOLDER
"""
import json
import os
import shutil
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

STATE_PRODUCTS = {
    "NSW/ACT": "IDN60920", "VIC": "IDV60920", "QLD": "IDQ60920", "SA": "IDS60920",
    "WA": "IDW60920", "NT": "IDD60920", "TAS": "IDT60920",
}
SOURCES = [
    "ftp://ftp.bom.gov.au/anon/gen/fwo/{pid}.xml",
    "https://www.bom.gov.au/fwo/{pid}.xml",
    "http://www.bom.gov.au/fwo/{pid}.xml",
]
HEADERS = {"User-Agent": "Mozilla/5.0 (personal station-plot map; python urllib)"}

KMH_PER_KT = 1.852
CLOUD_WORDS = [  # BoM 'cloud' text -> oktas, used when 'cloud_oktas' isn't given
    ("overcast", 8), ("mostly cloudy", 6), ("partly cloudy", 4), ("mostly clear", 2),
    ("mostly sunny", 2), ("cloudy", 7), ("clear", 0), ("sunny", 0), ("fine", 0),
    ("few", 2), ("scattered", 4), ("broken", 6), ("fog", 8),
]


def local(tag):
    return tag.rsplit("}", 1)[-1]


def num(v):
    if v is None:
        return None
    v = v.strip()
    if v in ("", "-", "--"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def parse_state_xml(data, state):
    root = ET.fromstring(data)
    out = []
    for stn in root.iter():
        if local(stn.tag) != "station":
            continue
        a = stn.attrib
        lat, lon = num(a.get("lat")), num(a.get("lon"))
        if lat is None or lon is None:
            continue
        period = next((c for c in stn.iter() if local(c.tag) == "period"), None)
        el = {}
        for e in (period.iter() if period is not None else ()):
            if local(e.tag) == "element" and e.get("type") and e.get("type") not in el:
                el[e.get("type")] = (e.text or "").strip()

        wspd = num(el.get("wind_spd"))
        if wspd is None and num(el.get("wind_spd_kmh")) is not None:
            wspd = num(el.get("wind_spd_kmh")) / KMH_PER_KT
        gust = num(el.get("wind_gust_spd"))
        if gust is None and num(el.get("gust_kmh")) is not None:
            gust = num(el.get("gust_kmh")) / KMH_PER_KT
        wcard = el.get("wind_dir") or None
        wdir = num(el.get("wind_dir_deg"))
        if (wcard or "").upper() == "CALM":
            wspd, wdir = 0.0, None
        if wspd is not None and wspd > 0 and wdir is None:
            wdir = cardinal_to_deg(wcard)

        cloud = num(el.get("cloud_oktas"))
        cloud_text = el.get("cloud") or None
        if cloud is None and cloud_text:
            low = cloud_text.lower()
            cloud = next((o for w, o in CLOUD_WORDS if w in low), None)

        out.append({
            "id": a.get("bom-id") or a.get("wmo-id") or "",
            "wmo": a.get("wmo-id") or "",
            "name": a.get("description") or a.get("stn-name") or "",
            "type": a.get("type") or "",
            "state": state,
            "lat": lat, "lon": lon,
            "time_utc": period.get("time-utc") if period is not None else None,
            "time_local": nice_local(period.get("time-local")) if period is not None else "",
            "temp": num(el.get("air_temperature")),
            "dew": num(el.get("dew_point")),
            "rh": num(el.get("rel-humidity")),
            "wdir": int(round(wdir)) if wdir is not None else None,
            "wcard": wcard,
            "wspd": wspd,
            "gust": gust,
            "mslp": num(el.get("msl_pres")),
            "cloud": int(cloud) if cloud is not None else None,
            "cloud_text": cloud_text,
            "rain": num(el.get("rainfall")),
        })
    return out


CARDINALS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]


def cardinal_to_deg(c):
    c = (c or "").upper().strip()
    return CARDINALS.index(c) * 22.5 if c in CARDINALS else None


def nice_local(s):
    # "2026-11-19T21:30:00+11:00" -> "Thu 9:30pm"
    if not s:
        return ""
    try:
        from datetime import datetime
        d = datetime.fromisoformat(s)
        return d.strftime("%a ") + d.strftime("%I:%M%p").lstrip("0").lower()
    except ValueError:
        return s


def fetch_product(pid, from_dir):
    if from_dir:
        with open(os.path.join(from_dir, pid + ".xml"), "rb") as f:
            return f.read()
    errors = []
    for tpl in SOURCES:
        url = tpl.format(pid=pid)
        try:
            req = urllib.request.Request(url, headers=HEADERS) if url.startswith("http") else url
            with urllib.request.urlopen(req, timeout=45) as r:
                data = r.read()
            if b"<station" not in data:
                raise ValueError("file had no station data")
            return data
        except Exception as e:  # noqa: BLE001 - report every failure reason
            errors.append("{}: {}".format(url.split(":", 1)[0].upper(), e))
    raise RuntimeError(" | ".join(errors))



def fetch_with_retry(pid):
    try:
        return fetch_product(pid, None)
    except Exception:
        time.sleep(10)
        return fetch_product(pid, None)


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "site"
    os.makedirs(out_dir, exist_ok=True)
    stations, errors, seen = [], {}, set()

    def job(item):
        state, pid = item
        try:
            return state, parse_state_xml(fetch_with_retry(pid), state), None
        except Exception as e:  # noqa: BLE001
            return state, [], str(e)

    with ThreadPoolExecutor(max_workers=7) as ex:
        for state, rows, err in ex.map(job, STATE_PRODUCTS.items()):
            print("{:8} {}".format(state, "FAILED: " + err if err else "{} stations".format(len(rows))))
            if err:
                errors[state] = err
            for r in rows:
                key = r["id"] or (r["lat"], r["lon"])
                if key not in seen:
                    seen.add(key)
                    stations.append(r)

    if not stations:
        sys.exit("No stations downloaded from BoM, so the website was left as it was.")
    payload = {"fetched": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "stations": stations, "errors": errors}
    with open(os.path.join(out_dir, "obs.json"), "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    here = os.path.dirname(os.path.abspath(__file__))
    shutil.copy(os.path.join(here, "index.html"), os.path.join(out_dir, "index.html"))
    print("Wrote {} stations to {}".format(len(stations), out_dir))


if __name__ == "__main__":
    main()
