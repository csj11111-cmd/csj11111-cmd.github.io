#!/usr/bin/env python3
"""Build cameras.json from OpenStreetMap (Overpass API) + optional
공공데이터포털 standard CSV.

Run from any directory; writes ../host/cameras.json (or path in --out).

Sources:
  1. OSM: highway=speed_camera | enforcement=maxspeed within Korea bbox.
     Maxspeed is read from `maxspeed` tag; defaults to 80 km/h if missing.
  2. (Optional) data.go.kr CSV path passed via --kma-csv. Skip if absent.

Output: cameras.json with shape:
  { _comment, _version, _updatedAt, cameras: [{id,name,lat,lon,limitKmh,heading,source}] }

The script de-duplicates cameras within 30 m and keeps the first source seen.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import sys
import urllib.parse
import urllib.request

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
KOREA_BBOX = "33.0,124.0,38.7,131.5"  # south, west, north, east


def fetch_osm() -> list[dict]:
    query = (
        "[out:json][timeout:60];"
        "("
        f' node["highway"="speed_camera"]({KOREA_BBOX});'
        f' node["enforcement"="maxspeed"]({KOREA_BBOX});'
        ");"
        "out body;"
    )
    url = OVERPASS_URL + "?" + urllib.parse.urlencode({"data": query})
    req = urllib.request.Request(url, headers={
        "User-Agent": "DriveCluster/1.0 (cameras-builder)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = json.load(resp)

    out: list[dict] = []
    for el in payload.get("elements", []):
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags", {}) or {}
        # parse maxspeed
        ms = tags.get("maxspeed") or tags.get("maxspeed:practical") or ""
        try:
            limit = int("".join(ch for ch in str(ms).split()[0] if ch.isdigit()))
            if limit <= 0:
                limit = 80
        except Exception:
            limit = 80
        # heading
        heading = None
        if "direction" in tags:
            try:
                heading = float(tags["direction"])
            except Exception:
                pass
        out.append({
            "id": f"osm-{el.get('id')}",
            "name": tags.get("name") or tags.get("ref") or "OSM 단속카메라",
            "lat": float(lat),
            "lon": float(lon),
            "limitKmh": limit,
            "heading": heading,
            "source": "osm",
        })
    return out


def fetch_kma_api(service_key: str, dataset_url: str = "") -> list[dict]:
    """data.go.kr OpenAPI:
       https://api.data.go.kr/openapi/tn_pubr_public_unmanned_traffic_camera_api
    Returns up to ~42k rows. Pages with numOfRows=1000 (max).
    """
    if not dataset_url:
        dataset_url = "https://api.data.go.kr/openapi/tn_pubr_public_unmanned_traffic_camera_api"
    out: list[dict] = []
    page = 1
    per = 1000
    while True:
        url = (f"{dataset_url}?serviceKey={urllib.parse.quote(service_key, safe='')}"
               f"&pageNo={page}&numOfRows={per}&type=json")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DriveCluster/1.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.load(resp)
        except Exception as e:
            print(f"OpenAPI page {page} failed: {e}", file=sys.stderr)
            break
        body = (payload.get("response") or {}).get("body") or {}
        items = body.get("items") or []
        if isinstance(items, dict):
            items = items.get("item", [])
        if not items:
            break
        for row in items:
            try:
                lat = float(row.get("latitude") or 0)
                lon = float(row.get("longitude") or 0)
            except Exception:
                continue
            if not lat or not lon:
                continue
            try:
                limit = int(float(row.get("lmttVe") or 80))
                if limit <= 0: limit = 80
            except Exception:
                limit = 80
            cid  = str(row.get("mnlssRegltCameraManageNo") or f"api-{page}-{len(out)}").strip()
            inst = (row.get("insttNm") or "").strip()
            spot = (row.get("itlpc") or row.get("rdnmadr") or row.get("lnmadr") or "").strip()
            name = (spot or inst or "단속카메라").strip()
            out.append({
                "id":       f"kma-api-{cid}",
                "name":     name,
                "lat":      lat,
                "lon":      lon,
                "limitKmh": limit,
                "heading":  None,
                "source":   "kma_api",
            })
        # pagination — stop when fewer than `per` rows returned
        if len(items) < per:
            break
        page += 1
        if page > 100:  # safety
            break
    return out


def parse_kma_csv(path: str) -> list[dict]:
    """Parse the public-data CSV `전국과속단속카메라표준데이터`.

    Encoding is usually CP949. Column names vary, so we look up by partial
    match.
    """
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr"]
    raw = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, newline="") as fh:
                raw = fh.read()
            break
        except UnicodeDecodeError:
            continue
    if raw is None:
        raise RuntimeError(f"Could not decode {path}")

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []

    def find(*candidates) -> str | None:
        for c in candidates:
            for f in fieldnames:
                if c in f:
                    return f
        return None

    f_lat   = find("위도", "Lat", "LAT", "latitude")
    f_lon   = find("경도", "Lon", "LON", "longitude")
    f_limit = find("제한속도", "limit", "LIMIT", "MaxSpd")
    f_road  = find("소재지도로명", "도로명")
    f_addr  = find("소재지지번", "지번", "주소")
    f_id    = find("관리번호", "ID", "시설관리번호")

    out: list[dict] = []
    for i, row in enumerate(reader):
        try:
            lat = float(row[f_lat]) if f_lat and row.get(f_lat) else None
            lon = float(row[f_lon]) if f_lon and row.get(f_lon) else None
        except Exception:
            continue
        if not lat or not lon:
            continue
        try:
            limit = int(row.get(f_limit) or 80) if f_limit else 80
        except Exception:
            limit = 80
        name = (row.get(f_road) or row.get(f_addr) or "단속카메라").strip()
        cid  = (row.get(f_id) or f"kma-{i}").strip()
        out.append({
            "id":      f"kma-{cid}",
            "name":    name,
            "lat":     lat,
            "lon":     lon,
            "limitKmh": limit,
            "heading": None,
            "source":  "kma",
        })
    return out


def haversine_m(a: dict, b: dict) -> float:
    R = 6371000.0
    la1 = math.radians(a["lat"]); la2 = math.radians(b["lat"])
    dla = la2 - la1
    dlo = math.radians(b["lon"] - a["lon"])
    h = math.sin(dla/2)**2 + math.cos(la1)*math.cos(la2)*math.sin(dlo/2)**2
    return 2 * R * math.asin(math.sqrt(h))


def dedupe(cameras: list[dict], radius_m: float = 30.0) -> list[dict]:
    """Drop cameras within radius_m of an already-seen one. Coarse grid keyed
    by 0.001° (~110 m) — check the cell + 8 neighbors."""
    grid: dict[tuple[int, int], list[dict]] = {}
    kept: list[dict] = []
    for c in cameras:
        gx = int(c["lat"] * 1000); gy = int(c["lon"] * 1000)
        clash = False
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for other in grid.get((gx+dx, gy+dy), ()):
                    if haversine_m(c, other) < radius_m:
                        clash = True
                        break
                if clash: break
            if clash: break
        if clash:
            continue
        grid.setdefault((gx, gy), []).append(c)
        kept.append(c)
    return kept


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "host", "cameras.json"))
    ap.add_argument("--kma-csv", default="", help="Path to data.go.kr CSV (optional)")
    ap.add_argument("--kma-api-key", default=os.environ.get("KMA_API_KEY", ""),
                    help="Service key for data.go.kr OpenAPI (or env KMA_API_KEY)")
    ap.add_argument("--kma-api-url", default=os.environ.get("KMA_API_URL", ""),
                    help="OpenAPI endpoint URL (or env KMA_API_URL)")
    ap.add_argument("--no-osm", action="store_true")
    args = ap.parse_args()

    cameras: list[dict] = []
    if not args.no_osm:
        try:
            osm = fetch_osm()
            print(f"OSM: {len(osm)} cameras", file=sys.stderr)
            cameras.extend(osm)
        except Exception as e:
            print(f"OSM fetch failed: {e}", file=sys.stderr)

    if args.kma_csv and os.path.exists(args.kma_csv):
        try:
            kma = parse_kma_csv(args.kma_csv)
            print(f"KMA(public-data CSV): {len(kma)} cameras", file=sys.stderr)
            cameras.extend(kma)
        except Exception as e:
            print(f"KMA CSV parse failed: {e}", file=sys.stderr)

    if args.kma_api_key:
        try:
            api = fetch_kma_api(args.kma_api_key, args.kma_api_url)
            print(f"KMA(OpenAPI): {len(api)} cameras", file=sys.stderr)
            cameras.extend(api)
        except Exception as e:
            print(f"KMA OpenAPI fetch failed: {e}", file=sys.stderr)

    before = len(cameras)
    cameras = dedupe(cameras, radius_m=30.0)
    print(f"After de-dupe: {len(cameras)} cameras (dropped {before - len(cameras)})", file=sys.stderr)

    payload = {
        "_comment": "한국 과속 단속카메라. OSM(Overpass) + (있으면) 공공데이터포털 표준데이터셋. 30m 이내 중복 제거. heading=null이면 양방향.",
        "_version": 2,
        "_updatedAt": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_count": len(cameras),
        "cameras": cameras,
    }

    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print(f"Wrote {out_path}  ({len(cameras)} cameras)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
