import time
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import xml.etree.ElementTree as ET

import requests
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="NFQS 친환경수산물 인증 조회 API", version="1.0.0")

API_URL = "https://www.nfqs.go.kr/hpmg/front/api/organic_api.do"

# ✅ 추천: Vercel 환경변수로 넣는 게 안전하지만,
# 사용자가 원하면 하드코딩도 가능.
CERT_KEY = "389CE834F4BEABF2200E4E8C77EA9A76E1FD9C4619227A4882BE128DA0A6A1F8"

HTTP_TIMEOUT = 20
CACHE_TTL_SECONDS = 60
_cache = {"ts": 0.0, "items": []}


def yyyymmdd_to_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def validity_status(vfrom: str, vto: str, today: Optional[date] = None) -> str:
    """
    VALID   : 오늘 유효
    EXPIRED : 만료
    FUTURE  : 시작 전
    UNKNOWN : 유효기간 미기재/파싱불가 (공란 포함)
    """
    if today is None:
        today = date.today()

    d_from = yyyymmdd_to_date(vfrom)
    d_to = yyyymmdd_to_date(vto)

    if not d_from or not d_to:
        return "UNKNOWN"
    if today < d_from:
        return "FUTURE"
    if today > d_to:
        return "EXPIRED"
    return "VALID"


def fetch_xml() -> str:
    r = requests.get(API_URL, params={"cert_key": CERT_KEY}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


def parse_items(xml_text: str) -> Dict[str, Any]:
    root = ET.fromstring(xml_text)
    result_code = (root.findtext("./header/resultCode") or "").strip()
    result_msg = (root.findtext("./header/resultMsg") or "").strip()

    items = []
    for it in root.findall(".//body/items/item"):
        items.append({
            "jisoknm": it.findtext("jisoknm") or "",
            "codeknm": it.findtext("codeknm") or "",
            "goodknm": it.findtext("goodknm") or "",
            "certno": it.findtext("certno") or "",
            "custkfirm": it.findtext("custkfirm") or "",
            "headknm": it.findtext("headknm") or "",
            "resino": it.findtext("resino") or "",
            "tel": it.findtext("tel") or "",
            "jisokaddr": it.findtext("jisokaddr") or "",
            "vdatefrom": it.findtext("vdatefrom") or "",
            "vdateto": it.findtext("vdateto") or "",
        })

    return {"resultCode": result_code, "resultMsg": result_msg, "items": items}


def get_items_cached(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    if (not force) and _cache["items"] and (now - _cache["ts"] < CACHE_TTL_SECONDS):
        return {"resultCode": "00", "resultMsg": "CACHED", "items": _cache["items"]}

    parsed = parse_items(fetch_xml())
    if parsed.get("resultCode") != "00":
        return parsed

    _cache["ts"] = now
    _cache["items"] = parsed["items"]
    return parsed


def compute_counts(items: List[Dict[str, Any]]) -> Dict[str, int]:
    today = date.today()
    counts = {"VALID": 0, "UNKNOWN": 0, "EXPIRED": 0, "FUTURE": 0}
    for it in items:
        st = validity_status(it.get("vdatefrom", ""), it.get("vdateto", ""), today=today)
        counts[st] += 1
    return {
        "rows_total": len(items),
        "rows_valid": counts["VALID"],
        "rows_unknown": counts["UNKNOWN"],
        "rows_expired": counts["EXPIRED"],
        "rows_future": counts["FUTURE"],
    }


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/search")
def search(
    keyword: str = Query(default="", description="부분검색(업체명/품목/주소/인증번호 등)"),
    jisoknm: str = Query(default="", description="인증기관(예: (유)오가닉티앤씨)"),
    force: int = Query(default=0, description="1이면 캐시 무시"),
):
    raw = get_items_cached(force=bool(force))
    if raw.get("resultCode") != "00":
        return JSONResponse({
            "resultCode": raw.get("resultCode", ""),
            "resultMsg": raw.get("resultMsg", ""),
            "today": date.today().isoformat(),
            "counts": {"rows_total": 0, "rows_valid": 0, "rows_unknown": 0, "rows_expired": 0, "rows_future": 0},
            "items": [],
        }, status_code=200)

    items = raw["items"]
    # 기관 필터
    if jisoknm.strip():
        items = [it for it in items if jisoknm.strip().lower() in (it.get("jisoknm") or "").lower()]

    # keyword 필터
    if keyword.strip():
        k = keyword.strip().lower()
        def hay(it):
            return " ".join([
                it.get("jisoknm",""), it.get("codeknm",""), it.get("goodknm",""),
                it.get("certno",""), it.get("custkfirm",""), it.get("headknm",""),
                it.get("jisokaddr","")
            ]).lower()
        items = [it for it in items if k in hay(it)]

    # 상태 붙이기
    today = date.today()
    out = []
    for it in items:
        it2 = dict(it)
        it2["_validity"] = validity_status(it.get("vdatefrom",""), it.get("vdateto",""), today=today)
        out.append(it2)

    return {
        "resultCode": "00",
        "resultMsg": raw.get("resultMsg", "OK"),
        "today": today.isoformat(),
        "counts": compute_counts(out),
        "items": out,
    }


@app.get("/api/expiry")
def expiry(
    certno: str = Query(..., description="인증번호 예: 104-0153"),
    jisoknm: str = Query(default="", description="인증기관(선택) 예: (유)오가닉티앤씨"),
    force: int = Query(default=0, description="1이면 캐시 무시"),
):
    """
    인증번호 1개에 대한 '현재 유효/만료/미기재'와 만료일을 돌려줌.
    같은 certno가 여러 줄이면:
      - vdatefrom 기준 최신(파싱 가능한 것) 우선
      - 전부 공란이면 UNKNOWN 중 1건 반환
    """
    raw = get_items_cached(force=bool(force))
    if raw.get("resultCode") != "00":
        return JSONResponse({
            "resultCode": raw.get("resultCode", ""),
            "resultMsg": raw.get("resultMsg", ""),
            "today": date.today().isoformat(),
            "found": False,
            "item": None,
        }, status_code=200)

    items = raw["items"]
    if jisoknm.strip():
        items = [it for it in items if jisoknm.strip().lower() in (it.get("jisoknm") or "").lower()]

    candidates = [it for it in items if (it.get("certno") or "").strip() == certno.strip()]
    if not candidates:
        return {
            "resultCode": "00",
            "resultMsg": "OK",
            "today": date.today().isoformat(),
            "found": False,
            "item": None,
        }

    # 최신 vdatefrom 우선(파싱 가능한 것)
    def key_fn(it):
        d = yyyymmdd_to_date(it.get("vdatefrom",""))
        return d or date(1900,1,1)

    candidates.sort(key=key_fn, reverse=True)
    picked = candidates[0]

    today = date.today()
    status = validity_status(picked.get("vdatefrom",""), picked.get("vdateto",""), today=today)

    item = dict(picked)
    item["_validity"] = status

    return {
        "resultCode": "00",
        "resultMsg": "OK",
        "today": today.isoformat(),
        "found": True,
        "item": item,
    }
