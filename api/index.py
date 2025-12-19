import time
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import xml.etree.ElementTree as ET

import requests
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="NFQS 친환경수산물 인증 조회 API", version="1.0.0")

# =========================
# 설정
# =========================
API_URL = "https://www.nfqs.go.kr/hpmg/front/api/organic_api.do"

# 사용자가 준 cert_key (원하면 Vercel 환경변수로 바꾸는 것도 가능)
CERT_KEY = "389CE834F4BEABF2200E4E8C77EA9A76E1FD9C4619227A4882BE128DA0A6A1F8"

HTTP_TIMEOUT = 20

# 너무 자주 원본 호출하지 않도록 간단 캐시(서버리스에서도 짧은 기간엔 효과 있음)
CACHE_TTL_SECONDS = 60
_cache = {"ts": 0.0, "items": []}


# =========================
# 유틸
# =========================
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


def format_date_yyyy_mm_dd(s: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD, 변환 실패/공란이면 원문 그대로(또는 빈값)"""
    d = yyyymmdd_to_date(s)
    return d.isoformat() if d else (s or "")


def safe_lower(s: str) -> str:
    return (s or "").strip().lower()


def build_haystack(it: Dict[str, Any]) -> str:
    return " ".join([
        it.get("jisoknm", ""),
        it.get("codeknm", ""),
        it.get("goodknm", ""),
        it.get("certno", ""),
        it.get("custkfirm", ""),
        it.get("headknm", ""),
        it.get("jisokaddr", ""),
        it.get("tel", ""),
    ]).lower()


# =========================
# 원본 API 호출 + XML 파싱
# =========================
def fetch_xml() -> str:
    r = requests.get(API_URL, params={"cert_key": CERT_KEY}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


def parse_items(xml_text: str) -> Dict[str, Any]:
    root = ET.fromstring(xml_text)
    result_code = (root.findtext("./header/resultCode") or "").strip()
    result_msg = (root.findtext("./header/resultMsg") or "").strip()

    items: List[Dict[str, Any]] = []
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


# =========================
# 엔드포인트
# =========================
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

    items: List[Dict[str, Any]] = raw["items"]

    # 기관 필터 (jisoknm)
    if jisoknm.strip():
        k = safe_lower(jisoknm)
        items = [it for it in items if k in safe_lower(it.get("jisoknm", ""))]

    # 키워드 필터 (여러 필드 합쳐서 부분검색)
    if keyword.strip():
        k = safe_lower(keyword)
        items = [it for it in items if k in build_haystack(it)]

    today = date.today()
    out: List[Dict[str, Any]] = []
    for it in items:
        it2 = dict(it)
        st = validity_status(it.get("vdatefrom", ""), it.get("vdateto", ""), today=today)
        it2["_validity"] = st
        # 사람이 읽기 쉽게 날짜 포맷도 같이 제공
        it2["vdatefrom_iso"] = format_date_yyyy_mm_dd(it.get("vdatefrom", ""))
        it2["vdateto_iso"] = format_date_yyyy_mm_dd(it.get("vdateto", ""))
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
    인증번호 1개에 대한 만료일/유효상태 조회.
    같은 certno가 여러 줄이면:
      - vdatefrom(파싱가능) 최신 건 우선
      - 전부 공란이면 UNKNOWN 건 중 1개 반환
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

    items: List[Dict[str, Any]] = raw["items"]

    # 기관 필터(선택)
    if jisoknm.strip():
        k = safe_lower(jisoknm)
        items = [it for it in items if k in safe_lower(it.get("jisoknm", ""))]

    cno = certno.strip()
    candidates = [it for it in items if (it.get("certno") or "").strip() == cno]

    if not candidates:
        return {
            "resultCode": "00",
            "resultMsg": "OK",
            "today": date.today().isoformat(),
            "found": False,
            "item": None,
        }

    # vdatefrom 최신 우선(파싱 가능 날짜 기준), 파싱 불가면 1900-01-01
    def key_fn(it: Dict[str, Any]) -> date:
        d = yyyymmdd_to_date(it.get("vdatefrom", ""))
        return d or date(1900, 1, 1)

    candidates.sort(key=key_fn, reverse=True)
    picked = candidates[0]

    today = date.today()
    st = validity_status(picked.get("vdatefrom", ""), picked.get("vdateto", ""), today=today)

    item = dict(picked)
    item["_validity"] = st
    item["vdatefrom_iso"] = format_date_yyyy_mm_dd(picked.get("vdatefrom", ""))
    item["vdateto_iso"] = format_date_yyyy_mm_dd(picked.get("vdateto", ""))

    # 만료일(사람용)만 따로 주면 GPT가 답변하기 편함
    expiry_date = item["vdateto_iso"] if item["vdateto_iso"] else ""

    return {
        "resultCode": "00",
        "resultMsg": "OK",
        "today": today.isoformat(),
        "found": True,
        "expiry_date": expiry_date,     # 예: "2026-09-04" 또는 ""(미기재)
        "validity": st,                 # VALID/EXPIRED/FUTURE/UNKNOWN
        "item": item,
    }
