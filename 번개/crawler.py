import requests
from datetime import datetime, timedelta

STATUS_MAP = {"0": "판매중", "1": "판매완료"}


def fetch_items_stream(short_cid: str):
    """
    6자리 f_category_id로 API를 반복 호출해 모든 아이템을 yield
    """
    url = "https://api.bunjang.co.kr/api/1/find_v2.json"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://m.bunjang.co.kr",
        "Referer": "https://m.bunjang.co.kr/",
        "Accept": "application/json, text/plain, */*",
    }

    page = 0
    while True:
        print(f"📄 Page {page} 로딩중...")
        params = {
            "f_category_id": short_cid,
            "order": "date",
            "page": page,
            "n": 100,
            "req_ref": "popular_category",
            "stat_device": "w",
            "version": "4",
        }
        try:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            items = resp.json().get("list", [])
            if not items:
                break
            for item in items:
                yield item
            page += 1
        except Exception as e:
            print(f"❌ Error fetching page {page}: {e}")
            break


def parse_item(item: dict) -> dict:
    """
    API로 받은 item → CSV 한 행용 dict 변환 (KST 기준 날짜·시간, 24h 표시 포함)
    """
    # UTC → KST 변환
    utc_dt = datetime.utcfromtimestamp(item.get("update_time", 0))
    kst_dt = utc_dt + timedelta(hours=9)
    now = datetime.now()

    # 날짜 (예: "2025.06.12")
    date_str = kst_dt.strftime("%Y.%m.%d")

    # 경과 시간 계산
    delta = now - kst_dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        time_diff = "방금 전"
    elif seconds < 3600:
        time_diff = f"{seconds // 60}분 전"
    elif seconds < 86400:
        time_diff = f"{seconds // 3600}시간 전"
    else:
        time_diff = f"{seconds // 3600}시간 전"

    # 24시간 이내 표시
    within_24h = "✅" if seconds < 86400 else ""

    return {
        "상품명": item.get("name", ""),
        "가격": item.get("price", ""),
        "판매여부": STATUS_MAP.get(str(item.get("status")), "Unknown"),
        "날짜": date_str,
        "시간": time_diff,
        "24시간이내": within_24h,
        "프로상점": item.get("proshop", False),
        "광고": item.get("ad", False),
        "케어": item.get("care", False),
        "무료배송": item.get("free_shipping", False),
        "지역": item.get("location", ""),
        "URL": f"https://m.bunjang.co.kr/products/{item.get('pid')}",
        "상품ID": item.get("pid"),
        "세부카테고리ID": item.get("category_id", ""),
    }
