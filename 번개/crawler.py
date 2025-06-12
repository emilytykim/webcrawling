import requests
from datetime import datetime, timedelta


STATUS_MAP = {"0": "판매중", "1": "판매완료"}


def fetch_items_stream(short_cid):
    url = "https://api.bunjang.co.kr/api/1/find_v2.json"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://m.bunjang.co.kr",
        "Referer": "https://m.bunjang.co.kr/",
        "Accept": "application/json, text/plain, */*",
    }

    # ✅ 기준 시각: 24시간 이전 UTC 기준 timestamp
    cutoff_dt = datetime.now() - timedelta(days=1)
    cutoff_utc_ts = int((cutoff_dt - timedelta(hours=9)).timestamp())

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
                update_time = item.get("update_time", 0)
                if update_time < cutoff_utc_ts:
                    print("🛑 24시간 이전 데이터 도달 → 크롤링 중단")
                    return
                yield item

            page += 1
        except Exception as e:
            print(f"❌ Error fetching page {page}: {e}")
            break


def parse_item(item):
    utc_dt = datetime.utcfromtimestamp(item.get("update_time", 0))
    kst_dt = utc_dt + timedelta(hours=9)
    now = datetime.now()

    # 날짜 (예: 2025.06.12)
    date_str = kst_dt.strftime("%Y.%m.%d")

    # 시간차 계산
    delta = now - kst_dt
    seconds = int(delta.total_seconds())

    if seconds < 60:
        time_diff = "방금 전"
    elif seconds < 3600:
        time_diff = f"{seconds // 60}분 전"
    elif seconds < 86400:
        time_diff = f"{seconds // 3600}시간 전"
    else:
        time_diff = f"{seconds // 3600}시간 전"  # 24시간 이상도 기록 가능

    # 24시간 이내 여부
    within_24h = "✅" if seconds < 86400 else ""

    return {
        "상품명": item.get("name", ""),
        "가격": item.get("price", ""),
        "판매여부": "판매중" if item.get("status") == "0" else "판매완료",
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
