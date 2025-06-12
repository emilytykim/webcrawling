import json, os, csv, time, sys
from datetime import datetime
from crawler import fetch_items_stream, parse_item

RESULTS_DIR = "results"
PROGRESS_FILE = "crawling_progress.json"
os.makedirs(RESULTS_DIR, exist_ok=True)

# 저장할 날짜 목록 및 조기 종료 기준
TARGET_DATES = ["2025-06-11", "2025-06-12"]
MIN_DATE = min(TARGET_DATES)  # "2025-06-11"

# 명령줄 인자 확인
if len(sys.argv) != 2:
    print("❗ 사용법: python main.py splits/category_01.json")
    sys.exit(1)

input_json = sys.argv[1]
with open(input_json, "r", encoding="utf-8") as f:
    CATEGORY_MAP = json.load(f)

# 진행 상황 불러오기
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        progress = json.load(f)
else:
    progress = {}


def save_progress(fid: str, pid: str):
    progress.setdefault(fid, [])
    if pid not in progress[fid]:
        progress[fid].append(pid)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def main():
    start_time = time.time()
    tasks = []

    # category_map.json 에서 up/low/sub 조합 생성
    for up_id, up_info in CATEGORY_MAP.items():
        for low_id, low_info in up_info["subgroups"].items():
            short_cid = up_id + low_id
            subsubs = low_info.get("subsubgroups")
            if subsubs:
                for sub_id, sub_name in subsubs.items():
                    full_id = up_id + low_id + sub_id
                    tasks.append(
                        {
                            "short_cid": short_cid,
                            "full_id": full_id,
                            "up_name": up_info["group"],
                            "low_name": low_info["name"],
                            "sub_name": sub_name,
                        }
                    )
            else:
                full_id = up_id + low_id
                tasks.append(
                    {
                        "short_cid": short_cid,
                        "full_id": full_id,
                        "up_name": up_info["group"],
                        "low_name": low_info["name"],
                        "sub_name": "",
                    }
                )

    for t in tasks:
        sid, fid = t["short_cid"], t["full_id"]
        filename = f"{t['up_name']}_{t['low_name']}_{t['sub_name']}.csv".replace(
            "/", "_"
        )
        file_path = os.path.join(RESULTS_DIR, filename)
        print(f"📂 {t['up_name']} > {t['low_name']} > {t['sub_name']} ({fid})")

        seen_pids = set(progress.get(fid, []))
        is_new = not os.path.exists(file_path)

        with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = None

            for item in fetch_items_stream(sid):
                # ➊ 카테고리·중복 체크
                if item.get("category_id") != fid or item.get("pid") in seen_pids:
                    continue

                row = parse_item(item)
                item_date = row["날짜"].replace(".", "-")  # "2025.06.12" → "2025-06-12"

                # ➋ 조기 종료: 기준일 이전이면 멈춤
                if item_date < MIN_DATE:
                    print(f"🛑 {item_date} < {MIN_DATE} → 크롤링 중단")
                    break

                # ➌ 날짜 필터링: 원하는 날짜만 저장
                if item_date not in TARGET_DATES:
                    continue

                # ➍ 카테고리 컬럼 추가
                row["대분류"] = t["up_name"]
                row["중분류"] = t["low_name"]
                row["소분류"] = t["sub_name"]

                # ➎ CSV 헤더 & 쓰기
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=row.keys())
                    if is_new:
                        writer.writeheader()
                writer.writerow(row)
                save_progress(fid, item.get("pid"))

        time.sleep(1)

    elapsed = round(time.time() - start_time, 2)
    print(f"⏱️ 전체 크롤링 완료! 소요 시간: {elapsed}초")


if __name__ == "__main__":
    main()
