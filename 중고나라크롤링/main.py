import requests
import csv
import os
import json
import datetime
import sys

# 기본 설정
CAFE_ID = 10050146
TARGET_DATES = ["2025-06-10"]


HEADERS = {
    "Accept": "*/*",
    "Origin": "https://cafe.naver.com",
    "Referer": "https://cafe.naver.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "x-cafe-product": "pc",
    "cookie": "NID_AUT=...; NID_SES=...",  # 여기에 로그인 쿠키 입력
}


def sanitize_filename(name):
    return name.replace("/", "_").replace("\\", "_")


def fetch_articles(menu_id, page):
    url = f"https://apis.naver.com/cafe-web/cafe-boardlist-api/v1/cafes/{CAFE_ID}/menus/{menu_id}/articles"
    params = {
        "page": page,
        "pageSize": 15,
        "sortBy": "TIME",
        "viewType": "L",
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()


def save_to_csv(articles, board_name):
    os.makedirs("results", exist_ok=True)

    date_to_articles = {}
    for art in articles:
        date = art["writeDate"]
        date_to_articles.setdefault(date, []).append(art)

    for date, arts in date_to_articles.items():
        safe_board_name = sanitize_filename(board_name)
        filename = f"{safe_board_name}_{date}.csv"
        filepath = os.path.join("results", filename)
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "카테고리",
                    "제목",
                    "가격",
                    "판매상태",
                    "판매자등급",
                    "escrow",
                    "판매중",
                    "작성자",
                    "작성일자",
                    "URL",
                    "article_id",
                ]
            )
            for art in arts:
                writer.writerow(
                    [
                        board_name,
                        art.get("subject", "").strip(),
                        art.get("price", ""),
                        art.get("saleStatus", ""),
                        art.get("memberLevelName", ""),
                        art.get("escrow", ""),
                        art.get("onSale", ""),
                        art.get("nickName", ""),
                        art.get("writeDate", ""),
                        f"https://cafe.naver.com{art.get('url')}",
                        art.get("articleId"),
                    ]
                )


def crawl_board(menu_id, board_name):
    print(f"📂 {board_name} (menu_id: {menu_id}) 시작")
    all_articles = []
    page = 1

    while True:
        print(f"🔄 Page {page} fetching...")
        try:
            data = fetch_articles(menu_id, page)
        except Exception as e:
            print(f"❌ 페이지 {page} 요청 실패: {e}")
            break

        articles = data.get("result", {}).get("articleList", [])
        if not articles:
            print("ℹ️ 더 이상 글 없음 (빈 리스트)")
            break

        for art_wrapper in articles:
            item = art_wrapper.get("item", {})
            ts = item.get("writeDateTimestamp")
            if not ts:
                continue

            dt = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            if dt in TARGET_DATES:
                all_articles.append(
                    {
                        "subject": item.get("subject", ""),
                        "price": item.get("formattedCost", ""),
                        "saleStatus": item.get("productSale", {}).get("saleStatus", ""),
                        "memberLevelName": item.get("writerInfo", {}).get(
                            "memberLevelName", ""
                        ),
                        "escrow": item.get("escrow", ""),
                        "onSale": item.get("onSale", ""),
                        "nickName": item.get("writerInfo", {}).get("nickName", ""),
                        "writeDate": dt,
                        "url": f"/ArticleRead.nhn?clubid={CAFE_ID}&articleid={item.get('articleId')}",
                        "articleId": item.get("articleId"),
                    }
                )
            elif dt < min(TARGET_DATES):
                print(f"🛑 {dt} < {min(TARGET_DATES)} → 조기 종료")
                save_to_csv(all_articles, board_name)
                print(f"✅ {board_name} 완료 ({len(all_articles)}건)")
                return

        page += 1

    save_to_csv(all_articles, board_name)
    print(f"✅ {board_name} 완료 ({len(all_articles)}건)")


def main():
    if len(sys.argv) < 2:
        print("❌ 실행 방법: python main.py [categories_json_filename]")
        print("예시: python main.py splits/categories_part_01.json")
        return

    category_file = sys.argv[1]

    if not os.path.exists(category_file):
        print(f"❌ 파일이 존재하지 않습니다: {category_file}")
        return

    print(f"📁 카테고리 파일 불러오는 중: {category_file}")
    with open(category_file, "r", encoding="utf-8") as f:
        categories = json.load(f)

    for cat in categories:
        menu_id = cat.get("menu_id")
        board_name = cat.get("board_name")
        if not menu_id or not board_name:
            continue
        try:
            crawl_board(menu_id, board_name)
        except Exception as e:
            print(f"❌ {board_name}에서 오류 발생: {e}")


if __name__ == "__main__":
    main()
