import json
import os

INPUT_FILE = "categories.json"
OUTPUT_DIR = "splits"
ITEMS_PER_FILE = 20  # 한 파일에 몇 개씩 담을지


def split_categories():
    # 원본 카테고리 로딩
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        categories = json.load(f)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total = len(categories)
    num_files = (total + ITEMS_PER_FILE - 1) // ITEMS_PER_FILE

    for i in range(num_files):
        chunk = categories[i * ITEMS_PER_FILE : (i + 1) * ITEMS_PER_FILE]
        filename = os.path.join(OUTPUT_DIR, f"categories_part_{i+1:02d}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)
        print(f"✅ {filename} 저장됨 ({len(chunk)}개)")

    print(f"\n🎉 총 {num_files}개 파일로 분할 완료")


if __name__ == "__main__":
    split_categories()
