import json
import os

INPUT_FILE = "category_map.json"
OUTPUT_DIR = "splits"
ITEMS_PER_FILE = 5  # 한 파일에 담을 항목 수 (key 개수 기준)


def split_category_map():
    # JSON 로드
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        category_dict = json.load(f)

    keys = list(category_dict.keys())
    total = len(keys)
    num_files = (total + ITEMS_PER_FILE - 1) // ITEMS_PER_FILE

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i in range(num_files):
        chunk_keys = keys[i * ITEMS_PER_FILE : (i + 1) * ITEMS_PER_FILE]
        chunk_dict = {k: category_dict[k] for k in chunk_keys}
        output_path = os.path.join(OUTPUT_DIR, f"category_part_{i+1:02d}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(chunk_dict, f, ensure_ascii=False, indent=2)
        print(f"✅ {output_path} 저장됨 ({len(chunk_keys)}개 카테고리)")

    print(f"\n🎉 총 {num_files}개 파일로 분할 완료")


if __name__ == "__main__":
    split_category_map()
