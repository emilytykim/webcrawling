import json
import glob
from collections import Counter, defaultdict

# splits 폴더 내 모든 JSON 파일 병합
file_paths = glob.glob("splits/*.json")
merged_list = []

for file_path in file_paths:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            merged_list.extend(data)
        else:
            raise ValueError(f"{file_path} is not a list-type JSON file.")

# 병합된 JSON 저장 (선택 사항)
with open("merged.json", "w", encoding="utf-8") as f:
    json.dump(merged_list, f, ensure_ascii=False, indent=2)

# ✅ 기능 1: 고유 group 개수 세기
groups = [item["group"] for item in merged_list if "group" in item]
unique_groups = set(groups)
print(f"📌 Unique 'group' count: {len(unique_groups)}")
print(f"📝 Groups: {sorted(unique_groups)}")

# ✅ 기능 2: 중복된 menu_id 찾기 + 고유 개수 세기
menu_ids = [item["menu_id"] for item in merged_list if "menu_id" in item]
menu_id_counts = Counter(menu_ids)
duplicates = [menu_id for menu_id, count in menu_id_counts.items() if count > 1]

unique_menu_ids = set(menu_ids)
print(f"📦 Total unique 'menu_id' count: {len(unique_menu_ids)}")

if duplicates:
    print(f"⚠️ Duplicate 'menu_id's found: {duplicates}")
else:
    print("✅ No duplicate 'menu_id's found.")

# ✅ 기능 3: group별 menu_id 수 세기
group_counts = defaultdict(int)
for item in merged_list:
    if "group" in item:
        group_counts[item["group"]] += 1

print("\n📊 menu_id count per group:")
for group, count in sorted(group_counts.items()):
    print(f" - {group}: {count}")
