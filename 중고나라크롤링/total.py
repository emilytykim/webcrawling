import pandas as pd
import glob
import os
from collections import defaultdict


def combine_all_csv_by_auto_category():
    input_dir = "results"
    output_dir = "result_total"
    os.makedirs(output_dir, exist_ok=True)

    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        print("📂 'results/' 폴더에 CSV 파일이 없습니다.")
        return

    exclude_prefixes = ["2025.05.28", "2025.05.29", "2025.05.30"]
    grouped_files = defaultdict(list)

    # 파일명을 기준으로 상위 카테고리 그룹핑
    for file in csv_files:
        filename = os.path.basename(file)
        if "_" not in filename:
            print(f"⚠️ 파일명에 구분자 '_' 없음: {filename}")
            continue
        big_category = filename.split("_")[0]
        grouped_files[big_category].append(file)

    for big_category, matched_files in grouped_files.items():
        dfs = []
        for file in matched_files:
            try:
                df = pd.read_csv(file, encoding="utf-8-sig")
                filename = os.path.basename(file).replace(".csv", "")
                parts = filename.split("_")
                if len(parts) >= 3:
                    mid_category = parts[1]
                    date_str = parts[2]
                else:
                    print(f"⚠️ 파일명 형식 오류: {filename}")
                    continue

                # 작성일자 또는 date 컬럼 찾기
                date_column = None
                for col in df.columns:
                    if col.strip().lower() in ["작성일자", "date"]:
                        date_column = col
                        break

                # 날짜 기반 필터링
                if date_column:
                    mask = (
                        df[date_column]
                        .astype(str)
                        .str.startswith(tuple(exclude_prefixes))
                    )
                    removed = df[mask]
                    print(f"[{filename}] 제외된 행: {len(removed)}개")
                    df = df[~mask]
                else:
                    print(f"⚠️ 날짜 컬럼 없음: {filename} → 필터링 생략")

                df.insert(0, "group", big_category)
                dfs.append(df)
                print(f"✅ 파일 로드 완료: {file}")
            except Exception as e:
                print(f"❌ 파일 로드 중 오류 발생 ({file}): {e}")

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            csv_output = f"{output_dir}/{big_category}_전체통합.csv"
            excel_output = f"{output_dir}/{big_category}_전체통합.xlsx"
            combined_df.to_csv(csv_output, index=False, encoding="utf-8-sig")
            combined_df.to_excel(excel_output, index=False, engine="openpyxl")

            print(f"\n✅ 저장 완료: {big_category}")
            print(f" - CSV  : {csv_output}")
            print(f" - Excel: {excel_output} ({len(combined_df)}개 행)")
        else:
            print(f"⚠️ {big_category}에 유효한 데이터 없음")


if __name__ == "__main__":
    combine_all_csv_by_auto_category()

# import pandas as pd
# import glob
# import os


# def combine_selected_csv_by_category(categories):
#     output_dir = "result_total"
#     os.makedirs(output_dir, exist_ok=True)

#     csv_files = glob.glob("results/*.csv")
#     if not csv_files:
#         print("📂 'results/' 폴더에 CSV 파일이 없습니다.")
#         return

#     exclude_prefixes = ["2025.05.28", "2025.05.29", "2025.05.30"]

#     for big_category in categories:
#         matched_files = [
#             f for f in csv_files if os.path.basename(f).startswith(big_category + "_")
#         ]
#         if not matched_files:
#             print(f"⚠️ {big_category}에 해당하는 파일 없음")
#             continue

#         dfs = []
#         for file in matched_files:
#             try:
#                 df = pd.read_csv(file, encoding="utf-8-sig")
#                 filename = os.path.basename(file).replace(".csv", "")
#                 parts = filename.split("_")
#                 if len(parts) >= 3:
#                     mid_category = parts[1]
#                     date_str = parts[2]
#                 else:
#                     print(f"⚠️ 파일명 형식 오류: {filename}")
#                     continue

#                 date_column = None
#                 for col in df.columns:
#                     if col.strip().lower() in ["작성일자", "date"]:
#                         date_column = col
#                         break

#                 if date_column:
#                     mask = (
#                         df[date_column]
#                         .astype(str)
#                         .str.startswith(tuple(exclude_prefixes))
#                     )
#                     removed = df[mask]
#                     print(f"[{filename}] 제외된 행: {len(removed)}개")
#                     df = df[~mask]
#                 else:
#                     print(f"⚠️ 날짜 컬럼 없음: {filename} → 필터링 생략")

#                 df.insert(0, "group", big_category)

#                 dfs.append(df)
#                 print(f"✅ 파일 로드 완료: {file}")
#             except Exception as e:
#                 print(f"❌ 파일 로드 중 오류 발생 ({file}): {e}")

#         if dfs:
#             combined_df = pd.concat(dfs, ignore_index=True)

#             # 저장
#             csv_output = f"{output_dir}/{big_category}_전체통합.csv"
#             excel_output = f"{output_dir}/{big_category}_전체통합.xlsx"
#             combined_df.to_csv(csv_output, index=False, encoding="utf-8-sig")
#             combined_df.to_excel(excel_output, index=False, engine="openpyxl")

#             print(f"\n✅ 저장 완료: {big_category}")
#             print(f" - CSV  : {csv_output}")
#             print(f" - Excel: {excel_output} ({len(combined_df)}개 행)")
#         else:
#             print(f"⚠️ {big_category}에 유효한 데이터 없음")


# if __name__ == "__main__":
#     raw_input = input("상위 카테고리를 입력하세요 (예: 남성패션,여성패션,가공식품): ")
#     categories = [c.strip() for c in raw_input.split(",") if c.strip()]
#     if not categories:
#         print("❌ 유효한 카테고리를 입력하세요.")
#     else:
#         combine_selected_csv_by_category(categories)
