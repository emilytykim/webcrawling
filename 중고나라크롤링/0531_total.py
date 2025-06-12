import pandas as pd
import glob
import os


def combine_all_category_totals():
    input_dir = "0602-0531/csv"
    output_dir = input_dir
    os.makedirs(output_dir, exist_ok=True)

    exclude_prefixes = [
        "2025.05.16",
        "2025.05.17",
        "2025.05.18",
        "2025.05.19",
        "2025.05.20",
        "2025.05.21",
        "2025.05.22",
        "2025.05.23",
        "2025.05.24",
        "2025.05.25",
        "2025.05.28",
        "2025.05.29",
        "2025.05.30",
    ]

    csv_files = glob.glob(os.path.join(input_dir, "*_전체통합.csv"))
    if not csv_files:
        print("📂 '_전체통합.csv' 파일이 없습니다.")
        return

    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, encoding="utf-8-sig")
            filename = os.path.basename(file)

            # 날짜 컬럼 찾기
            date_column = None
            for col in df.columns:
                if col.strip().lower() in ["작성일자", "date"]:
                    date_column = col
                    break

            if date_column:
                mask = (
                    df[date_column].astype(str).str.startswith(tuple(exclude_prefixes))
                )
                removed = df[mask]
                df = df[~mask]
                print(f"[{filename}] 제외된 행: {len(removed)}개")
            else:
                print(f"⚠️ 날짜 컬럼 없음: {filename} → 필터링 생략")

            dfs.append(df)
            print(f"✅ 파일 로드 완료: {file}")
        except Exception as e:
            print(f"❌ {file} 로드 실패: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        csv_output = os.path.join(output_dir, "전체전체통합.csv")
        excel_output = os.path.join(output_dir, "전체전체통합.xlsx")

        combined_df.to_csv(csv_output, index=False, encoding="utf-8-sig")
        combined_df.to_excel(excel_output, index=False, engine="openpyxl")

        print(f"\n✅ 전체 저장 완료 ({len(combined_df)}개 행)")
        print(f" - CSV  : {csv_output}")
        print(f" - Excel: {excel_output}")
    else:
        print("⚠️ 유효한 데이터가 없습니다.")


if __name__ == "__main__":
    combine_all_category_totals()
