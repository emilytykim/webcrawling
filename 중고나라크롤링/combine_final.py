import pandas as pd
import glob
import os


def combine_final_csv():
    input_dir = "result_total"
    output_dir = "result_final"
    os.makedirs(output_dir, exist_ok=True)

    csv_files = glob.glob(os.path.join(input_dir, "*_전체통합.csv"))
    if not csv_files:
        print("📂 'result_total/' 폴더에 통합 CSV 파일이 없습니다.")
        return

    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, encoding="utf-8-sig")
            dfs.append(df)
            print(f"✅ 파일 로드 완료: {file} ({len(df)}행)")
        except Exception as e:
            print(f"❌ 파일 로드 중 오류 발생 ({file}): {e}")

    if not dfs:
        print("⚠️ 병합할 유효한 데이터 없음")
        return

    combined_df = pd.concat(dfs, ignore_index=True)

    # CSV 저장
    csv_output = os.path.join(output_dir, "최종_통합.csv")
    combined_df.to_csv(csv_output, index=False, encoding="utf-8-sig")

    # Excel 저장
    excel_output = os.path.join(output_dir, "최종_통합.xlsx")
    combined_df.to_excel(excel_output, index=False, engine="openpyxl")

    print(f"\n✅ 최종 병합 완료:")
    print(f" - CSV  : {csv_output}")
    print(f" - Excel: {excel_output} ({len(combined_df)}개 행)")


if __name__ == "__main__":
    combine_final_csv()
