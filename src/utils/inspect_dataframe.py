import pandas as pd

def inspect_dataframe(df_or_path, name="DataFrame"):
    """
    DataFrame 또는 CSV 경로를 받아 구조, 컬럼 목록, 샘플, 통계 요약을 출력한다.
    
    Parameters:
    - df_or_path (pd.DataFrame 또는 str): 분석할 DataFrame 또는 CSV 경로
    - name (str): 출력에 사용할 이름 (기본값: 'DataFrame')
    """

    if isinstance(df_or_path, str):
        df = pd.read_csv(df_or_path)
        print(f"📁 파일명: {df_or_path}")
    elif isinstance(df_or_path, pd.DataFrame):
        df = df_or_path
        print(f"🧾 입력 데이터 이름: {name}")
    else:
        raise ValueError("Input must be a pandas DataFrame or a valid CSV file path.")

    print(f"🔢 shape: {df.shape}")
    print(f"🔑 컬럼 목록:\n{df.columns.tolist()}")
    print(f"\n🧾 예시 5개:")
    print(df.head())
    print(f"\n📈 수치형 통계 요약:")
    print(df.describe())
