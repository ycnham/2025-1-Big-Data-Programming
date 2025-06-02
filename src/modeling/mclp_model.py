import pandas as pd
import numpy as np
from pulp import LpProblem, LpMaximize, LpVariable, lpSum
from tqdm import tqdm

def solve_mclp(
    df: pd.DataFrame,
    coverage_radius: float = 0.02,
    facility_limit: int = 30,
    demand_column: str = 'predicted_demand_score',
    verbose: bool = True
) -> pd.DataFrame:
    """
    MCLP (Maximum Coverage Location Problem) 최적 입지 선정 함수

    Parameters:
    - df: 격자별 데이터 (center_lat, center_lon, 수요 열 포함)
    - coverage_radius: 커버 범위 (위경도 거리 기준)
    - facility_limit: 설치 가능한 충전소 최대 개수
    - demand_column: 수요 컬럼명 (예: predicted_demand_score)
    - verbose: 출력 여부

    Returns:
    - 설치 여부(selected 열)가 포함된 DataFrame
    """

    sites = list(df.index)
    demand_points = list(df.index)

    # 수요 컬럼 정의
    df['demand'] = df[demand_column]

    # 커버리지 매트릭스 생성
    coverage_matrix = {
        i: [
            j for j in demand_points
            if np.sqrt(
                (df.loc[i, 'center_lat'] - df.loc[j, 'center_lat'])**2 +
                (df.loc[i, 'center_lon'] - df.loc[j, 'center_lon'])**2
            ) <= coverage_radius
        ]
        for i in tqdm(sites, desc="📍 커버리지 행렬 생성 중")
    }

    # 최적화 문제 정의
    prob = LpProblem("Maximize_Coverage", LpMaximize)

    x = LpVariable.dicts("Site", sites, cat='Binary')     # 설치 여부
    y = LpVariable.dicts("Covered", demand_points, cat='Binary')  # 수요지 커버 여부

    # 목적 함수: 커버된 수요 총합 최대화
    prob += lpSum(df.loc[j, 'demand'] * y[j] for j in demand_points)

    # 제약조건 1: 설치 개수 제한
    prob += lpSum(x[i] for i in sites) <= facility_limit

    # 제약조건 2: 수요지 j는 설치된 i들에 의해 커버되어야 함
    for j in tqdm(demand_points, desc="🧩 제약조건 생성 중"):
        prob += y[j] <= lpSum(x[i] for i in coverage_matrix if j in coverage_matrix[i])

    # 문제 풀기
    prob.solve()

    # 선택된 격자 표시
    selected_sites = [i for i in sites if x[i].varValue == 1.0]
    df['selected'] = df.index.isin(selected_sites).astype(int)

    if verbose:
        print(f"선택된 설치 위치 수: {len(selected_sites)}")
        total_demand = df[df['selected'] == 1]['demand'].sum()
        print(f"총 커버된 수요: {round(total_demand, 2)}")

    return df

# ============================================
# 📊 민감도 분석 함수
# ============================================

def run_sensitivity_analysis(
    df: pd.DataFrame,
    coverage_radii: list = [0.01, 0.02, 0.03],
    facility_limits: list = [10, 20, 30],
    demand_column: str = 'predicted_demand_score'
) -> pd.DataFrame:
    """
    MCLP 민감도 분석: 반경(r)과 설치 수(p)의 변화에 따른 커버 수요 분석

    Parameters:
    - df: 입력 데이터프레임 (위경도, 수요 포함)
    - coverage_radii: 커버 반경 리스트 (float, 단위: degree)
    - facility_limits: 설치 가능 수 리스트 (int)
    - demand_column: 예측 수요 컬럼명

    Returns:
    - pd.DataFrame: 시나리오별 커버 수요 및 커버율 결과 테이블
    """
    results = []

    for r in coverage_radii:
        for p in facility_limits:
            result_df = solve_mclp(
                df.copy(),
                coverage_radius=r,
                facility_limit=p,
                demand_column=demand_column,
                verbose=False
            )
            covered = result_df[result_df['selected'] == 1][demand_column].sum()
            total = result_df[demand_column].sum()
            rate = covered / total * 100

            results.append({
                'coverage_radius': r,
                'facility_limit': p,
                'covered_demand': round(covered, 2),
                'total_demand': round(total, 2),
                'coverage_rate': round(rate, 2)
            })

    return pd.DataFrame(results)