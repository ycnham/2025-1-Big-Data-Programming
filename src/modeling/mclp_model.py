import pandas as pd
import numpy as np
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, LpStatus
from geopy.distance import geodesic
from tqdm import tqdm

def solve_mclp(
    df: pd.DataFrame,
    coverage_radius: float = 0.02,  # 대략 위경도 degree, 0.02 ≒ 2.2km
    facility_limit: int = 30,
    demand_column: str = 'predicted_demand_score',
    verbose: bool = True
) -> pd.DataFrame:
    """
    MCLP (Maximum Coverage Location Problem) 최적 입지 선정 함수

    Parameters:
    - df: 격자별 데이터 (center_lat, center_lon, 수요 열 포함)
    - coverage_radius: 커버 범위 (위경도 degree, 약 1도 ≈ 111km)
    - facility_limit: 설치 가능한 충전소 최대 개수
    - demand_column: 수요 컬럼명
    - verbose: 출력 여부

    Returns:
    - 설치 여부(selected 열)가 포함된 DataFrame
    """
    df = df.copy()
    df['demand'] = df[demand_column]

    coords = df[['center_lat', 'center_lon']].values
    sites = list(df.index)
    demand_points = list(df.index)

    # 실제 거리 기반 커버리지 행렬 생성
    coverage_matrix = {}
    for i, (lat_i, lon_i) in tqdm(enumerate(coords), total=len(coords), desc="📍 커버리지 행렬 생성"):
        covered = []
        for j, (lat_j, lon_j) in enumerate(coords):
            distance_km = geodesic((lat_i, lon_i), (lat_j, lon_j)).km
            if distance_km <= coverage_radius * 111.0:  # 위경도 degree -> km 환산
                covered.append(j)
        if covered:
            coverage_matrix[i] = covered

    # 최적화 문제 정의
    prob = LpProblem("Maximize_Coverage", LpMaximize)
    x = LpVariable.dicts("Site", sites, cat='Binary')      # 설치 여부
    y = LpVariable.dicts("Covered", demand_points, cat='Binary')  # 수요 커버 여부

    # 목적함수: 커버된 수요 총합 최대화
    prob += lpSum(df.loc[j, 'demand'] * y[j] for j in demand_points)

    # 제약조건 1: 설치 수 제한
    prob += lpSum(x[i] for i in sites) <= facility_limit

    # 제약조건 2: 수요지 j는 인근 설치지 i들에 의해 커버되어야 함
    for j in tqdm(demand_points, desc="🧩 제약조건 생성 중"):
        prob += y[j] <= lpSum(x[i] for i in coverage_matrix if j in coverage_matrix[i])

    # 최적화 수행
    prob.solve()

    if LpStatus[prob.status] != 'Optimal':
        print(f"⚠️ 최적해를 찾지 못했습니다. 상태: {LpStatus[prob.status]}")

    # 선택된 격자
    selected_sites = [i for i in sites if x[i].varValue == 1.0]
    df['selected'] = df.index.isin(selected_sites).astype(int)

    # 요약 출력
    if verbose:
        total = df['demand'].sum()
        covered = df[df['selected'] == 1]['demand'].sum()
        print(f"✅ 선택된 격자 수: {len(selected_sites)}")
        print(f"📦 커버된 수요: {covered:,.2f} / 총 수요: {total:,.2f} ({covered/total*100:.2f}%)")

    return df

# ============================================
# 📊 민감도 분석 함수
# ============================================

def run_sensitivity_analysis(
    df: pd.DataFrame,
    coverage_radii_km: list = [1.0, 2.0, 3.0],
    facility_limits: list = [10, 20, 30],
    demand_column: str = 'predicted_demand_score',
    verbose: bool = True
) -> pd.DataFrame:
    """
    MCLP 민감도 분석: 반경(km)과 설치 수(p)의 변화에 따른 커버 수요 분석

    Parameters:
    - df: 입력 데이터프레임 (위경도, 수요 포함)
    - coverage_radii_km: 커버 반경 리스트 (단위: km)
    - facility_limits: 설치 가능 수 리스트
    - demand_column: 예측 수요 컬럼명
    - verbose: 각 시나리오별 결과 출력 여부

    Returns:
    - pd.DataFrame: 시나리오별 커버 수요 및 커버율 결과 테이블
    """
    results = []
    total_demand = df[demand_column].sum()

    for r_km in coverage_radii_km:
        for p in facility_limits:
            if verbose:
                print(f"\n📌 반경 {r_km}km, 설치 {p}개 시나리오 실행 중...")
            result_df = solve_mclp(
                df.copy(),
                coverage_radius=r_km / 111.0,  # degree 환산
                facility_limit=p,
                demand_column=demand_column,
                verbose=False
            )

            selected = result_df[result_df['selected'] == 1]
            covered = selected[demand_column].sum()

            coverage_rate = covered / total_demand * 100 if total_demand else 0
            demand_satisfaction_ratio = covered / p if p else 0

            results.append({
                'coverage_radius_km': r_km,
                'facility_limit': p,
                'covered_demand': round(covered, 2),
                'total_demand': round(total_demand, 2),
                'coverage_rate': round(coverage_rate, 2),
                'demand_satisfaction_ratio': round(demand_satisfaction_ratio, 2),
                'installation_efficiency': round(demand_satisfaction_ratio, 2),
            })

            if verbose:
                print(f"✅ 커버 수요: {covered:,.2f}, 커버율: {coverage_rate:.2f}%")

    return pd.DataFrame(results)