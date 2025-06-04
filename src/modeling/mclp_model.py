import pandas as pd
import numpy as np
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, LpStatus
from geopy.distance import geodesic
from tqdm import tqdm
from pulp import PULP_CBC_CMD

def solve_mclp(
    df: pd.DataFrame,
    coverage_radius: float = 0.02,  # (위도/경도 degree 단위 ≒ 약 2.2km)
    facility_limit: int = 30,
    demand_column: str = 'predicted_demand_score',
    verbose: bool = True
) -> tuple[pd.DataFrame, dict, dict]:
    """
    MCLP (Maximum Coverage Location Problem) 최적 입지 선정 함수 (coverage_matrix 포함)

    Returns:
    - df: 설치 여부 'selected' 포함 DataFrame
    - summary: 성능 지표 dict
    - coverage_matrix: 설치지-수요지 매핑 dict
    """
    df = df.copy()
    df['demand'] = df[demand_column]
    coords = df[['center_lat', 'center_lon']].values.tolist()
    sites = list(df.index)
    demand_points = list(df.index)

    # ===============================
    # 1. 커버리지 행렬 생성
    # ===============================
    coverage_matrix = {}
    for i, (lat_i, lon_i) in tqdm(enumerate(coords), total=len(coords), desc="커버리지 행렬 생성"):
        covered = []
        for j, (lat_j, lon_j) in enumerate(coords):
            distance_km = geodesic((lat_i, lon_i), (lat_j, lon_j)).km
            if distance_km <= coverage_radius * 111.0:  # degree → km 환산
                covered.append(j)
        if covered:
            coverage_matrix[i] = covered

    # 커버되지 않는 수요지 체크
    uncovered = [j for j in demand_points if all(j not in coverage_matrix[i] for i in coverage_matrix)]
    if uncovered and verbose:
        print(f"커버되지 않는 수요지 존재: {len(uncovered)}개 (예: {uncovered[:5]})")

    # ===============================
    # 2. 최적화 문제 정의
    # ===============================
    prob = LpProblem("Maximize_Coverage", LpMaximize)
    x = LpVariable.dicts("Site", sites, cat='Binary')
    y = LpVariable.dicts("Covered", demand_points, cat='Binary')

    # 목적함수
    prob += lpSum(df.loc[j, 'demand'] * y[j] for j in demand_points)

    # 설치 제한
    prob += lpSum(x[i] for i in sites) <= facility_limit

    # 커버 제약조건
    for j in demand_points:
        covering_sites = [i for i in coverage_matrix if j in coverage_matrix[i]]
        if covering_sites:
            prob += y[j] <= lpSum(x[i] for i in covering_sites)
        else:
            prob += y[j] == 0  # 커버 불가능한 경우 명시

    # 최적화 수행 (CBC solver + 로그 비활성화)
    solver = PULP_CBC_CMD(msg=False)
    status = prob.solve(solver)

    # 상태 확인 및 문제 발생시 경고 출력
    status_str = LpStatus[prob.status]
    if status_str != 'Optimal':
        print(f"최적화 실패 상태: {status_str}")

    selected_sites = [i for i in sites if x[i].varValue == 1.0]
    df['selected'] = df.index.isin(selected_sites).astype(int)

    # ===============================
    # 3. 커버된 수요 계산
    # ===============================
    covered_points = set()
    for i in selected_sites:
        covered_points.update(coverage_matrix.get(i, []))

    covered_demand = df.loc[list(covered_points), 'demand'].sum()
    total_demand = df['demand'].sum()
    coverage_rate = covered_demand / total_demand * 100 if total_demand else 0
    efficiency = covered_demand / facility_limit if facility_limit else 0

    if verbose:
        print(f"설치지 수: {len(selected_sites)}개")
        print(f"커버 수요: {covered_demand:,.2f} / 총 수요: {total_demand:,.2f}")
        print(f"커버율: {coverage_rate:.2f}%")

    summary = {
        'selected_count': len(selected_sites),
        'covered_demand': round(covered_demand, 2),
        'total_demand': round(total_demand, 2),
        'coverage_rate': round(coverage_rate, 2),
        'demand_satisfaction_ratio': round(efficiency, 2),
        'coverage_radius_km': round(coverage_radius * 111.0, 2),
        'facility_limit': facility_limit
    }

    return df, summary, coverage_matrix

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from modeling.mclp_model import solve_mclp

def run_sensitivity_analysis(
    df: pd.DataFrame,
    coverage_radii_km: list = [1.0, 2.0, 3.0],
    facility_limits: list = [10, 20, 30],
    demand_column: str = 'predicted_demand_score',
    verbose: bool = True,
    save_path: str = None,
    plot: bool = False
) -> pd.DataFrame:
    """
    MCLP 민감도 분석: 반경(km)과 설치 수 변화에 따른 커버 수요 분석

    Parameters:
    - df: 입력 데이터프레임 (위경도, 수요 포함)
    - coverage_radii_km: 커버 반경 리스트 (단위: km)
    - facility_limits: 설치 가능 수 리스트
    - demand_column: 예측 수요 컬럼명
    - verbose: 각 시나리오별 결과 출력 여부
    - save_path: CSV 저장 경로 (선택)
    - plot: True일 경우 결과 시각화 출력

    Returns:
    - pd.DataFrame: 시나리오별 커버 수요 및 커버율 결과 테이블
    """
    results = []
    total_demand = df[demand_column].sum()

    for r_km in coverage_radii_km:
        for p in facility_limits:
            if verbose:
                print(f"\n반경 {r_km}km, 설치 {p}개 시나리오 실행 중...")

            result_df, summary, _ = solve_mclp(
                df.copy(),
                coverage_radius=r_km / 111.0,  # degree → km 환산
                facility_limit=p,
                demand_column=demand_column,
                verbose=False
            )

            results.append({
                'coverage_radius_km': r_km,
                'facility_limit': p,
                'covered_demand': round(summary['covered_demand'], 2),
                'total_demand': round(total_demand, 2),
                'coverage_rate': round(summary['coverage_rate'], 2), # 전체 수요 중에서 커버된 수요가 차지하는 비율
                'demand_satisfaction_ratio': round(summary['covered_demand'] / p, 2),  # 설치 1개당 커버한 수요의 평균
            })

            if verbose:
                print(f"커버 수요: {summary['covered_demand']:,.2f}, 커버율: {summary['coverage_rate']:.2f}%")

    result_df = pd.DataFrame(results)

    if save_path:
        result_df.to_csv(save_path, index=False)
        if verbose:
            print(f"\n민감도 분석 결과 저장 완료: {save_path}")

    if plot:
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=result_df, x="facility_limit", y="coverage_rate", hue="coverage_radius_km", marker="o")
        plt.title("설치 수에 따른 커버율 변화")
        plt.xlabel("Facility Limit")
        plt.ylabel("Coverage Rate (%)")
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    return result_df
