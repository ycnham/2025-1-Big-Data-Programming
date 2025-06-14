import pandas as pd

def evaluate_installed_coverage(grid_path: str, station_path: str, verbose: bool = True) -> dict:
    """
    격자 전체 대비 설치 충전소 비율 및 수요 커버율 계산

    Parameters:
    - grid_path: 전체 격자 데이터 (demand_score 포함)
    - station_path: 충전소 위치 (grid_id 포함)
    - verbose: 콘솔 출력 여부

    Returns:
    - dict: 결과 지표 딕셔너리
    """
    all_df = pd.read_csv(grid_path)
    stations = pd.read_csv(station_path)

    total_grids = all_df['grid_id'].nunique()
    installed_grids = stations['grid_id'].nunique()
    install_ratio = installed_grids / total_grids * 100

    covered = all_df[all_df['grid_id'].isin(stations['grid_id'])]
    covered_demand = covered['demand_score'].sum()
    total_demand = all_df['demand_score'].sum()
    coverage_rate = covered_demand / total_demand * 100

    if verbose:
        print("기존 충전소 기반 분석 결과")
        print(f"- 전체 서울 격자 수: {total_grids}")
        print(f"- 설치 격자 수: {installed_grids}")
        print(f"- 서울 전체 기준 설치 비율: {install_ratio:.2f}%")
        print(f"- 수요 기준 커버율: {coverage_rate:.2f}%")

    return {
        'total_grids': total_grids,
        'installed_grids': installed_grids,
        'install_ratio': install_ratio,
        'covered_demand': covered_demand,
        'total_demand': total_demand,
        'coverage_rate': coverage_rate
    }

def evaluate_grid_coverage(df_selected, df_all, demand_col='demand_score', label="클러스터링 기반", verbose=True):
    total_grids = df_all['grid_id'].nunique()
    selected_grids = df_selected['grid_id'].nunique()
    install_ratio = selected_grids / total_grids * 100

    total_demand = df_all[demand_col].sum()
    covered_demand = df_selected[demand_col].sum()
    coverage_rate = covered_demand / total_demand * 100

    if verbose:
        print(f"[{label} 분석 결과]")
        print(f"- 전체 서울 격자 수: {total_grids}")
        print(f"- 선택된 격자 수: {selected_grids}")
        print(f"- 서울 전체 기준 설치 비율: {install_ratio:.2f}%")
        print(f"- 수요 기준 커버율: {coverage_rate:.2f}%")

    return {
        "total_grids": total_grids,
        "selected_grids": selected_grids,
        "install_ratio": install_ratio,
        "total_demand": total_demand,
        "covered_demand": covered_demand,
        "coverage_rate": coverage_rate
    }
