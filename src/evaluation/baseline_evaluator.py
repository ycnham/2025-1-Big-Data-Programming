import pandas as pd
import numpy as np

def evaluate_existing_stations(
    features: pd.DataFrame,
    station_df: pd.DataFrame,
    lat_col: str = 'lat',
    lon_col: str = 'lon',
    coord_col: str = '위도경도',
    verbose: bool = True
) -> dict:
    """
    기존 충전소 위치를 기반으로 커버 수요를 계산하는 baseline 평가 함수

    Parameters:
    - features: 격자별 예측 수요 DataFrame (grid_id, center_lat, center_lon, predicted_demand_score 포함)
    - station_df: 기존 충전소 위치 DataFrame (lat/lon 또는 '위도경도' 열 포함)
    - lat_col, lon_col: 위도/경도 열 이름
    - coord_col: '위도,경도' 문자열 열 이름
    - verbose: 평가 지표 출력 여부

    Returns:
    - dict: {'coverage': float, 'coverage_rate': float, 'covered_grids': int}
    """

    # 위도, 경도 추출
    if coord_col in station_df.columns:
        station_df[[lat_col, lon_col]] = station_df[coord_col].str.split(",", expand=True).astype(float)

    # 가장 가까운 격자 grid_id 찾기
    def find_nearest_grid(lat, lon):
        dists = ((features['center_lat'] - lat) ** 2 + (features['center_lon'] - lon) ** 2)
        return features.loc[dists.idxmin(), 'grid_id']

    station_df['grid_id'] = station_df.apply(
        lambda row: find_nearest_grid(row[lat_col], row[lon_col]),
        axis=1
    )

    # 중복된 grid_id 제거 후 coverage 계산
    station_grids = station_df['grid_id'].unique()
    covered = features[features['grid_id'].isin(station_grids)]
    coverage = covered['predicted_demand_score'].sum()
    total = features['predicted_demand_score'].sum()
    rate = coverage / total * 100

    if verbose:
        print(f"[Baseline ① 기존 충전소 기준]")
        print(f"- 설치 격자 수: {len(station_grids)}")
        print(f"- 커버 수요: {coverage:,.2f}")
        print(f"- 전체 수요: {total:,.2f}")
        print(f"- 커버율: {rate:.2f}%")

    return {
        'coverage': coverage,
        'coverage_rate': rate,
        'covered_grids': len(station_grids)
    }
    
def evaluate_random_installation(features: pd.DataFrame, n: int, seed: int = 42, verbose: bool = True) -> dict:
    """
    전체 격자 중 무작위로 n개를 선택해 커버 수요 평가

    Returns:
    - dict with keys: 'coverage', 'coverage_rate', 'covered_grids'
    """
    np.random.seed(seed)
    sampled = features.sample(n=n, random_state=seed)
    coverage = sampled['predicted_demand_score'].sum()
    total = features['predicted_demand_score'].sum()
    rate = coverage / total * 100

    if verbose:
        print(f"[Baseline ② 랜덤 설치]")
        print(f"- 설치 격자 수: {n}")
        print(f"- 커버 수요: {coverage:,.2f}")
        print(f"- 전체 수요: {total:,.2f}")
        print(f"- 커버율: {rate:.2f}%")

    return {'coverage': coverage, 'coverage_rate': rate, 'covered_grids': n}

def evaluate_cluster_centers(features: pd.DataFrame, cluster_col: str = 'cluster', verbose: bool = True) -> dict:
    """
    클러스터 중심에 가장 가까운 격자를 선택하여 커버 수요 평가

    Returns:
    - dict with keys: 'coverage', 'coverage_rate', 'covered_grids'
    """
    selected_ids = []

    for cluster_id, group in features.groupby(cluster_col):
        center_lat = group['center_lat'].mean()
        center_lon = group['center_lon'].mean()
        dists = ((group['center_lat'] - center_lat) ** 2 + (group['center_lon'] - center_lon) ** 2)
        nearest = group.loc[dists.idxmin(), 'grid_id']
        selected_ids.append(nearest)

    selected = features[features['grid_id'].isin(selected_ids)]
    coverage = selected['predicted_demand_score'].sum()
    total = features['predicted_demand_score'].sum()
    rate = coverage / total * 100

    if verbose:
        print(f"[Baseline ③ 클러스터 중심 설치]")
        print(f"- 설치 격자 수: {len(selected_ids)}")
        print(f"- 커버 수요: {coverage:,.2f}")
        print(f"- 전체 수요: {total:,.2f}")
        print(f"- 커버율: {rate:.2f}%")

    return {
        'coverage': coverage,
        'coverage_rate': rate,
        'covered_grids': len(selected_ids)
    }

def evaluate_mclp_result(features: pd.DataFrame, verbose: bool = True) -> dict:
    selected = features[features['selected'] == 1]
    coverage = selected['predicted_demand_score'].sum()
    total = features['predicted_demand_score'].sum()
    rate = coverage / total * 100

    if verbose:
        print(f"[모델 (MCLP) 결과]")
        print(f"- 설치 격자 수: {len(selected)}")
        print(f"- 커버 수요: {coverage:,.2f}")
        print(f"- 전체 수요: {total:,.2f}")
        print(f"- 커버율: {rate:.2f}%")

    return {
        'coverage': coverage,
        'coverage_rate': rate,
        'covered_grids': len(selected)
    }