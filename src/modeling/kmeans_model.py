import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from tqdm import tqdm

def run_kmeans(
    df: pd.DataFrame,
    mode: str = 'manual',
    manual_k: int = 5,
    verbose: bool = True,
    return_top_cluster_only: bool = False
) -> pd.DataFrame:
    """
    KMeans 클러스터링 실행 함수

    Parameters:
    - df: pd.DataFrame  
        'demand_score', 'supply_score', 'center_lat', 'center_lon' 컬럼이 포함된 격자 데이터
    - mode: str, default='manual'  
        클러스터 개수 설정 방식. 'manual' 또는 'auto'
    - manual_k: int, default=5  
        수동 모드일 경우 사용할 클러스터 개수
    - verbose: bool, default=True  
        클러스터 개수 및 평균 수요 출력 여부
    - return_top_cluster_only: bool, default=False  
        True일 경우, 수요 평균이 가장 높은 클러스터에 속한 데이터만 반환

    Returns:
    - pd.DataFrame  
        'cluster' 열이 추가된 데이터프레임.  
        return_top_cluster_only=True인 경우 해당 클러스터의 데이터만 포함됨
    """

    # 클러스터링 대상 feature 추출
    features = df[['demand_score', 'supply_score', 'center_lat', 'center_lon']].dropna()

    if mode == 'auto':
        # Elbow method로 최적 k 자동 탐색
        inertias = []
        k_range = range(2, 11)

        for k in tqdm(k_range, desc="Finding optimal k"):
            model = KMeans(n_clusters=k, random_state=42)
            model.fit(features)
            inertias.append(model.inertia_)

        inertia_diff = np.diff(inertias)
        inertia_diff2 = np.diff(inertia_diff)
        optimal_k_idx = np.argmax(np.abs(inertia_diff2)) + 2
        k = k_range[optimal_k_idx - 2]

        if verbose:
            print(f"[AUTO MODE] 최적 k = {k}")
            print("Inertia by k:", dict(zip(k_range, inertias)))

    else:
        k = manual_k
        if verbose:
            print(f"[MANUAL MODE] 수동 설정 k = {k}")

    # 클러스터링 수행
    kmeans = KMeans(n_clusters=k, random_state=42)
    df['cluster'] = kmeans.fit_predict(features)

    # 클러스터별 평균 수요 출력
    if verbose:
        cluster_means = df.groupby('cluster')['demand_score'].mean().sort_values(ascending=False)
        print("\n[Cluster별 평균 수요]")
        print(cluster_means)

    # 수요 밀집 클러스터만 필터링
    if return_top_cluster_only:
        top_cluster = df.groupby('cluster')['demand_score'].mean().idxmax()
        df = df[df['cluster'] == top_cluster].copy()
        df = df.reset_index(drop=True)
        if verbose:
            print(f"\n[필터링] 수요가 가장 높은 클러스터 (cluster={top_cluster})만 반환됨.")

    return df
