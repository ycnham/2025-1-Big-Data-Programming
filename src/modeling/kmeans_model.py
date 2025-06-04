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
) -> tuple[pd.DataFrame, int]:
    """
    KMeans 클러스터링 실행 함수 (k값 반환 포함, 인덱스 안정성 보장)

    Parameters:
    - df: 입력 데이터 (DataFrame)
    - mode: 'manual' 또는 'auto'
    - manual_k: 수동 설정 시 사용할 클러스터 수
    - return_top_cluster_only: 수요가 가장 높은 클러스터만 반환 여부

    Returns:
    - Tuple[pd.DataFrame, int]: 클러스터링된 DataFrame, 사용된 클러스터 수 k
    """

    # 클러스터링에 사용할 feature만 추출 + 결측 제거
    feature_cols = ['demand_score', 'supply_score', 'center_lat', 'center_lon']
    features = df[feature_cols].copy()
    valid_features = features.dropna()
    valid_index = valid_features.index

    # 클러스터 수 결정 (Elbow Method)
    if mode == 'auto':
        inertias = []
        k_range = range(2, 11)
        for k_val in tqdm(k_range, desc="Finding optimal k"):
            model = KMeans(n_clusters=k_val, random_state=42)
            model.fit(valid_features)
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

    # KMeans 학습
    kmeans = KMeans(n_clusters=k, random_state=42)
    cluster_labels = kmeans.fit_predict(valid_features)

    # 원래 df에 cluster 할당
    df = df.copy()  # 원본 df 수정 방지
    df.loc[valid_index, 'cluster'] = cluster_labels

    # 수요 중심 클러스터 평균 출력
    if verbose:
        cluster_means = df.dropna(subset=['cluster']).groupby('cluster')['demand_score'].mean().sort_values(ascending=False)
        print("\n[Cluster별 평균 수요]")
        print(cluster_means)

    # 가장 수요가 높은 클러스터만 선택
    if return_top_cluster_only:
        top_cluster = cluster_means.idxmax()
        df = df[df['cluster'] == top_cluster].copy().reset_index(drop=True)
        if verbose:
            print(f"\n[필터링] 수요가 가장 높은 클러스터 (cluster={top_cluster})만 반환됨.")

    return df, k