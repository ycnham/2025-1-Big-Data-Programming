import pandas as pd
from modeling.kmeans_model import run_kmeans

def generate_kmeans_features(
    grid_path: str,
    features_path: str,
    output_path: str,
    mode: str = 'manual',
    manual_k: int = 5,
    return_top_cluster_only: bool = True,
    verbose: bool = True
) -> tuple[pd.DataFrame, int]:
    """
    KMeans 클러스터링 실행 후, 수요 중심 격자 feature 저장

    Returns:
    - features (DataFrame): 병합된 결과
    - used_k (int): 사용된 클러스터 수
    """

    # 데이터 로드
    grid = pd.read_csv(grid_path)
    features_all = pd.read_csv(features_path)

    # 클러스터링
    grid, used_k = run_kmeans(
        df=grid,
        mode=mode,
        manual_k=manual_k,
        return_top_cluster_only=return_top_cluster_only,
        verbose=verbose
    )

    # 필요한 컬럼만 유지
    grid = grid[['grid_id', 'center_lat', 'center_lon', 'cluster']]

    # 병합
    features_all = features_all.drop(columns=['center_lat', 'center_lon'], errors='ignore')
    features = features_all.merge(grid, on='grid_id', how='inner')
    features['cluster'] = features['cluster'].astype(int)
    features = features.loc[:, ~features.columns.duplicated()]

    # 저장
    features.to_csv(output_path, index=False)
    if verbose:
        print(f"✅ 저장 완료: {output_path}")
        print(f"사용 가능한 feature 컬럼: {features.columns.tolist()}")

    return features, used_k
