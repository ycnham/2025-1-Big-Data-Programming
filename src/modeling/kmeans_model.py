def run_kmeans(
    df: pd.DataFrame,
    mode: str = 'manual',
    manual_k: int = 5,
    verbose: bool = True,
    return_top_cluster_only: bool = False
) -> tuple[pd.DataFrame, int]:
    """
    KMeans 클러스터링 실행 함수 (k값 반환 포함)

    Returns:
    - Tuple[pd.DataFrame, int]: 클러스터링된 DataFrame, 사용된 클러스터 수 k
    """

    features = df[['demand_score', 'supply_score', 'center_lat', 'center_lon']].dropna()

    if mode == 'auto':
        inertias = []
        k_range = range(2, 11)

        for k_val in tqdm(k_range, desc="Finding optimal k"):
            model = KMeans(n_clusters=k_val, random_state=42)
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

    kmeans = KMeans(n_clusters=k, random_state=42)
    df['cluster'] = kmeans.fit_predict(features)

    if verbose:
        cluster_means = df.groupby('cluster')['demand_score'].mean().sort_values(ascending=False)
        print("\n[Cluster별 평균 수요]")
        print(cluster_means)

    if return_top_cluster_only:
        top_cluster = df.groupby('cluster')['demand_score'].mean().idxmax()
        df = df[df['cluster'] == top_cluster].copy().reset_index(drop=True)
        if verbose:
            print(f"\n[필터링] 수요가 가장 높은 클러스터 (cluster={top_cluster})만 반환됨.")

    return df, k  # 👈 클러스터링 결과 + 사용된 k 반환