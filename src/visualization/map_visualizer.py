import folium
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as colors
import os

def visualize_cluster_map(
    all_features_path: str,
    filtered_features_path: str,
    output_path: str = "../outputs/maps/cluster_colored_visualization.html",
    map_center: tuple = (37.5665, 126.9780),
    zoom_start: int = 11,
    verbose: bool = True
):
    """
    KMeans 클러스터링 결과를 folium으로 시각화하는 함수.

    Parameters:
    - all_features_path: 전체 grid 데이터 경로
    - filtered_features_path: 클러스터링 필터링된 grid 데이터 경로
    - output_path: 저장할 HTML 파일 경로
    - map_center: 서울 지도 중심 좌표
    - zoom_start: 초기 줌 레벨
    - verbose: 완료 메시지 출력 여부
    """
    # 데이터 로드
    all_features = pd.read_csv(all_features_path)
    filtered_features = pd.read_csv(filtered_features_path)

    # 클러스터 수 및 색상 설정
    unique_clusters = sorted(filtered_features['cluster'].unique())
    num_clusters = len(unique_clusters)
    colormap = cm.get_cmap('Set1', num_clusters)
    cluster_colors = {
        cluster: colors.to_hex(colormap(i))
        for i, cluster in enumerate(unique_clusters)
    }

    # 지도 생성
    seoul_map = folium.Map(location=map_center, zoom_start=zoom_start)

    # 전체 격자 (회색)
    for _, row in all_features.iterrows():
        folium.CircleMarker(
            location=[row["center_lat"], row["center_lon"]],
            radius=2,
            color='gray',
            fill=True,
            fill_opacity=0.2
        ).add_to(seoul_map)

    # 필터링된 클러스터 (컬러)
    for _, row in filtered_features.iterrows():
        cluster = row["cluster"]
        color = cluster_colors.get(cluster, 'blue')
        folium.CircleMarker(
            location=[row["center_lat"], row["center_lon"]],
            radius=3,
            color=color,
            fill=True,
            fill_opacity=0.9
        ).add_to(seoul_map)

    # 저장
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    seoul_map.save(output_path)

    if verbose:
        print(f"클러스터 지도 저장 완료: {output_path}")

def visualize_selected_sites_map(
    features_path: str,
    output_path: str = "../outputs/maps/mclp_selected_sites.html",
    map_center: tuple = (37.5665, 126.9780),
    zoom_start: int = 11,
    verbose: bool = True
):
    """
    MCLP 결과 기반 설치 여부 시각화 (selected == 1)
    """
    import folium
    import pandas as pd
    import os

    df = pd.read_csv(features_path)

    # 지도 생성
    seoul_map = folium.Map(location=map_center, zoom_start=zoom_start)

    for _, row in df.iterrows():
        color = 'red' if row.get('selected', 0) == 1 else 'gray'
        folium.CircleMarker(
            location=[row["center_lat"], row["center_lon"]],
            radius=3,
            color=color,
            fill=True,
            fill_opacity=0.8 if color == 'red' else 0.2
        ).add_to(seoul_map)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    seoul_map.save(output_path)

    if verbose:
        print(f"설치지 시각화 저장 완료: {output_path}")
