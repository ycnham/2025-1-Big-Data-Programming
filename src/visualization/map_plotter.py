import folium
import pandas as pd
import os

def plot_strategy_map(
    final_df: pd.DataFrame,
    coord_df: pd.DataFrame,
    output_path: str,
    demand_col: str = "predicted_demand_score",
    center_lat: float = 37.55,
    center_lon: float = 126.98,
    zoom_start: int = 11
):
    """
    전략별 신규 커버 격자 위치를 지도 위에 시각화

    Parameters:
    - final_df: 전략별 신규 격자 데이터 (grid_id, strategy, percentile 등 포함)
    - coord_df: 좌표 정보 포함된 원본 데이터 (grid_id, center_lat, center_lon 포함)
    - output_path: 저장할 html 파일 경로
    """
    # 좌표 병합
    map_df = final_df.merge(coord_df[['grid_id', 'center_lat', 'center_lon']], on='grid_id', how='left')

    # 지도 생성
    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start)

    # 색상 지정
    color_map = {
        '클러스터 기반 설치': 'blue',
        'MCLP 추천 설치': 'red',
        '랜덤 설치': 'green'
    }

    # 마커 추가
    for _, row in map_df.iterrows():
        folium.CircleMarker(
            location=[row['center_lat'], row['center_lon']],
            radius=7,
            color=color_map.get(row['strategy'], 'gray'),
            fill=True,
            fill_color=color_map.get(row['strategy'], 'gray'),
            fill_opacity=0.8,
            popup=folium.Popup(
                f"{row['grid_id']}<br>{row['strategy']}<br>수요: {row[demand_col]:.1f}<br>상위 {row['percentile']:.2f}%",
                max_width=250
            )
        ).add_to(m)

    # 저장
    m.save(output_path)
    print(f"\n🗺️ 지도 파일 저장 완료: {output_path}")
