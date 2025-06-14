import pandas as pd
from tqdm import tqdm

def map_stations_to_grid(env_path, grid_path, output_path, encoding='cp949'):
    # 📂 데이터 로드
    env_station = pd.read_csv(env_path, encoding=encoding)
    grid = pd.read_csv(grid_path)
    tqdm.pandas()

    # 1. 필수 정보 결측치 제거
    env_station = env_station.dropna(subset=["주소", "위도경도"])

    # 2. 서울시 데이터만 필터링
    env_station = env_station[env_station["시도"] == "서울특별시"].copy()

    # 3. 위도/경도 분리
    env_station[['위도', '경도']] = env_station["위도경도"].str.split(",", expand=True).astype(float)

    # 4. 가장 가까운 격자 찾기
    def find_nearest_grid(lat, lon):
        dists = ((grid['center_lat'] - lat) ** 2 + (grid['center_lon'] - lon) ** 2)
        return grid.loc[dists.idxmin(), 'grid_id']

    env_station['grid_id'] = env_station.progress_apply(
        lambda row: find_nearest_grid(row['위도'], row['경도']),
        axis=1
    )

    # 5. 필요한 컬럼 정제
    processed = env_station[[
        '설치년도', '시도', '군구', '주소', '충전소명',
        '시설구분(대)', '시설구분(소)', '기종(대)', '기종(소)',
        '운영기관(대)', '운영기관(소)', '충전기타입',
        '이용자제한', '위도', '경도', 'grid_id'
    ]].copy()

    # 6. 저장
    processed.to_csv(output_path, index=False)
    print(f"✅ 저장 완료: {output_path}")