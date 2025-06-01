# src/preprocessing/data_cleaner.py

import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        self.processed_data = {}
    
    def clean_all_data(self, datasets):
        """모든 데이터를 전처리합니다."""
        print("🔧 모든 데이터 전처리를 시작합니다...")
        
        # 전기차 등록 데이터 전처리
        if 'ev_registration_monthly' in datasets:
            self.processed_data['ev_registration'] = self._clean_ev_registration(
                datasets['ev_registration_monthly']
            )
        
        # 충전소 데이터 전처리 (1-3월 통합)
        charging_datasets = []
        for month in ['202501', '202502', '202503']:
            key = f'charging_stations_{month}'
            if key in datasets:
                charging_datasets.append(datasets[key])
        
        if charging_datasets:
            self.processed_data['charging_stations'] = self._clean_charging_stations(charging_datasets)
        
        # 시간별 충전 데이터 전처리
        if 'charging_load_hourly' in datasets:
            self.processed_data['charging_hourly'] = self._clean_charging_hourly(
                datasets['charging_load_hourly']
            )
        
        # 상업시설 데이터 전처리
        if 'commercial_facilities' in datasets:
            self.processed_data['commercial_facilities'] = self._clean_commercial_facilities(
                datasets['commercial_facilities']
            )
        
        # 격자 시스템 생성 및 수요-공급 분석
        self._create_grid_system_with_analysis()
        
        return self.processed_data
    
    def _clean_ev_registration(self, df):
        """전기차 등록 데이터를 전처리합니다."""
        print("🔧 전기차 등록 데이터 전처리 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        # Excel 파일의 복잡한 헤더 구조 처리
        cleaned_df = self._parse_ev_registration_excel(df)
        
        print(f"✅ 전기차 등록 데이터 전처리 완료: {len(cleaned_df):,}행")
        return cleaned_df
    
    def _parse_ev_registration_excel(self, df):
        """전기차 등록 Excel 파일의 복잡한 구조를 파싱합니다."""
        try:
            # 실제 데이터가 시작되는 행 찾기
            data_start_row = None
            for i in range(min(20, len(df))):
                # '구' 또는 '시군구' 같은 지역 정보가 있는 행 찾기
                row_values = df.iloc[i].fillna('').astype(str)
                if any('구' in str(val) and len(str(val)) > 1 for val in row_values):
                    # 헤더 찾기
                    if i > 0:
                        header_row = i - 1
                        data_start_row = i
                        break
            
            if data_start_row is not None:
                # 헤더와 데이터 분리
                headers = df.iloc[header_row].fillna('').astype(str)
                data_df = df.iloc[data_start_row:].copy()
                
                # 의미있는 헤더만 사용
                meaningful_headers = []
                for i, header in enumerate(headers):
                    if header and header != 'nan' and len(header.strip()) > 0:
                        meaningful_headers.append((i, header))
                    else:
                        meaningful_headers.append((i, f'col_{i}'))
                
                # 새로운 컬럼명 설정
                new_columns = [f'col_{i}' for i in range(len(df.columns))]
                for i, header in meaningful_headers:
                    if i < len(new_columns):
                        new_columns[i] = header
                
                data_df.columns = new_columns
                
                # 빈 행 제거
                data_df = data_df.dropna(how='all')
                
                # 지역 정보가 있는 행만 필터링
                region_col = None
                for col in data_df.columns:
                    if data_df[col].astype(str).str.contains('구|시|동', na=False).any():
                        region_col = col
                        break
                
                if region_col:
                    data_df = data_df[data_df[region_col].astype(str).str.contains('구|시|동', na=False)]
                
                return data_df.reset_index(drop=True)
            else:
                print("⚠️ 전기차 등록 데이터의 구조를 파악할 수 없습니다. 원본 데이터를 그대로 사용합니다.")
                return df
                
        except Exception as e:
            print(f"⚠️ 전기차 등록 데이터 파싱 중 오류: {e}")
            return df
    
    def _clean_charging_stations(self, datasets):
        """충전소 데이터를 전처리합니다."""
        print("🔧 충전소 충전량 데이터 전처리 중...")
        
        # 데이터 통합
        combined_df = pd.concat(datasets, ignore_index=True)
        
        # 충전구분 컬럼 통일
        if '충전기구분' in combined_df.columns and '충전구분' not in combined_df.columns:
            combined_df['충전구분'] = combined_df['충전기구분']
        elif '충전기구분' in combined_df.columns and '충전구분' in combined_df.columns:
            combined_df['충전구분'] = combined_df['충전구분'].fillna(combined_df['충전기구분'])
        
        print(f"📊 통합된 데이터 형태: {combined_df.shape}")
        
        # 충전량 데이터 정리
        if '충전량' in combined_df.columns:
            # 충전량을 숫자로 변환
            combined_df['충전량_numeric'] = pd.to_numeric(combined_df['충전량'], errors='coerce')
            
            # 이상치 제거
            before_count = len(combined_df)
            combined_df = combined_df[
                (combined_df['충전량_numeric'] >= 0) & 
                (combined_df['충전량_numeric'] <= 1000)
            ].copy()
            after_count = len(combined_df)
            
            print(f"📊 충전량 이상치 제거: {before_count:,}행 → {after_count:,}행")
        
        # 날짜 데이터 정리
        date_columns = ['충전종료일', '충전시작시각', '충전종료시각']
        for col in date_columns:
            if col in combined_df.columns:
                combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')
        
        print(f"✅ 충전소 데이터 전처리 완료: {len(combined_df):,}행")
        return combined_df
    
    def _clean_charging_hourly(self, df):
        """서울시 충전기 시간별 데이터를 전처리합니다."""
        print("🔧 서울시 충전기 시간별 데이터 전처리 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        # 헤더가 있는 행 찾기
        header_row = None
        for i in range(min(10, len(df))):
            if '순번' in str(df.iloc[i, 0]):
                header_row = i
                break
        
        if header_row is not None:
            # 새로운 컬럼명 설정
            new_columns = df.iloc[header_row].fillna('Unknown').astype(str)
            # 실제 데이터만 추출
            cleaned_df = df.iloc[header_row+1:].copy()
            cleaned_df.columns = new_columns
            
            # 빈 행 및 비정상 데이터 제거
            cleaned_df = cleaned_df.dropna(how='all')
            
            # 순번이 숫자인 행만 유지
            if '순번' in cleaned_df.columns:
                cleaned_df = cleaned_df[pd.to_numeric(cleaned_df['순번'], errors='coerce').notna()]
        else:
            cleaned_df = df.copy()
        
        print(f"✅ 시간별 충전 데이터 전처리 완료: {len(cleaned_df)}행")
        return cleaned_df
    
    def _clean_commercial_facilities(self, df):
        """상업시설 데이터를 전처리합니다."""
        print("🔧 상업시설 데이터 전처리 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        # 좌표 데이터 필터링 (서울 지역)
        before_count = len(df)
        if '경도' in df.columns and '위도' in df.columns:
            df_filtered = df[
                (df['경도'].between(126.7, 127.2)) & 
                (df['위도'].between(37.4, 37.7))
            ].copy()
        else:
            df_filtered = df.copy()
        
        after_count = len(df_filtered)
        print(f"📍 상업시설 좌표 필터링: {before_count:,}행 → {after_count:,}행")
        
        # 업종 분류 정리
        if '상권업종대분류명' in df_filtered.columns:
            df_filtered['업종_대분류'] = df_filtered['상권업종대분류명']
        
        if '상권업종중분류명' in df_filtered.columns:
            df_filtered['업종_중분류'] = df_filtered['상권업종중분류명']
        
        print("🏪 업종 분류 정리 완료")
        print(f"✅ 상업시설 데이터 전처리 완료: {len(df_filtered):,}행")
        return df_filtered
    
    def _create_grid_system_with_analysis(self):
        """500m x 500m 격자 시스템을 생성하고 수요-공급 분석을 수행합니다."""
        print("🗺️ 500m x 500m 격자 시스템 생성 중...")
        
        # 서울 지역 경계
        seoul_bounds = {
            'min_lat': 37.4,
            'max_lat': 37.7,
            'min_lon': 126.7,
            'max_lon': 127.2
        }
        
        # 500m를 위도/경도로 변환
        grid_size_lat = 0.5 / 111  # 500m를 위도로 변환
        grid_size_lon = 0.5 / 88.8  # 500m를 경도로 변환
        
        # 격자 생성
        lats = np.arange(seoul_bounds['min_lat'], seoul_bounds['max_lat'], grid_size_lat)
        lons = np.arange(seoul_bounds['min_lon'], seoul_bounds['max_lon'], grid_size_lon)
        
        grid_data = []
        
        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                grid_center_lat = lat + grid_size_lat/2
                grid_center_lon = lon + grid_size_lon/2
                
                # 수요 계산 (상업시설 밀도 기반)
                demand_score = self._calculate_demand_score(
                    grid_center_lat, grid_center_lon, grid_size_lat, grid_size_lon
                )
                
                # 공급 계산 (충전소 수 기반)
                supply_score = self._calculate_supply_score(
                    grid_center_lat, grid_center_lon, grid_size_lat, grid_size_lon
                )
                
                grid_data.append({
                    'grid_id': f'GRID_{i}_{j}',
                    'min_lat': lat,
                    'max_lat': lat + grid_size_lat,
                    'min_lon': lon,
                    'max_lon': lon + grid_size_lon,
                    'center_lat': grid_center_lat,
                    'center_lon': grid_center_lon,
                    'demand_score': demand_score,
                    'supply_score': supply_score
                })
        
        grid_df = pd.DataFrame(grid_data)
        self.processed_data['grid_system'] = grid_df
        
        print(f"✅ 격자 시스템 생성 완료: {len(grid_df):,}개 격자")
        print(f"📊 수요가 있는 격자: {(grid_df['demand_score'] > 0).sum():,}개")
        print(f"📊 공급이 있는 격자: {(grid_df['supply_score'] > 0).sum():,}개")
        
        return grid_df
    
    def _calculate_demand_score(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """격자 내 수요 점수를 계산합니다."""
        if 'commercial_facilities' not in self.processed_data:
            return 0
        
        facilities_df = self.processed_data['commercial_facilities']
        
        # 격자 범위 내 상업시설 수 계산
        min_lat = center_lat - grid_size_lat/2
        max_lat = center_lat + grid_size_lat/2
        min_lon = center_lon - grid_size_lon/2
        max_lon = center_lon + grid_size_lon/2
        
        if '경도' in facilities_df.columns and '위도' in facilities_df.columns:
            facilities_in_grid = facilities_df[
                (facilities_df['위도'].between(min_lat, max_lat)) &
                (facilities_df['경도'].between(min_lon, max_lon))
            ]
            
            # 업종별 가중치 적용
            demand_score = 0
            if len(facilities_in_grid) > 0:
                if '업종_대분류' in facilities_in_grid.columns or '상권업종대분류명' in facilities_in_grid.columns:
                    # 업종 컬럼 찾기
                    business_col = '업종_대분류' if '업종_대분류' in facilities_in_grid.columns else '상권업종대분류명'
                    
                    # 음식점, 소매업 등에 높은 가중치
                    weights = {'음식': 3, '소매': 2, '서비스': 1, '교육': 1, '과학': 1}
                    
                    for category, weight in weights.items():
                        count = facilities_in_grid[business_col].str.contains(category, na=False).sum()
                        demand_score += count * weight
                    
                    # 가중치에 해당하지 않는 시설들도 기본 점수 부여
                    unweighted_count = len(facilities_in_grid)
                    for category in weights.keys():
                        unweighted_count -= facilities_in_grid[business_col].str.contains(category, na=False).sum()
                    demand_score += max(0, unweighted_count) * 0.5
                else:
                    demand_score = len(facilities_in_grid)
            
            return demand_score
        
        return 0
    
    def _calculate_supply_score(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """격자 내 공급 점수를 계산합니다."""
        if 'charging_stations' not in self.processed_data:
            return 0
        
        charging_df = self.processed_data['charging_stations']
        
        # 서울 지역 충전소만 필터링
        seoul_charging = charging_df[charging_df['시도'].str.contains('서울', na=False)]
        
        if len(seoul_charging) == 0:
            return 0
        
        # 격자 범위 내 충전소 찾기 (주소 기반)
        grid_districts = self._get_districts_in_grid(center_lat, center_lon)
        
        supply_score = 0
        
        # 각 구별로 충전 기록 수 계산
        for district in grid_districts:
            # 주소에 해당 구가 포함된 충전 기록 찾기
            district_charging = seoul_charging[seoul_charging['주소'].str.contains(district, na=False)]
            
            if len(district_charging) > 0:
                # 충전량을 고려한 공급 점수 계산
                if '충전량_numeric' in district_charging.columns:
                    total_charging = district_charging['충전량_numeric'].sum()
                    supply_score += total_charging
                else:
                    supply_score += len(district_charging)
        
        # 격자 크기에 따른 조정 (더 작은 격자일수록 점수를 세분화)
        grid_area = grid_size_lat * grid_size_lon
        adjustment_factor = grid_area / (0.5/111 * 0.5/88.8)  # 기준 격자 크기 대비
        
        return supply_score * adjustment_factor
    
    def _get_districts_in_grid(self, center_lat, center_lon):
        """격자 중심점을 기준으로 해당하는 서울 구를 추정합니다."""
        # 대략적인 서울 구별 좌표 (중심점 기준)
        district_coords = {
            '강남구': (37.517, 127.047),
            '강동구': (37.530, 127.124),
            '강북구': (37.639, 127.025),
            '강서구': (37.551, 126.850),
            '관악구': (37.478, 126.951),
            '광진구': (37.538, 127.082),
            '구로구': (37.495, 126.858),
            '금천구': (37.457, 126.895),
            '노원구': (37.654, 127.056),
            '도봉구': (37.669, 127.047),
            '동대문구': (37.574, 127.040),
            '동작구': (37.512, 126.940),
            '마포구': (37.566, 126.901),
            '서대문구': (37.579, 126.937),
            '서초구': (37.484, 127.033),
            '성동구': (37.563, 127.037),
            '성북구': (37.589, 127.017),
            '송파구': (37.515, 127.106),
            '양천구': (37.517, 126.867),
            '영등포구': (37.526, 126.896),
            '용산구': (37.532, 126.990),
            '은평구': (37.603, 126.929),
            '종로구': (37.573, 126.979),
            '중구': (37.564, 126.997),
            '중랑구': (37.606, 127.093)
        }
        
        # 가장 가까운 구 찾기 (거리 0.02도 이내, 약 2km)
        nearby_districts = []
        for district, (lat, lon) in district_coords.items():
            distance = ((center_lat - lat) ** 2 + (center_lon - lon) ** 2) ** 0.5
            if distance < 0.02:  # 약 2km 반경
                nearby_districts.append(district)
        
        return nearby_districts if nearby_districts else ['서울']
    
    def save_processed_data(self, output_dir='data/processed'):
        """전처리된 데이터를 저장합니다."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        print("💾 전처리된 데이터 저장 중...")
        
        saved_files = []
        for data_name, df in self.processed_data.items():
            filename = f"{data_name}_processed.csv"
            filepath = os.path.join(output_dir, filename)
            
            try:
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
                print(f"✅ {filepath} 저장 완료")
                saved_files.append(filename)
            except Exception as e:
                print(f"❌ {filepath} 저장 실패: {e}")
        
        # 요약 정보 저장
        summary_data = []
        for data_name, df in self.processed_data.items():
            summary_data.append({
                'dataset': data_name,
                'rows': len(df),
                'columns': len(df.columns),
                'file_saved': f"{data_name}_processed.csv"
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_path = os.path.join(output_dir, 'preprocessing_summary.csv')
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        
        print()
        print("📋 전처리 요약:")
        print(summary_df.to_string(index=False))
        print()
        print("✅ 모든 데이터 전처리 완료!")
        print(f"📁 저장된 파일 수: {len(saved_files)}개")
        print(f"📁 저장 위치: {output_dir}/")
        
        return summary_df

# 외부에서 호출할 수 있는 함수들
def run_all_preprocessing():
    """모든 전처리를 실행하는 함수"""
    from .data_loader import DataLoader
    
    # 데이터 로딩
    loader = DataLoader()
    datasets = loader.load_all_datasets()
    
    if not datasets:
        print("❌ 로딩된 데이터가 없습니다.")
        return None
    
    # 데이터 전처리
    cleaner = DataCleaner()
    processed_data = cleaner.clean_all_data(datasets)
    
    # 데이터 저장
    summary = cleaner.save_processed_data()
    
    return processed_data, summary

def create_data_cleaner():
    """DataCleaner 인스턴스를 생성하는 함수"""
    return DataCleaner()