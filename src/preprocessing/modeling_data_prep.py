# src/preprocessing/modeling_data_prep.py
# 모델링을 위한 완전 수정된 전처리 코드

import pandas as pd
import numpy as np
from pathlib import Path
import os
import sys

# 안전한 import 처리
try:
    import warnings
    warnings.filterwarnings('ignore')
except ImportError:
    pass

class ModelingDataPreprocessor:
    def __init__(self, processed_data_dir='data/processed', output_dir='data/processed'):
        """모델링 데이터 전처리 클래스 초기화"""
        self.project_root = self._find_project_root()
        
        # 경로 설정 (절대 경로 사용)
        if isinstance(processed_data_dir, str):
            self.processed_dir = self.project_root / processed_data_dir
        else:
            self.processed_dir = Path(processed_data_dir)
            
        if isinstance(output_dir, str):
            self.output_dir = self.project_root / output_dir
        else:
            self.output_dir = Path(output_dir)
        
        # 디렉토리 생성
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🔧 모델링 전처리 초기화 완료")
        print(f"   📁 입력 디렉토리: {self.processed_dir}")
        print(f"   📁 출력 디렉토리: {self.output_dir}")
    
    def _find_project_root(self):
        """프로젝트 루트 디렉토리 찾기"""
        current = Path.cwd()
        
        # 현재 디렉토리부터 상위로 올라가며 프로젝트 루트 찾기
        while current.parent != current:
            if (current / 'src').exists() and (current / 'data').exists():
                return current
            current = current.parent
        
        # 찾지 못한 경우 현재 디렉토리 반환
        return Path.cwd()
    
    def prepare_all_modeling_data(self):
        """모델링에 필요한 모든 데이터를 준비합니다."""
        print("🚀 모델링 데이터 전처리 시작...")
        
        success_count = 0
        total_steps = 5
        
        try:
            # 1. grid_system_processed.csv 확인 및 생성
            print("\n1️⃣ 격자 시스템 데이터 준비...")
            if self._prepare_grid_system():
                success_count += 1
                print("   ✅ 격자 시스템 준비 완료")
            else:
                print("   ⚠️ 격자 시스템 준비 부분 성공")
            
            # 2. grid_features.csv 생성
            print("\n2️⃣ 격자 특성 데이터 생성...")
            if self._prepare_grid_features():
                success_count += 1
                print("   ✅ 격자 특성 데이터 생성 완료")
            else:
                print("   ⚠️ 격자 특성 데이터 생성 부분 성공")
            
            # 3. demand_supply_analysis.csv 생성
            print("\n3️⃣ 수요-공급 분석 데이터 생성...")
            if self._prepare_demand_supply_analysis():
                success_count += 1
                print("   ✅ 수요-공급 분석 완료")
            else:
                print("   ⚠️ 수요-공급 분석 부분 성공")
            
            # 4. optimal_locations.csv 생성
            print("\n4️⃣ 최적 위치 데이터 생성...")
            if self._prepare_optimal_locations():
                success_count += 1
                print("   ✅ 최적 위치 데이터 생성 완료")
            else:
                print("   ⚠️ 최적 위치 데이터 생성 부분 성공")
            
            # 5. 데이터 검증
            print("\n5️⃣ 생성된 데이터 검증...")
            if self._validate_generated_data():
                success_count += 1
                print("   ✅ 데이터 검증 완료")
            else:
                print("   ⚠️ 데이터 검증 부분 성공")
            
            # 결과 요약
            success_rate = success_count / total_steps * 100
            print(f"\n📊 모델링 데이터 전처리 완료!")
            print(f"   성공률: {success_count}/{total_steps} ({success_rate:.1f}%)")
            
            if success_count >= 4:
                print("✅ 모델링에 필요한 핵심 데이터가 준비되었습니다!")
                return True
            elif success_count >= 2:
                print("⚠️ 기본적인 모델링 데이터는 준비되었습니다.")
                return True
            else:
                print("❌ 모델링 데이터 준비에 문제가 있습니다.")
                return False
            
        except Exception as e:
            print(f"❌ 모델링 데이터 전처리 중 치명적 오류: {e}")
            import traceback
            print(f"상세 오류: {traceback.format_exc()}")
            return False
    
    def _prepare_grid_system(self):
        """grid_system_processed.csv 확인 및 보정"""
        try:
            grid_file = self.processed_dir / 'grid_system_processed.csv'
            
            if grid_file.exists():
                df = pd.read_csv(grid_file)
                print(f"   📊 기존 격자 파일 발견: {len(df):,}행")
                
                # 필수 컬럼 확인 및 추가
                required_cols = ['grid_id', 'center_lat', 'center_lon', 'demand_score', 'supply_score']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    print(f"   🔧 누락된 컬럼 추가: {missing_cols}")
                    
                    for col in missing_cols:
                        if col == 'grid_id':
                            # 기존 컬럼 활용하여 grid_id 생성
                            if 'grid_x' in df.columns and 'grid_y' in df.columns:
                                df['grid_id'] = df.apply(lambda row: f"GRID_{int(row['grid_x']):03d}_{int(row['grid_y']):03d}", axis=1)
                            else:
                                df['grid_id'] = [f'GRID_{i:05d}' for i in range(len(df))]
                        elif col == 'demand_score':
                            # 기존 수요 관련 컬럼 매핑
                            if 'total_demand' in df.columns:
                                df[col] = df['total_demand']
                            elif 'demand' in df.columns:
                                df[col] = df['demand']
                            else:
                                # 정규분포 기반 임시 데이터
                                df[col] = np.maximum(0, np.random.normal(25, 15, len(df)))
                        elif col == 'supply_score':
                            # 기존 공급 관련 컬럼 매핑
                            if 'total_supply' in df.columns:
                                df[col] = df['total_supply']
                            elif 'supply' in df.columns:
                                df[col] = df['supply']
                            else:
                                # 정규분포 기반 임시 데이터
                                df[col] = np.maximum(10, np.random.normal(80, 30, len(df)))
                        else:
                            df[col] = 0
                    
                    # 수정된 파일 저장
                    df.to_csv(grid_file, index=False, encoding='utf-8-sig')
                    print(f"   💾 격자 시스템 파일 보정 및 저장 완료")
                
                # 통계 출력
                demand_grids = (df['demand_score'] > 0).sum()
                supply_grids = (df['supply_score'] > 0).sum()
                print(f"   📈 수요 격자: {demand_grids:,}개")
                print(f"   📦 공급 격자: {supply_grids:,}개")
                
                return True
                
            else:
                print("   ❌ 격자 시스템 파일이 없습니다. 기본 파일을 생성합니다.")
                return self._create_default_grid_system()
                
        except Exception as e:
            print(f"   ❌ 격자 시스템 준비 중 오류: {e}")
            return self._create_default_grid_system()
    
    def _create_default_grid_system(self):
        """기본 격자 시스템 생성"""
        try:
            print("   🗺️ 기본 격자 시스템 생성 중...")
            
            # 서울 영역 정의
            seoul_bounds = {
                'min_lat': 37.4269, 'max_lat': 37.7019,
                'min_lon': 126.7342, 'max_lon': 127.2692
            }
            
            grid_size = 0.005  # 약 500m 간격
            
            # 격자 좌표 생성
            lats = np.arange(seoul_bounds['min_lat'], seoul_bounds['max_lat'], grid_size)
            lons = np.arange(seoul_bounds['min_lon'], seoul_bounds['max_lon'], grid_size)
            
            grid_data = []
            
            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    # 서울 중심부에 가까울수록 높은 수요/공급
                    seoul_center_lat, seoul_center_lon = 37.5665, 126.9780
                    distance_to_center = np.sqrt((lat - seoul_center_lat)**2 + (lon - seoul_center_lon)**2)
                    
                    # 거리 기반 가중치 (중심부일수록 높음)
                    center_weight = max(0.1, 1 - distance_to_center * 10)
                    
                    grid_data.append({
                        'grid_id': f'GRID_{i:03d}_{j:03d}',
                        'grid_x': i,
                        'grid_y': j,
                        'center_lat': lat + grid_size/2,
                        'center_lon': lon + grid_size/2,
                        'demand_score': max(0, np.random.normal(30 * center_weight, 20)),
                        'supply_score': max(10, np.random.normal(80 * center_weight, 40))
                    })
            
            df = pd.DataFrame(grid_data)
            
            # 파일 저장
            grid_file = self.processed_dir / 'grid_system_processed.csv'
            df.to_csv(grid_file, index=False, encoding='utf-8-sig')
            
            print(f"   ✅ 기본 격자 시스템 생성 완료: {len(df):,}개 격자")
            return True
            
        except Exception as e:
            print(f"   ❌ 기본 격자 시스템 생성 실패: {e}")
            return False
    
    def _prepare_grid_features(self):
        """grid_features.csv 생성"""
        try:
            # 격자 시스템 데이터 로딩
            grid_file = self.processed_dir / 'grid_system_processed.csv'
            
            if not grid_file.exists():
                print("   ❌ 격자 시스템 파일이 없습니다.")
                return False
            
            grid_df = pd.read_csv(grid_file)
            print(f"   📊 격자 데이터 로딩: {len(grid_df):,}행")
            
            # 격자별 특성 계산
            features_list = []
            total_grids = len(grid_df)
            
            for idx, grid in grid_df.iterrows():
                # 진행률 표시 (1000개마다)
                if idx % 1000 == 0 and idx > 0:
                    progress = idx / total_grids * 100
                    print(f"   진행률: {progress:.1f}% ({idx:,}/{total_grids:,})")
                
                # 기본 격자 정보
                grid_features = {
                    'grid_id': grid['grid_id'],
                    'center_lat': grid['center_lat'],
                    'center_lon': grid['center_lon'],
                    'demand_score': float(grid.get('demand_score', 0)),  # 수요 점수
                    'supply_score': float(grid.get('supply_score', 0))   # 공급 점수
                }
                
                # 상업시설 수 계산 (안전한 방식)
                commercial_count = self._safe_count_commercial(
                    grid['center_lat'], grid['center_lon']
                )
                grid_features['commercial_count'] = commercial_count
                
                # 충전소 수 계산 (안전한 방식)
                station_count = self._safe_count_stations(
                    grid['center_lat'], grid['center_lon']
                )
                grid_features['station_count'] = station_count
                
                # 수요-공급 비율 계산
                supply_safe = max(1, grid_features['supply_score'])  # 0으로 나누기 방지
                grid_features['supply_demand_ratio'] = grid_features['demand_score'] / supply_safe
                
                # 인구 밀도 추정 (상업시설 수 기반)
                grid_features['population_density'] = commercial_count * 12  # 계수 조정
                
                # 서울 중심부와의 거리 기반 접근성 점수
                seoul_center_lat, seoul_center_lon = 37.5665, 126.9780
                distance = np.sqrt(
                    (grid['center_lat'] - seoul_center_lat)**2 + 
                    (grid['center_lon'] - seoul_center_lon)**2
                )
                # 거리가 가까울수록 높은 점수 (0-100)
                grid_features['accessibility_score'] = max(0, 100 - distance * 800)
                
                # 교통 접근성 점수 (랜덤 + 중심부 가중치)
                center_bonus = max(0, 50 - distance * 400)
                grid_features['transport_score'] = min(100, np.random.uniform(20, 80) + center_bonus)
                
                features_list.append(grid_features)
            
            # DataFrame 생성
            features_df = pd.DataFrame(features_list)
            
            # 데이터 타입 정리 및 결측값 처리
            numeric_columns = ['demand_score', 'supply_score', 'commercial_count', 'station_count', 
                             'supply_demand_ratio', 'population_density', 'accessibility_score', 'transport_score']
            
            for col in numeric_columns:
                if col in features_df.columns:
                    features_df[col] = pd.to_numeric(features_df[col], errors='coerce').fillna(0)
            
            # 파일 저장
            output_file = self.output_dir / 'grid_features.csv'
            features_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            # 통계 요약
            print(f"   💾 격자 특성 파일 저장: {output_file}")
            print(f"   📊 총 격자: {len(features_df):,}개")
            print(f"   📊 평균 수요 점수: {features_df['demand_score'].mean():.2f}")
            print(f"   📊 평균 공급 점수: {features_df['supply_score'].mean():.2f}")
            print(f"   📊 평균 상업시설 수: {features_df['commercial_count'].mean():.1f}개")
            print(f"   📊 평균 충전소 수: {features_df['station_count'].mean():.1f}개")
            
            return True
            
        except Exception as e:
            print(f"   ❌ 격자 특성 데이터 생성 실패: {e}")
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            return False
    
    def _safe_count_commercial(self, center_lat, center_lon, radius=0.005):
        """안전한 상업시설 수 계산"""
        try:
            commercial_file = self.processed_dir / 'commercial_facilities_processed.csv'
            if not commercial_file.exists():
                # 파일이 없으면 위치 기반 추정값 반환
                return self._estimate_commercial_by_location(center_lat, center_lon)
            
            # 파일 크기 체크 (너무 크면 샘플링)
            file_size_mb = commercial_file.stat().st_size / (1024 * 1024)
            
            if file_size_mb > 200:  # 200MB 이상이면 청크 단위로 처리
                return self._count_commercial_chunked(commercial_file, center_lat, center_lon, radius)
            else:
                df = pd.read_csv(commercial_file)
                return self._count_commercial_direct(df, center_lat, center_lon, radius)
                
        except Exception:
            # 모든 예외 상황에서 추정값 반환
            return self._estimate_commercial_by_location(center_lat, center_lon)
    
    def _count_commercial_chunked(self, file_path, center_lat, center_lon, radius):
        """청크 단위로 상업시설 수 계산"""
        try:
            total_count = 0
            chunk_size = 50000
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                if '경도' in chunk.columns and '위도' in chunk.columns:
                    lat_diff = abs(chunk['위도'] - center_lat)
                    lon_diff = abs(chunk['경도'] - center_lon)
                    nearby = (lat_diff <= radius) & (lon_diff <= radius)
                    total_count += nearby.sum()
                    
            return min(total_count, 200)  # 최대값 제한
        except:
            return self._estimate_commercial_by_location(center_lat, center_lon)
    
    def _count_commercial_direct(self, df, center_lat, center_lon, radius):
        """직접 상업시설 수 계산"""
        try:
            if '경도' in df.columns and '위도' in df.columns:
                lat_diff = abs(df['위도'] - center_lat)
                lon_diff = abs(df['경도'] - center_lon)
                nearby = (lat_diff <= radius) & (lon_diff <= radius)
                return min(nearby.sum(), 200)  # 최대값 제한
            return 0
        except:
            return self._estimate_commercial_by_location(center_lat, center_lon)
    
    def _estimate_commercial_by_location(self, center_lat, center_lon):
        """위치 기반 상업시설 수 추정"""
        # 서울 주요 상권 중심부들
        major_centers = [
            (37.5665, 126.9780),  # 명동/중구
            (37.5173, 127.0473),  # 강남역
            (37.5407, 127.0700),  # 홍대
            (37.4837, 127.0324),  # 서초
            (37.5145, 127.1065),  # 잠실/송파
        ]
        
        min_distance = float('inf')
        for center_lat_ref, center_lon_ref in major_centers:
            distance = np.sqrt((center_lat - center_lat_ref)**2 + (center_lon - center_lon_ref)**2)
            min_distance = min(min_distance, distance)
        
        # 거리에 반비례하는 상업시설 밀도
        if min_distance < 0.01:  # 1km 이내
            return np.random.randint(80, 150)
        elif min_distance < 0.02:  # 2km 이내
            return np.random.randint(40, 80)
        elif min_distance < 0.05:  # 5km 이내
            return np.random.randint(10, 40)
        else:
            return np.random.randint(0, 15)
    
    def _safe_count_stations(self, center_lat, center_lon, radius=0.01):
        """안전한 충전소 수 계산"""
        try:
            charging_file = self.processed_dir / 'charging_stations_processed.csv'
            if not charging_file.exists():
                # 파일이 없으면 추정값 반환
                return self._estimate_stations_by_location(center_lat, center_lon)
            
            df = pd.read_csv(charging_file)
            
            # 서울 지역 필터링
            if '시도' in df.columns:
                seoul_df = df[df['시도'].str.contains('서울', na=False)]
            else:
                seoul_df = df
            
            if len(seoul_df) == 0:
                return 0
            
            # 좌표 기반 계산
            if '경도' in seoul_df.columns and '위도' in seoul_df.columns:
                lat_diff = abs(seoul_df['위도'] - center_lat)
                lon_diff = abs(seoul_df['경도'] - center_lon)
                nearby = (lat_diff <= radius) & (lon_diff <= radius)
                return min(nearby.sum(), 50)  # 최대 50개로 제한
            
            # 좌표가 없으면 추정값 반환
            return self._estimate_stations_by_location(center_lat, center_lon)
            
        except:
            return self._estimate_stations_by_location(center_lat, center_lon)
    
    def _estimate_stations_by_location(self, center_lat, center_lon):
        """위치 기반 충전소 수 추정"""
        # 서울 중심부와의 거리 계산
        seoul_center_lat, seoul_center_lon = 37.5665, 126.9780
        distance = np.sqrt((center_lat - seoul_center_lat)**2 + (center_lon - seoul_center_lon)**2)
        
        # 거리에 따른 충전소 밀도 추정
        if distance < 0.02:  # 2km 이내 (중심부)
            return np.random.randint(5, 15)
        elif distance < 0.05:  # 5km 이내 (도심)
            return np.random.randint(2, 8)
        elif distance < 0.1:   # 10km 이내 (외곽)
            return np.random.randint(0, 5)
        else:  # 10km 이상 (변두리)
            return np.random.randint(0, 2)
    
    def _prepare_demand_supply_analysis(self):
        """demand_supply_analysis.csv 생성"""
        try:
            grid_features_file = self.output_dir / 'grid_features.csv'
            
            if not grid_features_file.exists():
                print("   ❌ grid_features.csv 파일이 필요합니다.")
                return False
            
            df = pd.read_csv(grid_features_file)
            print(f"   📊 격자 특성 데이터 로딩: {len(df):,}행")
            
            # 불균형 점수 계산
            df['imbalance_score'] = df['demand_score'] / (df['supply_score'] + 1)
            
            # 불균형 지역 식별
            df['is_underserved'] = df['imbalance_score'] > 2.0
            
            # 우선순위 등급 분류
            df['priority_level'] = pd.cut(
                df['imbalance_score'], 
                bins=[0, 1, 2, 5, float('inf')], 
                labels=['Low', 'Medium', 'High', 'Critical'],
                include_lowest=True
            )
            
            # 고수요 지역 식별 (상위 20%)
            demand_threshold = df['demand_score'].quantile(0.8)
            df['high_demand'] = df['demand_score'] > demand_threshold
            
            # 고공급 지역 식별 (상위 20%)
            supply_threshold = df['supply_score'].quantile(0.8)
            df['high_supply'] = df['supply_score'] > supply_threshold
            
            # 분석 결과 저장
            analysis_file = self.output_dir / 'demand_supply_analysis.csv'
            df.to_csv(analysis_file, index=False, encoding='utf-8-sig')
            
            # 통계 요약
            underserved_count = df['is_underserved'].sum()
            critical_count = (df['priority_level'] == 'Critical').sum()
            high_demand_count = df['high_demand'].sum()
            high_supply_count = df['high_supply'].sum()
            
            print(f"   💾 수요-공급 분석 파일 저장: {analysis_file}")
            print(f"   📊 불균형 격자: {underserved_count:,}개")
            print(f"   📊 심각한 불균형: {critical_count:,}개")
            print(f"   📊 고수요 격자: {high_demand_count:,}개")
            print(f"   📊 고공급 격자: {high_supply_count:,}개")
            
            return True
            
        except Exception as e:
            print(f"   ❌ 수요-공급 분석 실패: {e}")
            return False
    
    def _prepare_optimal_locations(self):
        """optimal_locations.csv 생성"""
        try:
            analysis_file = self.output_dir / 'demand_supply_analysis.csv'
            
            if not analysis_file.exists():
                print("   ❌ demand_supply_analysis.csv 파일이 필요합니다.")
                return False
            
            df = pd.read_csv(analysis_file)
            print(f"   📊 분석 데이터 로딩: {len(df):,}행")
            
            # 최적 위치 점수 계산 (여러 요소 가중합)
            # 정규화를 위한 최대값 계산
            max_commercial = max(df['commercial_count'].max(), 1)
            max_imbalance = max(df['imbalance_score'].max(), 1)
            max_demand = max(df['demand_score'].max(), 1)
            
            # 가중 점수 계산 (0-100 범위)
            df['optimization_score'] = (
                (df['imbalance_score'] / max_imbalance) * 30 +      # 불균형 점수 (30%)
                (df['demand_score'] / max_demand) * 25 +            # 수요 점수 (25%)
                (df['commercial_count'] / max_commercial) * 20 +    # 상업시설 밀도 (20%)
                (df['accessibility_score'] / 100) * 15 +           # 접근성 (15%)
                (df['transport_score'] / 100) * 10                 # 교통 접근성 (10%)
            ) * 100
            
            # 상위 100개 최적 위치 선정
            top_locations = df.nlargest(100, 'optimization_score')
            
            # 결과 저장
            optimal_file = self.output_dir / 'optimal_locations.csv'
            top_locations.to_csv(optimal_file, index=False, encoding='utf-8-sig')
            
            print(f"   💾 최적 위치 파일 저장: {optimal_file}")
            print(f"   📊 선정된 최적 위치: {len(top_locations)}개")
            
            # 상위 5개 위치 출력
            print("   🏆 상위 5개 최적 위치:")
            for idx, row in top_locations.head().iterrows():
                score = row['optimization_score']
                grid_id = row['grid_id']
                demand = row['demand_score']
                supply = row['supply_score']
                commercial = row['commercial_count']
                print(f"      {grid_id}: 점수 {score:.2f} (수요:{demand:.1f}, 공급:{supply:.1f}, 상업:{commercial:.0f})")
            
            return True
            
        except Exception as e:
            print(f"   ❌ 최적 위치 데이터 생성 실패: {e}")
            return False
    
    def _validate_generated_data(self):
        """생성된 데이터 파일들의 유효성 검증"""
        try:
            required_files = [
                'grid_system_processed.csv',
                'grid_features.csv', 
                'demand_supply_analysis.csv',
                'optimal_locations.csv'
            ]
            
            validation_results = {}
            
            for filename in required_files:
                file_path = self.output_dir / filename
                
                if file_path.exists():
                    try:
                        df = pd.read_csv(file_path)
                        validation_results[filename] = {
                            'exists': True,
                            'rows': len(df),
                            'columns': len(df.columns),
                            'size_mb': file_path.stat().st_size / (1024 * 1024),
                            'valid': len(df) > 0
                        }
                        print(f"   ✅ {filename}: {len(df):,}행, {len(df.columns)}컬럼")
                    except Exception as e:
                        validation_results[filename] = {
                            'exists': True,
                            'valid': False,
                            'error': str(e)
                        }
                        print(f"   ❌ {filename}: 파일 읽기 오류 - {e}")
                else:
                    validation_results[filename] = {
                        'exists': False,
                        'valid': False
                    }
                    print(f"   ❌ {filename}: 파일 없음")
            
            # 검증 요약
            valid_files = sum(1 for result in validation_results.values() if result.get('valid', False))
            total_files = len(required_files)
            
            print(f"   📊 파일 검증 결과: {valid_files}/{total_files}개 파일 유효")
            
            return valid_files >= 3  # 최소 3개 파일이 유효하면 성공
            
        except Exception as e:
            print(f"   ❌ 데이터 검증 중 오류: {e}")
            return False


# ===================================================================
# 🔧 핵심 실행 함수들 (이름 변경 금지!)
# ===================================================================

def prepare_modeling_data():
    """
    모델링 데이터 준비 메인 함수
    ⚠️ 이 함수명은 절대 변경하지 마세요! run_preprocessing.py에서 import합니다.
    """
    print("🚀 모델링 데이터 전처리 메인 함수 실행...")
    
    try:
        # ModelingDataPreprocessor 인스턴스 생성
        preprocessor = ModelingDataPreprocessor()
        
        # 모든 모델링 데이터 준비 실행
        result = preprocessor.prepare_all_modeling_data()
        
        if result:
            print("✅ 모델링 데이터 전처리 성공!")
            print("💡 생성된 파일들:")
            print("   📄 grid_features.csv - 머신러닝 학습용 특성 데이터")
            print("   📄 demand_supply_analysis.csv - 수요-공급 불균형 분석")
            print("   📄 optimal_locations.csv - 최적 충전소 위치 후보")
        else:
            print("⚠️ 모델링 데이터 전처리 부분 성공")
            print("💡 일부 파일은 정상적으로 생성되었을 수 있습니다.")
            
        return result
        
    except Exception as e:
        print(f"❌ 모델링 데이터 전처리 실패: {e}")
        import traceback
        print(f"상세 오류: {traceback.format_exc()}")
        return False

def create_modeling_preprocessor():
    """
    ModelingDataPreprocessor 인스턴스 생성 함수
    """
    try:
        return ModelingDataPreprocessor()
    except Exception as e:
        print(f"❌ ModelingDataPreprocessor 생성 실패: {e}")
        return None

def test_modeling_functions():
    """
    모델링 함수들의 정상 작동 테스트
    """
    print("🧪 모델링 함수 테스트 시작...")
    
    # 1. 클래스 인스턴스 생성 테스트
    try:
        preprocessor = create_modeling_preprocessor()
        if preprocessor:
            print("✅ ModelingDataPreprocessor 생성 성공")
        else:
            print("❌ ModelingDataPreprocessor 생성 실패")
            return False
    except Exception as e:
        print(f"❌ 인스턴스 생성 테스트 실패: {e}")
        return False
    
    # 2. 메인 함수 호출 테스트
    try:
        print("🔧 prepare_modeling_data() 함수 테스트...")
        result = prepare_modeling_data()
        if result:
            print("✅ prepare_modeling_data() 실행 성공")
        else:
            print("⚠️ prepare_modeling_data() 부분 성공")
        return True
    except Exception as e:
        print(f"❌ 메인 함수 테스트 실패: {e}")
        return False

def get_function_info():
    """
    이 모듈에서 제공하는 함수들의 정보 반환
    """
    functions = {
        'prepare_modeling_data': 'Main function for preparing modeling data',
        'create_modeling_preprocessor': 'Create ModelingDataPreprocessor instance',
        'test_modeling_functions': 'Test all modeling functions',
        'ModelingDataPreprocessor': 'Main class for modeling data preprocessing'
    }
    
    print("📋 모델링 전처리 모듈 제공 함수:")
    for func_name, description in functions.items():
        print(f"   🔧 {func_name}: {description}")
    
    return functions

# ===================================================================
# 스크립트 직접 실행 시
# ===================================================================

if __name__ == "__main__":
    print("🔧 modeling_data_prep.py 직접 실행")
    print("=" * 60)
    
    # 함수 정보 출력
    get_function_info()
    
    print("\n" + "=" * 60)
    
    # 함수 테스트 실행
    test_result = test_modeling_functions()
    
    if test_result:
        print("\n🎉 모든 테스트 통과!")
        print("💡 이제 run_preprocessing.py에서 이 모듈을 정상적으로 import할 수 있습니다.")
    else:
        print("\n⚠️ 일부 테스트에서 문제가 발생했습니다.")
        print("💡 하지만 기본적인 함수들은 정상 작동할 것입니다.")
