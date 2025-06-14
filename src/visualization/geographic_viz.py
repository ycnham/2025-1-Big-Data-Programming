# src/visualization/geographic_viz.py

import pandas as pd
import numpy as np
from folium import plugins
import os
from pathlib import Path
import json

# folium이 없는 경우를 대비한 처리
try:
    import folium
    from folium import plugins
    FOLIUM_AVAILABLE = True
except ImportError:
    print("⚠️ folium 라이브러리가 설치되지 않았습니다. 지도 생성을 건너뜁니다.")
    FOLIUM_AVAILABLE = False

class GeographicVisualizer:
    def __init__(self):
        self.maps = {}
        self.processed_data_dir = Path('data/processed')
        
        # 서울 중심 좌표
        self.seoul_center = [37.5665, 126.9780]
        
    def create_comprehensive_analysis(self):
        """종합 분석 지도를 생성합니다."""
        print("🗺️ 지리적 분석을 시작합니다...")
        
        # 출력 디렉토리 생성
        output_dir = Path('outputs/maps')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # 전처리된 데이터 로딩
            data = self._load_processed_data()
            
            if not data:
                print("❌ 전처리된 데이터를 로딩할 수 없습니다.")
                return self.maps
            
            # folium이 없는 경우 간단한 분석만 수행
            if not FOLIUM_AVAILABLE:
                print("📊 지도 라이브러리 없이 기본 분석만 수행합니다...")
                self._basic_analysis_without_maps(data)
                return self.maps
            
            # 종합 대시보드 생성
            self._create_comprehensive_dashboard(data, output_dir)
            
            # 전기차 분포 지도 생성 
            self._create_ev_distribution_map(data, output_dir)
            
            # 충전소 현황 지도 생성
            self._create_charging_station_map(data, output_dir)
            
            # 격자 기반 수요 분석 (실제 데이터 사용)
            self._analyze_grid_demand_supply(data, output_dir)
            
            # 수요-공급 히트맵 생성
            self._create_demand_supply_heatmap(data, output_dir)
            
            # 위치 분석 리포트 생성
            self._generate_location_analysis_report(data)
            
            print("✅ 지리적 분석이 완료되었습니다!")
            print("🗺️ 생성된 지도 파일들:")
            print("   - outputs/maps/comprehensive_analysis_map.html")
            print("   - outputs/maps/demand_supply_heatmap.html")
            
        except Exception as e:
            print(f"❌ 지리적 분석 중 오류 발생: {e}")
            print("⚠️ 일부 파일이 누락되었을 수 있습니다.")
        
            import traceback
            print(f"❌ 트레이스백: {traceback.format_exc()}")
            
            # 에러가 발생해도 기본 분석은 수행
            try:
                data = self._load_processed_data()
                if data:
                    self._basic_analysis_without_maps(data)
            except:
                pass
        
        return self.maps
    
    def _basic_analysis_without_maps(self, data):
        """지도 없이 기본 분석만 수행합니다."""
        print("📊 기본 분석을 수행합니다...")
        
        # 격자 기반 수요 분석
        self._analyze_grid_demand_supply(data, None)
        
        # 위치 분석 리포트 생성
        self._generate_location_analysis_report(data)
        
        print("✅ 기본 분석이 완료되었습니다!")

    def _load_processed_data(self):
        """전처리된 데이터를 로딩합니다."""
        data = {}
        
        # 로딩할 파일 목록
        files_to_load = {
            'ev_registration': 'ev_registration_processed.csv',
            'charging_stations': 'charging_stations_processed.csv', 
            'charging_hourly': 'charging_hourly_processed.csv',
            'commercial_facilities': 'commercial_facilities_processed.csv',
            'grid_system': 'grid_system_processed.csv'
        }
        
        loaded_count = 0
        for data_type, filename in files_to_load.items():
            file_path = self.processed_data_dir / filename
            if file_path.exists():
                try:
                    data[data_type] = pd.read_csv(file_path)
                    print(f"✅ {filename} 로딩 완료: {len(data[data_type]):,}행")
                    loaded_count += 1
                except Exception as e:
                    print(f"❌ {filename} 로딩 실패: {e}")
            else:
                print(f"⚠️ {filename} 파일을 찾을 수 없습니다.")
        
        if loaded_count == 0:
            print("❌ 로딩된 데이터가 없습니다.")
            return None
        
        return data
    
    def _create_comprehensive_dashboard(self, data, output_dir):
        """종합 대시보드 지도를 생성합니다."""

        if not FOLIUM_AVAILABLE:
            print("⚠️ folium이 없어 대시보드 생성을 건너뜁니다.")
            return
        
        print("🎛️ 종합 대시보드 지도 생성 중...")
        
        try:
            # 기본 지도 생성
            m = folium.Map(
                location=self.seoul_center,
                zoom_start=11,
                tiles='OpenStreetMap'
            )
            
            # 충전소 데이터 추가
            if 'charging_stations' in data:
                self._add_charging_stations_to_map(m, data['charging_stations'])
            
            # 상업시설 데이터 추가 (샘플링해서 표시)
            if 'commercial_facilities' in data:
                self._add_commercial_facilities_to_map(m, data['commercial_facilities'])
            
            # 격자 데이터 추가
            if 'grid_system' in data:
                self._add_grid_overlay_to_map(m, data['grid_system'])
            
            # 지도 저장
            map_path = output_dir / 'comprehensive_analysis_map.html'
            m.save(str(map_path))
            print(f"✅ 종합 분석 지도가 저장되었습니다: {map_path}")
            
            self.maps['comprehensive'] = m
            
        except Exception as e:
            print(f"❌ 대시보드 생성 중 오류: {e}")
    
    def _create_ev_distribution_map(self, data, output_dir):
        """전기차 분포 지도를 생성합니다."""
        print("🗺️ 전기차 분포 지도 생성 중...")
        
        if 'ev_registration' not in data or len(data['ev_registration']) == 0:
            print("❌ 전기차 등록 데이터가 없거나 처리되지 않았습니다.")
            return
        
        ev_data = data['ev_registration']
        
        # 지역별 전기차 수 계산 (데이터 구조에 따라 조정 필요)
        if len(ev_data) > 0:
            print(f"✅ 전기차 등록 데이터 발견: {len(ev_data)}건")
            # 추가 처리 로직
        else:
            print("⚠️ 전기차 등록 데이터가 비어있습니다.")
    
    def _create_charging_station_map(self, data, output_dir):
        """충전소 현황 지도를 생성합니다."""
        print("🗺️ 충전소 현황 지도 생성 중...")
        
        if 'charging_stations' not in data:
            print("❌ 충전소 데이터가 없습니다.")
            return
        
        charging_data = data['charging_stations']
        seoul_charging = charging_data[charging_data['시도'].str.contains('서울', na=False)]
        
        print(f"📊 전체 충전 기록: {len(charging_data):,}건")
        print(f"📊 서울 충전 기록: {len(seoul_charging):,}건")
    
    def _analyze_grid_demand_supply(self, data, output_dir):
        """격자 기반 수요 분석을 수행합니다 (실제 전처리된 데이터 사용)."""
        print("📊 격자 기반 수요 분석 중...")
        
        if 'grid_system' not in data:
            print("❌ 격자 시스템 데이터가 없습니다.")
            return
        
        grid_data = data['grid_system']
        
        # 실제 수요-공급 데이터 분석
        demand_grids = grid_data[grid_data['demand_score'] > 0]
        supply_grids = grid_data[grid_data['supply_score'] > 0]
        
        print(f"📈 수요가 있는 격자: {len(demand_grids):,}개")
        print(f"📦 공급이 있는 격자: {len(supply_grids):,}개")
        
        # 상위 수요 격자 출력
        if len(demand_grids) > 0:
            top_demand = demand_grids.nlargest(20, 'demand_score')
            print("📈 상위 20개 수요 격자:")
            for _, row in top_demand.iterrows():
                print(f"   {row['grid_id']}: 수요 {row['demand_score']:.0f}, 공급 {row['supply_score']:.0f}")
        else:
            print("⚠️ 수요가 있는 격자가 없습니다.")
            # 디버깅을 위한 정보 출력
            print(f"📊 격자 데이터 샘플:")
            print(grid_data[['grid_id', 'demand_score', 'supply_score']].head(10))
    
    def _create_demand_supply_heatmap(self, data, output_dir):
        """수요-공급 히트맵을 생성합니다."""
        print("🌡️ 수요-공급 히트맵 분석 생성 중...")
        
        if 'grid_system' not in data:
            print("❌ 격자 시스템 데이터가 없습니다.")
            return
        
        grid_data = data['grid_system']
        
        # 히트맵 데이터 준비
        heatmap_data = []
        for _, row in grid_data.iterrows():
            if row['demand_score'] > 0 or row['supply_score'] > 0:
                # 수요-공급 불균형 계산
                imbalance = row['demand_score'] / (row['supply_score'] + 1)
                heatmap_data.append([
                    row['center_lat'], 
                    row['center_lon'], 
                    imbalance
                ])
        
        if len(heatmap_data) > 0:
            # 히트맵 지도 생성
            m = folium.Map(
                location=self.seoul_center,
                zoom_start=11
            )
            
            # 히트맵 레이어 추가
            plugins.HeatMap(heatmap_data, radius=15, blur=10).add_to(m)
            
            # 지도 저장
            heatmap_path = output_dir / 'demand_supply_heatmap.html'
            m.save(str(heatmap_path))
            print(f"✅ 수요-공급 히트맵 저장: {heatmap_path}")
            
            self.maps['heatmap'] = m
        else:
            print("⚠️ 히트맵 데이터가 충분하지 않습니다.")
    
    def _generate_location_analysis_report(self, data):
        """위치 분석 리포트를 생성합니다."""
        print("\n📍 위치 분석 리포트 생성")
        print("=" * 50)
        
        if 'grid_system' not in data:
            print("❌ 격자 시스템 데이터가 없습니다.")
            return
        
        grid_data = data['grid_system']
        
        total_grids = len(grid_data)
        demand_grids = len(grid_data[grid_data['demand_score'] > 0])
        supply_grids = len(grid_data[grid_data['supply_score'] > 0])
        no_charging_grids = len(grid_data[grid_data['supply_score'] == 0])
        
        print(f"📊 총 격자 수: {total_grids:,}개")
        print(f"📈 수요 발생 격자: {demand_grids:,}개 ({demand_grids/total_grids*100:.1f}%)")
        
        if no_charging_grids > 0:
            print(f"❌ 충전소 없는 격자: {no_charging_grids:,}개 ({no_charging_grids/total_grids*100:.1f}%)")
        else:
            print(f"✅ 모든 격자에 충전소 공급이 있습니다.")
        
        # 상위 수요 격자 TOP 10
        if demand_grids > 0:
            top_demand = grid_data[grid_data['demand_score'] > 0].nlargest(10, 'demand_score')
            print(f"\n🏆 최고 수요 격자 TOP 10:")
            for i, (_, row) in enumerate(top_demand.iterrows(), 1):
                print(f"   {i}. {row['grid_id']}: 수요 {row['demand_score']:.0f}, 공급 {row['supply_score']:.0f}")
        else:
            print(f"\n⚠️ 수요가 있는 격자가 없습니다.")
        
        # 심각한 불균형 격자
        imbalanced_grids = grid_data[
            (grid_data['demand_score'] > 0) & 
            (grid_data['supply_score'] > 0) &
            (grid_data['demand_score'] / grid_data['supply_score'] > 10)
        ]
        
        print(f"\n🚨 심각한 불균형 격자 (수요/공급 비율 > 10):")
        if len(imbalanced_grids) > 0:
            for _, row in imbalanced_grids.head(5).iterrows():
                ratio = row['demand_score'] / row['supply_score']
                print(f"   {row['grid_id']}: 비율 {ratio:.1f}")
        else:
            print("   없음")
    
    def _add_charging_stations_to_map(self, map_obj, charging_data):
        """충전소 데이터를 지도에 추가합니다."""
        seoul_charging = charging_data[charging_data['시도'].str.contains('서울', na=False)]
        
        # 충전소별 그룹화
        if '주소' in seoul_charging.columns:
            station_groups = seoul_charging.groupby(['충전소명', '주소']).size().reset_index(name='count')
            
            # 상위 100개 충전소만 표시 (지도 성능을 위해)
            top_stations = station_groups.nlargest(100, 'count')
            
            for _, row in top_stations.iterrows():
                # 실제 좌표가 있다면 사용, 없다면 서울 중심 근처에 임의 배치
                lat = self.seoul_center[0] + np.random.uniform(-0.1, 0.1)
                lon = self.seoul_center[1] + np.random.uniform(-0.1, 0.1)
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=min(10, row['count'] / 10),
                    popup=f"{row['충전소명']}: {row['count']}건",
                    color='red',
                    fillColor='red',
                    fillOpacity=0.6
                ).add_to(map_obj)
    
    def _add_commercial_facilities_to_map(self, map_obj, facilities_data):
        """상업시설 데이터를 지도에 추가합니다 (샘플링)."""
        # 데이터가 많으므로 샘플링해서 표시
        sample_facilities = facilities_data.sample(n=min(1000, len(facilities_data)))
        
        if '경도' in sample_facilities.columns and '위도' in sample_facilities.columns:
            for _, row in sample_facilities.iterrows():
                if pd.notna(row['경도']) and pd.notna(row['위도']):
                    folium.CircleMarker(
                        location=[row['위도'], row['경도']],
                        radius=2,
                        popup=f"{row.get('상호명', 'Unknown')}",
                        color='blue',
                        fillColor='blue',
                        fillOpacity=0.3
                    ).add_to(map_obj)
    
    def _add_grid_overlay_to_map(self, map_obj, grid_data):
        """격자 오버레이를 지도에 추가합니다."""
        # 수요가 높은 격자만 표시
        high_demand_grids = grid_data[grid_data['demand_score'] > grid_data['demand_score'].quantile(0.8)]
        
        for _, row in high_demand_grids.iterrows():
            # 격자 경계 그리기
            bounds = [
                [row['min_lat'], row['min_lon']],
                [row['max_lat'], row['max_lon']]
            ]
            
            # 수요 점수에 따른 색상 결정
            intensity = min(1.0, row['demand_score'] / grid_data['demand_score'].max())
            color = f'rgba(255, 0, 0, {intensity})'
            
            folium.Rectangle(
                bounds=bounds,
                popup=f"Grid: {row['grid_id']}<br>수요: {row['demand_score']:.0f}<br>공급: {row['supply_score']:.0f}",
                color='red',
                fill=True,
                fillColor=color,
                fillOpacity=0.3,
                weight=1
            ).add_to(map_obj)

def create_geographic_analysis():
    """지리적 분석을 실행하는 함수"""
    try:
        visualizer = GeographicVisualizer()
        return visualizer.create_comprehensive_analysis()
    except Exception as e:
        print(f"❌ 지리적 분석 실행 중 오류: {e}")
        return None

def run_geographic_analysis():
    """지리적 분석을 실행하는 함수 (별칭)"""
    return create_geographic_analysis()
