# src/preprocessing/data_loader.py

import pandas as pd
import numpy as np
import os
from pathlib import Path
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, data_dir='data/raw'):
        self.data_dir = Path(data_dir)
        self.datasets = {}
        
    def load_all_datasets(self):
        """모든 데이터셋을 로딩합니다."""
        print("🚀 모든 데이터 전처리를 시작합니다...")
        print("=" * 60)
        print("🚀 데이터 로딩을 시작합니다...")
        print()
        
        # 로딩할 데이터셋 정의
        dataset_configs = {
            'charging_load_hourly': {
                'file': '서울시 소유 충전기 일별 시간별 충전현황.xlsx',
                'description': '서울시 소유 충전기 시간별 충전현황'
            },
            'ev_registration_monthly': {
                'file': '서울시 자치구 읍면동별 연료별 자동차 등록현황(행정동)(25년04월).xls',
                'description': '서울시 자치구별 전기차 등록현황 (2025년 4월)'
            },
            'commercial_facilities': {
                'file': '소상공인시장진흥공단_상가(상권)정보_서울_202503.csv',
                'description': '서울시 상가(상권) 정보 (2025년 3월)'
            },
            'charging_stations_202501': {
                'file': '전기차 충전소 충전량 데이터_202501.xlsx',
                'description': '전기차 충전소 충전량 데이터 (2025년 1월)'
            },
            'charging_stations_202502': {
                'file': '전기차 충전소 충전량 데이터_202502.xlsx',
                'description': '전기차 충전소 충전량 데이터 (2025년 2월)'
            },
            'charging_stations_202503': {
                'file': '전기차 충전소 충전량 데이터_202503.xlsx',
                'description': '전기차 충전소 충전량 데이터 (2025년 3월)'
            }
        }
        
        # 선택적으로 로딩할 데이터셋
        optional_datasets = {
            'ev_charging_service': {
                'file': '(참고자료) 한국전력공사_전기차충전서비스운영시스템_고객센터 상담내역_코드표.xlsx',
                'description': '전기차 충전서비스 코드표 (참고자료)'
            },
            'public_parking': {
                'file': '월별 소통정보 (구간별-첨두시별).csv',
                'description': '월별 교통소통 정보'
            },
            'gov_charging_service': {
                'file': '한국전력공사_전기차충전서비스운영시스템_고객센터 상담 내역_20241231.csv',
                'description': '한국전력공사 전기차 충전서비스 상담내역'
            },
            'charging_facility_management': {
                'file': '한국환경공단_전기차 충전소 위치 및 운영정보(충전소 ID 포함)_20230531.csv',
                'description': '한국환경공단 전기차 충전소 위치 및 운영 정보'
            }
        }
        
        # 필수 데이터셋 로딩
        for dataset_name, config in dataset_configs.items():
            self._load_dataset(dataset_name, config, required=True)
        
        # 선택적 데이터셋 로딩
        for dataset_name, config in optional_datasets.items():
            self._load_dataset(dataset_name, config, required=False)
        
        self._print_loading_summary()
        return self.datasets
    
    def _load_dataset(self, dataset_name, config, required=True):
        """개별 데이터셋을 로딩합니다."""
        print(f"🔄 로딩 중: {dataset_name}")
        print()
        print("=" * 60)
        print(f"📊 {config['description']}")
        print("=" * 60)
        
        file_path = self.data_dir / config['file']
        
        if not file_path.exists():
            if required:
                print(f"❌ 필수 데이터 파일이 없습니다: {file_path}")
                print(f"❌ {dataset_name} 로딩 실패")
            else:
                print(f"⚠️ 선택적 데이터 파일이 없습니다: {file_path}")
                print(f"⚠️ {dataset_name} 건너뜀 (선택적 파일)")
            print()
            return None
        
        try:
            # 파일 확장자에 따른 로딩
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif file_path.suffix.lower() == '.csv':
                # 인코딩 문제 해결을 위한 다중 시도
                df = self._load_csv_with_encoding(file_path)
            else:
                print(f"❌ 지원하지 않는 파일 형식: {file_path.suffix}")
                print(f"❌ {dataset_name} 로딩 실패")
                print()
                return None
            
            if df is None:
                print(f"❌ {dataset_name} 로딩 실패")
                print()
                return None
            
            print(f"📁 파일명: {file_path}")
            print(f"📏 데이터 크기: {df.shape[0]:,}행 × {df.shape[1]}열")
            print()
            print("🔍 컬럼 정보:")
            print(df.info())
            print()
            print("📋 첫 5행 데이터:")
            print(df.head())
            print()
            print("🔢 기본 통계:")
            print(df.describe())
            
            self.datasets[dataset_name] = df
            print(f"✅ {dataset_name} 로딩 성공")
            
        except Exception as e:
            print(f"❌ 데이터 로딩 중 오류 발생: {e}")
            print(f"❌ {dataset_name} 로딩 실패")
        
        print()
    
    def _load_csv_with_encoding(self, file_path):
        """다양한 인코딩으로 CSV 파일을 로딩 시도합니다."""
        encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig', 'latin1']
        
        for encoding in encodings:
            try:
                print(f"🔄 인코딩 시도: {encoding}")
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"✅ 인코딩 성공: {encoding}")
                return df
            except UnicodeDecodeError:
                print(f"❌ 인코딩 실패: {encoding}")
                continue
            except Exception as e:
                print(f"❌ 기타 오류 ({encoding}): {e}")
                continue
        
        print("❌ 모든 인코딩 시도 실패")
        return None
    
    def _print_loading_summary(self):
        """로딩 결과 요약을 출력합니다."""
        print("=" * 60)
        print("📋 데이터 로딩 결과 요약")
        print("=" * 60)
        
        total_datasets = 10  # 전체 시도한 데이터셋 수
        successful = len(self.datasets)
        failed = total_datasets - successful
        
        print(f"총 데이터셋: {total_datasets}개")
        print(f"로딩 성공: {successful}개")
        print(f"로딩 실패: {failed}개")
        print()
        
        if self.datasets:
            print("✅ 성공적으로 로딩된 데이터셋:")
            for name, df in self.datasets.items():
                print(f"   • {name}: {df.shape[0]:,}행 × {df.shape[1]}열")
        print()
        
        if failed > 0:
            failed_datasets = []
            all_possible = [
                'ev_charging_service', 'charging_load_hourly', 'ev_registration_monthly',
                'commercial_facilities', 'public_parking', 'charging_stations_202501',
                'charging_stations_202502', 'charging_stations_202503', 
                'gov_charging_service', 'charging_facility_management'
            ]
            failed_datasets = [name for name in all_possible if name not in self.datasets]
            
            if failed_datasets:
                print("❌ 로딩 실패한 데이터셋:")
                for name in failed_datasets:
                    print(f"   • {name}")
        print()
        print("✅ 모든 데이터 로딩 프로세스 완료!")
    
    def get_dataset(self, dataset_name):
        """특정 데이터셋을 반환합니다."""
        return self.datasets.get(dataset_name, None)
    
    def get_all_datasets(self):
        """모든 로딩된 데이터셋을 반환합니다."""
        return self.datasets

# 외부에서 호출할 수 있는 함수들
def load_all_datasets():
    """모든 데이터셋을 로딩하는 함수"""
    loader = DataLoader()
    return loader.load_all_datasets()

def create_data_loader():
    """DataLoader 인스턴스를 생성하는 함수"""
    return DataLoader()