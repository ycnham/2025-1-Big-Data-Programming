# src/analysis/eda.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from pathlib import Path
import warnings
import platform
warnings.filterwarnings('ignore')

# 한글 폰트 설정
import platform

def set_korean_font():
    system = platform.system()
    if system == 'Windows':
        try:
            plt.rcParams['font.family'] = 'Malgun Gothic'
        except:
            plt.rcParams['font.family'] = 'DejaVu Sans'
    elif system == 'Darwin':  # Mac
        try:
            plt.rcParams['font.family'] = 'AppleGothic'
        except:
            plt.rcParams['font.family'] = 'DejaVu Sans'
    else:  # Linux
        plt.rcParams['font.family'] = 'DejaVu Sans'
    
    plt.rcParams['axes.unicode_minus'] = False

# 폰트 설정 적용
set_korean_font()

class EDAAnalyzer:
    def __init__(self, processed_data_dir='data/processed'):
        self.processed_dir = Path(processed_data_dir)
        self.data = {}
        self.figures = []
    
    def load_processed_data(self):
        """전처리된 데이터를 로딩합니다."""
        print("📊 전처리된 데이터 로딩 중...")
        
        # 로딩할 파일 목록
        files_to_load = {
            'ev_registration': 'ev_registration_processed.csv',
            'charging_stations': 'charging_stations_processed.csv',
            'charging_hourly': 'charging_hourly_processed.csv',
            'commercial_facilities': 'commercial_facilities_processed.csv',
            'grid_system': 'grid_system_processed.csv'
        }
        
        for data_type, filename in files_to_load.items():
            file_path = self.processed_dir / filename
            if file_path.exists():
                try:
                    self.data[data_type] = pd.read_csv(file_path)
                    print(f"✅ {filename} 로딩 완료: {len(self.data[data_type]):,}행")
                except Exception as e:
                    print(f"❌ {filename} 로딩 실패: {e}")
            else:
                print(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")
        
        return self.data
    
    def run_comprehensive_eda(self):
        """종합적인 탐색적 데이터 분석을 수행합니다."""
        print("🚀 탐색적 데이터 분석을 시작합니다...")
        
        # 데이터 로딩
        self.load_processed_data()
        
        if not self.data:
            print("❌ 로딩된 데이터가 없습니다. 전처리 단계를 먼저 실행해주세요.")
            return
        
        # 출력 디렉토리 생성
        output_dir = Path('outputs/eda')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 각 데이터셋별 EDA 수행
        if 'charging_stations' in self.data:
            self._analyze_charging_stations(output_dir)
        
        if 'commercial_facilities' in self.data:
            self._analyze_commercial_facilities(output_dir)
        
        if 'ev_registration' in self.data:
            self._analyze_ev_registration(output_dir)
        
        if 'charging_hourly' in self.data:
            self._analyze_charging_hourly(output_dir)
        
        if 'grid_system' in self.data:
            self._analyze_grid_system(output_dir)
        
        # 종합 분석
        self._comprehensive_analysis(output_dir)
        
        print("✅ EDA 완료")
        return self.data
    
    def _analyze_charging_stations(self, output_dir):
        """충전소 데이터 분석"""
        print("🔌 충전소 데이터 분석 중...")
        
        df = self.data['charging_stations']
        
        # 기본 통계
        print(f"📊 총 충전 기록: {len(df):,}건")
        
        if '시도' in df.columns:
            sido_counts = df['시도'].value_counts()
            print(f"📍 시도별 충전 기록 수 (상위 5개):")
            for sido, count in sido_counts.head().items():
                print(f"   {sido}: {count:,}건")
        
        if '충전구분' in df.columns:
            charging_type = df['충전구분'].value_counts()
            print(f"⚡ 충전 방식별 분포:")
            for ctype, count in charging_type.items():
                print(f"   {ctype}: {count:,}건")
        
        # 충전량 분석
        if '충전량_numeric' in df.columns:
            charging_amounts = df['충전량_numeric'].dropna()
            if len(charging_amounts) > 0:
                print(f"🔋 충전량 통계:")
                print(f"   평균: {charging_amounts.mean():.2f}kW")
                print(f"   중앙값: {charging_amounts.median():.2f}kW")
                print(f"   최대값: {charging_amounts.max():.2f}kW")
                print(f"   표준편차: {charging_amounts.std():.2f}kW")
        
        # 서울 지역 분석
        if '시도' in df.columns:
            seoul_data = df[df['시도'].str.contains('서울', na=False)]
            if len(seoul_data) > 0:
                print(f"🏙️ 서울 지역 충전 기록: {len(seoul_data):,}건")
                
                if '시군구' in seoul_data.columns:
                    seoul_districts = seoul_data['시군구'].value_counts()
                    print(f"🗺️ 서울 구별 충전 기록 (상위 5개):")
                    for district, count in seoul_districts.head().items():
                        print(f"   {district}: {count:,}건")
        
        # 시각화 생성
        try:
            self._create_charging_visualizations(df, output_dir)
        except Exception as e:
            print(f"⚠️ 충전소 시각화 생성 중 오류: {e}")
    
    def _analyze_commercial_facilities(self, output_dir):
        """상업시설 데이터 분석"""
        print("🏪 상업시설 데이터 분석 중...")
        
        df = self.data['commercial_facilities']
        
        print(f"📊 총 상업시설: {len(df):,}개")
        
        # 업종 분석
        business_col = None
        for col in ['업종_대분류', '상권업종대분류명']:
            if col in df.columns:
                business_col = col
                break
        
        if business_col:
            business_types = df[business_col].value_counts()
            print(f"🏢 업종별 분포 (상위 5개):")
            for btype, count in business_types.head().items():
                print(f"   {btype}: {count:,}개")
        
        # 지역별 분포
        if '시군구명' in df.columns:
            district_counts = df['시군구명'].value_counts()
            print(f"📍 구별 분포 (상위 5개):")
            for district, count in district_counts.head().items():
                print(f"   {district}: {count:,}개")
        
        # 좌표 유효성 분석
        if '경도' in df.columns and '위도' in df.columns:
            valid_coords = (
                (df['경도'].between(126.7, 127.2)) & 
                (df['위도'].between(37.4, 37.7))
            )
            print(f"📍 서울 지역 좌표 유효율: {valid_coords.mean()*100:.1f}%")
    
    def _analyze_ev_registration(self, output_dir):
        """전기차 등록 데이터 분석"""
        print("🚗 전기차 등록 데이터 분석 중...")
        
        df = self.data['ev_registration']
        
        print(f"📊 총 데이터 행: {len(df):,}개")
        print(f"📋 컬럼 수: {len(df.columns)}개")
        
        # 지역 정보 분석
        region_columns = [col for col in df.columns if any(x in str(col) for x in ['구', '시', '동', '지역'])]
        if region_columns:
            print(f"🗺️ 지역 관련 컬럼: {region_columns[:3]}")
            
            # 첫 번째 지역 컬럼의 분포 분석
            first_region_col = region_columns[0]
            if not df[first_region_col].isna().all():
                region_dist = df[first_region_col].value_counts()
                print(f"📍 {first_region_col} 분포 (상위 5개):")
                for region, count in region_dist.head().items():
                    print(f"   {region}: {count:,}개")
        
        # 전기차 관련 컬럼 찾기
        ev_columns = [col for col in df.columns if any(x in str(col).lower() for x in ['전기', 'ev', '전동', 'electric'])]
        if ev_columns:
            print(f"⚡ 전기차 관련 컬럼: {ev_columns}")
    
    def _analyze_charging_hourly(self, output_dir):
        """시간별 충전 데이터 분석"""
        print("⏰ 시간별 충전 데이터 분석 중...")
        
        df = self.data['charging_hourly']
        
        print(f"📊 총 데이터 행: {len(df):,}개")
        
        if '충전소명' in df.columns:
            station_counts = df['충전소명'].value_counts()
            print(f"🔌 충전소별 이용 기록 (상위 3개):")
            for station, count in station_counts.head(3).items():
                print(f"   {station}: {count}건")
        
        # 충전량 분석
        charging_col = None
        for col in df.columns:
            if '충전량' in str(col) or 'kW' in str(col):
                charging_col = col
                break
        
        if charging_col:
            try:
                charging_values = pd.to_numeric(df[charging_col], errors='coerce').dropna()
                if len(charging_values) > 0:
                    print(f"🔋 {charging_col} 통계:")
                    print(f"   평균: {charging_values.mean():.2f}")
                    print(f"   최대값: {charging_values.max():.2f}")
                    print(f"   최소값: {charging_values.min():.2f}")
            except Exception as e:
                print(f"⚠️ 충전량 분석 중 오류: {e}")
    
    def _analyze_grid_system(self, output_dir):
        """격자 시스템 분석"""
        print("🗺️ 격자 시스템 분석 중...")
        
        df = self.data['grid_system']
        
        print(f"📊 총 격자 수: {len(df):,}개")
        
        # 수요-공급 분석
        if 'demand_score' in df.columns and 'supply_score' in df.columns:
            demand_grids = (df['demand_score'] > 0).sum()
            supply_grids = (df['supply_score'] > 0).sum()
            
            print(f"📈 수요가 있는 격자: {demand_grids:,}개 ({demand_grids/len(df)*100:.1f}%)")
            print(f"📦 공급이 있는 격자: {supply_grids:,}개 ({supply_grids/len(df)*100:.1f}%)")
            
            if demand_grids > 0:
                print(f"🔥 평균 수요 점수: {df[df['demand_score'] > 0]['demand_score'].mean():.2f}")
                print(f"🔥 최대 수요 점수: {df['demand_score'].max():.2f}")
            
            if supply_grids > 0:
                print(f"⚡ 평균 공급 점수: {df[df['supply_score'] > 0]['supply_score'].mean():.2f}")
                print(f"⚡ 최대 공급 점수: {df['supply_score'].max():.2f}")
            
            # 수요-공급 불균형 분석
            df['demand_supply_ratio'] = df['demand_score'] / (df['supply_score'] + 1)  # +1로 0 나누기 방지
            high_demand_low_supply = df[
                (df['demand_score'] > df['demand_score'].quantile(0.8)) & 
                (df['supply_score'] < df['supply_score'].quantile(0.2))
            ]
            
            print(f"🚨 고수요-저공급 격자: {len(high_demand_low_supply):,}개")
            
            if len(high_demand_low_supply) > 0:
                print("   상위 5개 고수요-저공급 격자:")
                top_priority = high_demand_low_supply.nlargest(5, 'demand_score')
                for idx, row in top_priority.iterrows():
                    print(f"   {row['grid_id']}: 수요 {row['demand_score']:.1f}, 공급 {row['supply_score']:.1f}")
    
    def _comprehensive_analysis(self, output_dir):
        """종합 분석"""
        print("📊 종합 분석 수행 중...")
        
        # 데이터 요약 리포트 생성
        summary_data = []
        
        for data_type, df in self.data.items():
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            summary_data.append({
                'Dataset': data_type,
                'Rows': f"{len(df):,}",
                'Columns': len(df.columns),
                'Memory_MB': f"{memory_mb:.2f}"
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        print("📋 데이터셋 요약:")
        print(summary_df.to_string(index=False))
        
        # 핵심 인사이트
        print("\n🔍 핵심 발견사항:")
        
        # 충전소 관련 인사이트
        if 'charging_stations' in self.data:
            charging_df = self.data['charging_stations']
            seoul_charging = charging_df[charging_df['시도'].str.contains('서울', na=False)]
            if len(seoul_charging) > 0:
                print(f"   • 서울 충전 기록 비율: {len(seoul_charging)/len(charging_df)*100:.1f}%")
        
        # 격자 시스템 인사이트
        if 'grid_system' in self.data:
            grid_df = self.data['grid_system']
            if 'demand_score' in grid_df.columns and 'supply_score' in grid_df.columns:
                high_demand = (grid_df['demand_score'] > grid_df['demand_score'].quantile(0.9)).sum()
                high_supply = (grid_df['supply_score'] > grid_df['supply_score'].quantile(0.9)).sum()
                print(f"   • 최고 수요 격자(상위 10%): {high_demand:,}개")
                print(f"   • 최고 공급 격자(상위 10%): {high_supply:,}개")
        
        # 상업시설 인사이트
        if 'commercial_facilities' in self.data:
            facilities_df = self.data['commercial_facilities']
            if '상권업종대분류명' in facilities_df.columns:
                food_facilities = facilities_df[facilities_df['상권업종대분류명'].str.contains('음식', na=False)]
                print(f"   • 음식점 비율: {len(food_facilities)/len(facilities_df)*100:.1f}%")
        
        # 요약 저장
        summary_path = output_dir / 'eda_summary.csv'
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 EDA 요약 저장: {summary_path}")
        
        # 종합 분석 리포트 저장
        insights_path = output_dir / 'eda_insights.txt'
        with open(insights_path, 'w', encoding='utf-8') as f:
            f.write("전기차 충전소 최적화 프로젝트 - EDA 종합 분석 리포트\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"분석 일시: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("데이터셋 요약:\n")
            f.write(summary_df.to_string(index=False))
            f.write("\n\n주요 발견사항:\n")
            # 추가 인사이트들 작성
        
        print(f"📝 인사이트 리포트 저장: {insights_path}")
    
    def _create_charging_visualizations(self, df, output_dir):
        """충전소 관련 시각화 생성"""
        try:
            # 1. 충전 방식별 분포
            if '충전구분' in df.columns:
                plt.figure(figsize=(10, 6))
                charging_type_counts = df['충전구분'].value_counts()
                
                colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
                plt.pie(charging_type_counts.values, 
                        labels=charging_type_counts.index, 
                        autopct='%1.1f%%',
                        colors=colors[:len(charging_type_counts)])
                plt.title('Charging Type Distribution', fontsize=14, fontweight='bold')
                plt.tight_layout()
                
                plot_path = output_dir / 'charging_type_distribution.png'
                plt.savefig(plot_path, dpi=300, bbox_inches='tight', facecolor='white')
                plt.close()
                print(f"📊 차트 저장: {plot_path}")
            
            # 2. 충전량 분포
            if '충전량_numeric' in df.columns:
                plt.figure(figsize=(12, 6))
                
                charging_amounts = df['충전량_numeric'].dropna()
                charging_amounts = charging_amounts[
                    (charging_amounts > 0) & (charging_amounts <= 200)
                ]  # 0-200kW 범위로 제한
                
                if len(charging_amounts) > 0:
                    plt.hist(charging_amounts, bins=50, alpha=0.7, 
                            color='#45B7D1', edgecolor='black', linewidth=0.5)
                    plt.xlabel('Charging Amount (kW)', fontsize=12)
                    plt.ylabel('Frequency', fontsize=12)
                    plt.title('Distribution of Charging Amounts', fontsize=14, fontweight='bold')
                    plt.grid(True, alpha=0.3)
                    
                    # 통계 정보 추가
                    mean_val = charging_amounts.mean()
                    median_val = charging_amounts.median()
                    plt.axvline(mean_val, color='red', linestyle='--', 
                                label=f'Mean: {mean_val:.1f}kW')
                    plt.axvline(median_val, color='orange', linestyle='--', 
                                label=f'Median: {median_val:.1f}kW')
                    plt.legend()
                    
                    plt.tight_layout()
                    
                    plot_path = output_dir / 'charging_amount_distribution.png'
                    plt.savefig(plot_path, dpi=300, bbox_inches='tight', facecolor='white')
                    plt.close()
                    print(f"📊 차트 저장: {plot_path}")
            
            # 3. 시도별 충전 현황
            if '시도' in df.columns:
                plt.figure(figsize=(14, 8))
                
                sido_counts = df['시도'].value_counts().head(10)
                
                bars = plt.bar(range(len(sido_counts)), sido_counts.values, 
                              color='#96CEB4', alpha=0.8, edgecolor='black', linewidth=0.5)
                plt.xlabel('Province/City', fontsize=12)
                plt.ylabel('Number of Charging Records', fontsize=12)
                plt.title('Charging Records by Province/City (Top 10)', fontsize=14, fontweight='bold')
                plt.xticks(range(len(sido_counts)), sido_counts.index, rotation=45, ha='right')
                plt.grid(True, alpha=0.3)
                
                # 값 표시
                for bar, value in zip(bars, sido_counts.values):
                    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + value*0.01,
                            f'{value:,}', ha='center', va='bottom', fontsize=10)
                
                plt.tight_layout()
                
                plot_path = output_dir / 'charging_by_province.png'
                plt.savefig(plot_path, dpi=300, bbox_inches='tight', facecolor='white')
                plt.close()
                print(f"📊 차트 저장: {plot_path}")
                
        except Exception as e:
            print(f"⚠️ 시각화 생성 중 오류: {e}")

# 외부에서 호출할 수 있는 함수들
def run_eda():
    """EDA 실행 함수"""
    analyzer = EDAAnalyzer()
    return analyzer.run_comprehensive_eda()

def run_complete_analysis():
    """완전한 분석을 실행하는 함수"""
    analyzer = EDAAnalyzer()
    return analyzer.run_comprehensive_eda()

def create_eda_analyzer():
    """EDAAnalyzer 인스턴스를 생성하는 함수"""
    return EDAAnalyzer()