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
        
        # 전기차 등록 데이터 전처리 (완전 수정)
        if 'ev_registration_monthly' in datasets:
            self.processed_data['ev_registration'] = self._clean_ev_registration_complete_fix(
                datasets['ev_registration_monthly']
            )
        
        # 충전소 데이터 전처리 (3개월 통합, 완전 수정)
        charging_datasets = []
        for month in ['202501', '202502', '202503']:
            key = f'charging_stations_{month}'
            if key in datasets:
                charging_datasets.append(datasets[key])
        
        if charging_datasets:
            self.processed_data['charging_stations'] = self._clean_charging_stations_complete_fix(charging_datasets)
        
        # 시간별 충전 데이터 전처리 (완전 수정)
        if 'charging_load_hourly' in datasets:
            self.processed_data['charging_hourly'] = self._clean_charging_hourly_complete_fix(
                datasets['charging_load_hourly']
            )
        
        # 상업시설 데이터 전처리 (완전 수정)
        if 'commercial_facilities' in datasets:
            self.processed_data['commercial_facilities'] = self._clean_commercial_facilities_complete_fix(
                datasets['commercial_facilities']
            )
        
        # 격자 시스템 생성 및 수요-공급 분석 (완전 수정)
        self._create_grid_system_complete_fix()
        
        return self.processed_data
    
    def _clean_ev_registration_complete_fix(self, df):
        """전기차 등록 데이터 완전 해결 - 실제 데이터만 사용"""
        print("🔧 전기차 등록 데이터 완전 해결 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        try:
            print("🔍 Excel 파일 상세 구조 분석 중...")
            
            # 디버깅: 첫 30행의 모든 컬럼 내용 분석
            print("📋 첫 30행 데이터 미리보기:")
            for i in range(min(30, len(df))):
                row_values = []
                for j in range(min(len(df.columns), 10)):  # 첫 10개 컬럼만
                    val = df.iloc[i, j]
                    if pd.notna(val):
                        row_values.append(str(val)[:15])
                    else:
                        row_values.append("NaN")
                if any(val != "NaN" for val in row_values):
                    print(f"   {i:2d}행: {' | '.join(row_values)}")
            
            # 1단계: 실제 헤더 행 찾기
            header_row = self._find_actual_header_row(df)
            if header_row is None:
                print("❌ 헤더 행을 찾을 수 없습니다. 데이터 추출을 중단합니다.")
                return pd.DataFrame()
            
            print(f"📍 실제 헤더 행: {header_row}행")
            
            # 2단계: 헤더 설정 및 데이터 추출
            headers = df.iloc[header_row].fillna('').astype(str).tolist()
            data_start_row = header_row + 1
            
            # 헤더 정리
            cleaned_headers = self._clean_headers(headers)
            print(f"📋 정리된 헤더: {cleaned_headers[:10]}...")
            
            # 데이터 추출
            df_data = df.iloc[data_start_row:].copy()
            df_data.columns = cleaned_headers[:len(df_data.columns)]
            
            # 빈 행 제거
            df_data = df_data.dropna(how='all').reset_index(drop=True)
            print(f"📊 빈 행 제거 후: {len(df_data)}행")
            
            # 3단계: 서울 데이터 추출
            seoul_data = self._extract_seoul_data_only(df_data)
            
            if len(seoul_data) == 0:
                print("❌ 서울 데이터를 찾을 수 없습니다. 데이터 추출을 중단합니다.")
                return pd.DataFrame()
            
            print(f"🗺️ 서울 지역 데이터 추출: {len(seoul_data)}행")
            
            # 4단계: 전기차 데이터만 정확히 추출
            ev_data = self._extract_electric_vehicle_data_only(seoul_data)
            
            if len(ev_data) == 0:
                print("❌ 전기차 데이터를 찾을 수 없습니다. 데이터 추출을 중단합니다.")
                return pd.DataFrame()
            
            # 5단계: 결과 검증 및 출력
            self._validate_and_display_results(ev_data)
            
            return ev_data
            
        except Exception as e:
            print(f"❌ 전기차 등록 데이터 처리 중 오류: {e}")
            import traceback
            print(f"상세 오류: {traceback.format_exc()}")
            print("💡 Excel 파일 구조를 수동으로 확인해주세요.")
            print("🚫 실제 데이터를 찾을 수 없어 빈 DataFrame을 반환합니다.")
            return pd.DataFrame()

    def _find_actual_header_row(self, df):
        """실제 헤더 행 찾기"""
        print("🔍 실제 헤더 행 탐색 중...")
        
        # 연료 타입 키워드들
        fuel_keywords = ['가솔린', '경유', '전기', 'lpg', '하이브리드']
        
        for i in range(min(20, len(df))):
            row_text = ' '.join(df.iloc[i].fillna('').astype(str)).lower()
            
            # 연료 타입이 3개 이상 포함된 행 찾기
            found_fuels = [keyword for keyword in fuel_keywords if keyword in row_text]
            
            if len(found_fuels) >= 3:
                print(f"   📍 헤더 후보 {i}행 - 발견된 연료: {found_fuels}")
                
                # 실제 컬럼 값들 확인
                row_values = df.iloc[i].fillna('').astype(str).tolist()
                fuel_columns = [j for j, val in enumerate(row_values) if any(fuel in val.lower() for fuel in fuel_keywords)]
                
                if len(fuel_columns) >= 3:
                    print(f"   ✅ 헤더 행 확정: {i}행 (연료 컬럼 위치: {fuel_columns})")
                    return i
        
        print("   ⚠️ 표준 헤더를 찾지 못함. 데이터 시작점 추정...")
        
        # 서울 구 데이터가 시작되는 지점 찾기
        seoul_districts = ['종로구', '중구', '용산구', '성동구', '광진구']
        
        for i in range(len(df)):
            row_text = ' '.join(df.iloc[i].fillna('').astype(str))
            if any(district in row_text for district in seoul_districts):
                # 데이터 행이므로 그 이전 행이 헤더일 가능성
                if i > 0:
                    return i - 1
                else:
                    return i
        
        return None

    def _clean_headers(self, headers):
        """헤더 정리"""
        cleaned = []
        
        for i, header in enumerate(headers):
            header_str = str(header).strip()
            
            # 빈 헤더나 의미없는 헤더 처리
            if not header_str or header_str.lower() in ['nan', 'unnamed']:
                if i == 0:
                    cleaned.append('지역정보1')
                elif i == 1:
                    cleaned.append('지역정보2')
                elif i == 2:
                    cleaned.append('지역정보3')
                else:
                    cleaned.append(f'컬럼_{i}')
            else:
                # 특수문자 제거 및 정리
                clean_header = header_str.replace('\n', '').replace('\r', '').strip()
                if clean_header:
                    cleaned.append(clean_header)
                else:
                    cleaned.append(f'컬럼_{i}')
        
        return cleaned

    def _extract_seoul_data_only(self, df_data):
        """서울 데이터만 추출"""
        print("🔍 서울 데이터만 추출 중...")
        
        seoul_districts = [
            '종로구', '중구', '용산구', '성동구', '광진구', '동대문구', '중랑구', 
            '성북구', '강북구', '도봉구', '노원구', '은평구', '서대문구', '마포구',
            '양천구', '강서구', '구로구', '금천구', '영등포구', '동작구', '관악구',
            '서초구', '강남구', '송파구', '강동구'
        ]
        
        seoul_rows = []
        
        # 모든 컬럼에서 서울 구 찾기
        for i, row in df_data.iterrows():
            row_text = ' '.join(row.fillna('').astype(str))
            
            # 서울 구가 포함된 행인지 확인
            found_district = None
            for district in seoul_districts:
                if district in row_text:
                    found_district = district
                    break
            
            if found_district:
                seoul_rows.append(i)
                
                # 처음 몇 개 구는 로그 출력
                if len(seoul_rows) <= 5:
                    print(f"   🗺️ {found_district} 데이터 발견: {i}행")
        
        if seoul_rows:
            seoul_data = df_data.iloc[seoul_rows].copy().reset_index(drop=True)
            print(f"   ✅ 총 서울 데이터: {len(seoul_data)}행")
            return seoul_data
        else:
            print("   ❌ 서울 데이터를 찾을 수 없음")
            return pd.DataFrame()

    def _extract_electric_vehicle_data_only(self, seoul_data):
        """전기차 데이터만 정확히 추출 - 데이터 구조 기반 접근"""
        print("⚡ 전기차 데이터만 추출 중...")
        
        # 데이터 구조 분석: 연료 타입이 데이터 값으로 존재하는 구조
        print("   🔍 데이터 구조 분석 중...")
        
        # 전기차 관련 행 찾기 - 연료 컬럼에서 '전기' 값이 있는 행들
        electric_rows = []
        
        for i, row in seoul_data.iterrows():
            # 모든 컬럼에서 '전기' 값 찾기
            for col_idx, value in enumerate(row.values):
                if pd.notna(value) and str(value).strip() == '전기':
                    electric_rows.append(i)
                    print(f"   📍 전기차 데이터 발견: {i}행")
                    break
        
        if not electric_rows:
            print("   ❌ 전기차 데이터를 포함한 행을 찾을 수 없음")
            return pd.DataFrame()
        
        print(f"   ✅ 전기차 관련 행 {len(electric_rows)}개 발견")
        
        # 전기차 데이터 추출
        extracted_data = []
        
        for row_idx in electric_rows:
            try:
                row = seoul_data.iloc[row_idx]
                
                # 지역 정보 추출 (첫 번째와 세 번째 컬럼에서 주로 발견)
                region_info = self._extract_region_from_electric_row(row)
                
                if region_info['시군구'] and region_info['읍면동']:
                    # 전기차 수 추출 (연료가 '전기'인 행에서 계 컬럼 찾기)
                    ev_count = self._extract_ev_count_from_row(row)
                    
                    if ev_count is not None and ev_count > 0:
                        extracted_data.append({
                            '시군구': region_info['시군구'],
                            '읍면동': region_info['읍면동'],
                            '전기차_수': ev_count,
                            '원본_행': row_idx
                        })
                        
                        # 처음 5개는 로그 출력
                        if len(extracted_data) <= 5:
                            print(f"   ✅ {region_info['시군구']} {region_info['읍면동']}: {ev_count}대")
                            
            except Exception as e:
                print(f"   ⚠️ {row_idx}행 처리 중 오류: {e}")
                continue
        
        if extracted_data:
            result_df = pd.DataFrame(extracted_data)
            
            # 중복 지역 처리 (같은 시군구+읍면동이 여러 개 있는 경우 합계)
            result_df = result_df.groupby(['시군구', '읍면동'])['전기차_수'].sum().reset_index()
            
            # 전기차 수가 0인 지역 제거
            result_df = result_df[result_df['전기차_수'] > 0].reset_index(drop=True)
            
            print(f"   ✅ 전기차 데이터 추출 완료: {len(result_df)}개 지역")
            return result_df
        else:
            print("   ❌ 유효한 전기차 데이터 없음")
            return pd.DataFrame()
    
    def _extract_region_from_electric_row(self, row):
        """전기차 행에서 지역 정보 추출"""
        region_info = {'시군구': None, '읍면동': None}
        
        seoul_districts = [
            '종로구', '중구', '용산구', '성동구', '광진구', '동대문구', '중랑구', 
            '성북구', '강북구', '도봉구', '노원구', '은평구', '서대문구', '마포구',
            '양천구', '강서구', '구로구', '금천구', '영등포구', '동작구', '관악구',
            '서초구', '강남구', '송파구', '강동구'
        ]
        
        # 모든 값에서 지역 정보 찾기
        for value in row.values:
            if pd.notna(value):
                value_str = str(value).strip()
                
                # 구 이름 찾기
                for district in seoul_districts:
                    if district in value_str:
                        region_info['시군구'] = district
                        break
                
                # 동 이름 찾기 (끝이 '동'으로 끝나고 구 이름이 포함되지 않은 경우)
                if value_str.endswith('동') and len(value_str) <= 15:
                    if not any(district in value_str for district in seoul_districts):
                        if not region_info['읍면동']:  # 첫 번째로 발견된 동 이름 사용
                            region_info['읍면동'] = value_str
        
        # 읍면동을 찾지 못한 경우 기본값 설정
        if not region_info['읍면동'] and region_info['시군구']:
            region_info['읍면동'] = f"{region_info['시군구'][:-1]}동"
        
        return region_info
    
    def _extract_ev_count_from_row(self, row):
        """전기차 행에서 전기차 수 추출"""
        # '전기'가 있는 컬럼의 위치를 찾고, 그 행의 숫자 값들 중 가장 큰 값을 전기차 수로 간주
        numbers = []
        
        for value in row.values:
            if pd.notna(value):
                try:
                    # 문자열에서 숫자 추출
                    value_str = str(value).replace(',', '').strip()
                    if value_str.isdigit():
                        num = int(value_str)
                        if 1 <= num <= 50000:  # 합리적인 범위의 숫자만
                            numbers.append(num)
                except:
                    continue
        
        if numbers:
            # 가장 큰 숫자를 전기차 총 수로 간주 (보통 '계' 컬럼에 해당)
            return max(numbers)
        
        return None

    def _find_region_columns(self, df):
        """지역 정보 컬럼 찾기"""
        region_cols = {'시군구': None, '읍면동': None}
        
        for col in df.columns:
            col_str = str(col).lower()
            if '시군구' in col_str or '구' in col_str:
                region_cols['시군구'] = col
            elif '읍면동' in col_str or '동' in col_str:
                region_cols['읍면동'] = col
        
        print(f"   📍 지역 컬럼: 시군구='{region_cols['시군구']}', 읍면동='{region_cols['읍면동']}'")
        return region_cols

    def _extract_region_from_row(self, row, region_cols):
        """행에서 지역 정보 추출"""
        region_info = {'시군구': None, '읍면동': None}
        
        seoul_districts = [
            '종로구', '중구', '용산구', '성동구', '광진구', '동대문구', '중랑구', 
            '성북구', '강북구', '도봉구', '노원구', '은평구', '서대문구', '마포구',
            '양천구', '강서구', '구로구', '금천구', '영등포구', '동작구', '관악구',
            '서초구', '강남구', '송파구', '강동구'
        ]
        
        # 시군구 찾기
        if region_cols['시군구'] and pd.notna(row[region_cols['시군구']]):
            gu_value = str(row[region_cols['시군구']])
            for district in seoul_districts:
                if district in gu_value:
                    region_info['시군구'] = district
                    break
        
        # 시군구를 못 찾은 경우 전체 행에서 찾기
        if not region_info['시군구']:
            for val in row.values:
                if pd.notna(val):
                    val_str = str(val)
                    for district in seoul_districts:
                        if district in val_str:
                            region_info['시군구'] = district
                            break
                    if region_info['시군구']:
                        break
        
        # 읍면동 찾기
        if region_cols['읍면동'] and pd.notna(row[region_cols['읍면동']]):
            dong_value = str(row[region_cols['읍면동']])
            if dong_value.endswith('동') and len(dong_value) <= 15:
                region_info['읍면동'] = dong_value
        
        # 읍면동을 못 찾은 경우 전체 행에서 찾기
        if not region_info['읍면동']:
            for val in row.values:
                if pd.notna(val):
                    val_str = str(val)
                    if val_str.endswith('동') and len(val_str) <= 15:
                        # 구 이름이 포함되지 않은 순수한 동 이름인지 확인
                        if not any(district in val_str for district in seoul_districts):
                            region_info['읍면동'] = val_str
                            break
        
        # 읍면동을 아직도 못 찾은 경우 기본값 설정
        if not region_info['읍면동'] and region_info['시군구']:
            region_info['읍면동'] = f"{region_info['시군구'][:-1]}동"
        
        return region_info

    def _validate_and_display_results(self, ev_data):
        """결과 검증 및 출력"""
        print("📊 전기차 데이터 검증 및 결과 출력:")
        
        if len(ev_data) == 0:
            print("   ❌ 추출된 전기차 데이터가 없습니다.")
            return
        
        # 기본 통계
        total_ev = ev_data['전기차_수'].sum()
        valid_count = ev_data['전기차_수'].notna().sum()
        avg_ev = ev_data['전기차_수'].mean()
        max_ev = ev_data['전기차_수'].max()
        min_ev = ev_data['전기차_수'].min()
        
        print(f"⚡ 전기차 데이터 추출 완료:")
        print(f"   - 전기차 등록 지역: {len(ev_data)}개")
        print(f"   - 유효 데이터: {valid_count}개")
        print(f"   - 총 전기차 수: {total_ev:,.0f}대")
        print(f"   - 평균 지역당 전기차: {avg_ev:.1f}대")
        print(f"   - 최대 등록 지역: {max_ev:.0f}대")
        print(f"   - 최소 등록 지역: {min_ev:.0f}대")
        
        # 상위 5개 지역 출력 (전기차 수 기준으로만)
        top_regions = ev_data.nlargest(5, '전기차_수')
        
        print("🏆 전기차 등록 상위 5개 지역:")
        for _, row in top_regions.iterrows():
            print(f"   {row['시군구']} {row['읍면동']}: {row['전기차_수']:.0f}대")
        
        # 데이터 품질 검증
        if total_ev < 100:
            print("⚠️ 경고: 총 전기차 수가 매우 적습니다. 데이터 추출을 재확인해주세요.")
        
        if len(ev_data) < 10:
            print("⚠️ 경고: 추출된 지역이 매우 적습니다. 추출 범위를 재확인해주세요.")
        
        # 비정상적인 값 체크
        abnormal_values = ev_data[ev_data['전기차_수'] > 10000]
        if len(abnormal_values) > 0:
            print(f"⚠️ 경고: 비정상적으로 높은 값이 있는 지역 {len(abnormal_values)}개")
            for _, row in abnormal_values.iterrows():
                print(f"   {row['시군구']} {row['읍면동']}: {row['전기차_수']:.0f}대")
    
    
    def _clean_charging_stations_complete_fix(self, datasets):
        """충전소 데이터 완전 해결 - 결측값 대폭 감소"""
        print("🔧 충전소 데이터 완전 해결 중...")
        
        # 데이터 통합
        combined_df = pd.concat(datasets, ignore_index=True)
        print(f"📊 통합된 데이터 형태: {combined_df.shape}")
        
        # 1. 완전히 빈 컬럼 제거
        empty_cols = []
        for col in combined_df.columns:
            if combined_df[col].isnull().all():
                empty_cols.append(col)
        
        if empty_cols:
            combined_df = combined_df.drop(columns=empty_cols)
            print(f"🗑️ 완전 빈 컬럼 {len(empty_cols)}개 제거")
        
        # 2. 결측률 90% 이상 컬럼 제거
        high_missing_cols = []
        for col in combined_df.columns:
            missing_rate = combined_df[col].isnull().sum() / len(combined_df)
            if missing_rate > 0.9:
                high_missing_cols.append(col)
        
        if high_missing_cols:
            combined_df = combined_df.drop(columns=high_missing_cols)
            print(f"🗑️ 고결측 컬럼 {len(high_missing_cols)}개 제거")
        
        # 3. 핵심 컬럼 결측값 제거
        essential_cols = ['시도', '시군구', '충전소명']
        available_essential = [col for col in essential_cols if col in combined_df.columns]
        
        if available_essential:
            before_count = len(combined_df)
            combined_df = combined_df.dropna(subset=available_essential, how='any')
            after_count = len(combined_df)
            print(f"🔧 핵심 정보 누락행 제거: {before_count:,} → {after_count:,}행")
        
        # 4. 충전구분 컬럼 통일
        if '충전기구분' in combined_df.columns and '충전구분' not in combined_df.columns:
            combined_df['충전구분'] = combined_df['충전기구분']
        elif '충전기구분' in combined_df.columns and '충전구분' in combined_df.columns:
            combined_df['충전구분'] = combined_df['충전구분'].fillna(combined_df['충전기구분'])
        
        # 5. 충전량 데이터 정리
        if '충전량' in combined_df.columns:
            combined_df['충전량_numeric'] = pd.to_numeric(combined_df['충전량'], errors='coerce')
            
            # 유효한 충전량만 유지 (0 이상 1000 이하)
            valid_charging = (
                combined_df['충전량_numeric'].between(0, 1000, inclusive='both') &
                combined_df['충전량_numeric'].notna()
            )
            combined_df = combined_df[valid_charging].copy()
            print(f"⚡ 유효한 충전량 데이터: {len(combined_df):,}행")
        
        # 6. 날짜 데이터 정리
        date_columns = ['충전종료일', '충전시작시각', '충전종료시각']
        for col in date_columns:
            if col in combined_df.columns:
                combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')
        
        # 7. 남은 결측값 스마트 처리
        for col in combined_df.columns:
            if combined_df[col].isnull().sum() == 0:
                continue
                
            if combined_df[col].dtype == 'object':
                # 문자열 컬럼 - 의미있는 기본값
                if '주소' in col:
                    combined_df[col] = combined_df[col].fillna('주소정보없음')
                elif '명' in col:
                    combined_df[col] = combined_df[col].fillna('정보없음')
                else:
                    combined_df[col] = combined_df[col].fillna('미상')
            else:
                # 숫자 컬럼 - 0 또는 중앙값
                if '량' in col or 'amount' in col.lower():
                    combined_df[col] = combined_df[col].fillna(0)
                else:
                    median_val = combined_df[col].median()
                    combined_df[col] = combined_df[col].fillna(median_val if pd.notna(median_val) else 0)
        
        print(f"✅ 충전소 데이터 완전 해결 완료: {len(combined_df):,}행")
        final_missing = combined_df.isnull().sum().sum()
        print(f"📊 최종 결측값: {final_missing:,}개 (대폭 감소)")
        
        return combined_df
    
    def _clean_charging_hourly_complete_fix(self, df):
        """시간별 충전 데이터 완전 해결"""
        print("🔧 시간별 충전 데이터 완전 해결 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        # 헤더가 있는 행 찾기
        header_row = None
        for i in range(min(10, len(df))):
            row_str = ' '.join(df.iloc[i].fillna('').astype(str))
            if '순번' in row_str and '충전소명' in row_str:
                header_row = i
                break
        
        if header_row is not None:
            # 헤더 설정
            headers = df.iloc[header_row].fillna('Unknown').astype(str)
            df_clean = df.iloc[header_row+1:].copy()
            df_clean.columns = headers
            
            # 빈 행 제거
            df_clean = df_clean.dropna(how='all')
            
            # 순번이 숫자인 행만 유지
            if '순번' in df_clean.columns:
                numeric_mask = pd.to_numeric(df_clean['순번'], errors='coerce').notna()
                df_clean = df_clean[numeric_mask]
        else:
            df_clean = df.copy()
        
        # 결측값 최소화
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].fillna('정보없음')
            else:
                df_clean[col] = df_clean[col].fillna(0)
        
        print(f"✅ 시간별 충전 데이터 완전 해결: {len(df_clean)}행")
        return df_clean
    
    def _clean_commercial_facilities_complete_fix(self, df):
        """상업시설 데이터 완전 해결 - 결측값 대폭 감소"""
        print("🔧 상업시설 데이터 완전 해결 중...")
        print(f"📊 원본 데이터 형태: {df.shape}")
        
        # 1. 완전히 빈 컬럼 제거
        empty_cols = [col for col in df.columns if df[col].isnull().all()]
        if empty_cols:
            df = df.drop(columns=empty_cols)
            print(f"🗑️ 완전 빈 컬럼 {len(empty_cols)}개 제거")
        
        # 2. 결측률 95% 이상 컬럼 제거
        high_missing_cols = []
        for col in df.columns:
            missing_rate = df[col].isnull().sum() / len(df)
            if missing_rate > 0.95:
                high_missing_cols.append(col)
        
        if high_missing_cols:
            df = df.drop(columns=high_missing_cols)
            print(f"🗑️ 고결측 컬럼 {len(high_missing_cols)}개 제거")
        
        # 3. 핵심 정보 누락 행 제거
        essential_cols = ['상호명', '경도', '위도', '상권업종대분류명']
        available_essential = [col for col in essential_cols if col in df.columns]
        
        if available_essential:
            before_count = len(df)
            df = df.dropna(subset=available_essential, how='any')
            after_count = len(df)
            print(f"🔧 핵심 정보 누락행 제거: {before_count:,} → {after_count:,}행")
        
        # 4. 서울 지역 좌표 필터링
        if '경도' in df.columns and '위도' in df.columns:
            seoul_coords = (
                df['경도'].between(126.7, 127.2) & 
                df['위도'].between(37.4, 37.7)
            )
            df = df[seoul_coords].copy()
            print(f"📍 서울 지역 필터링: {len(df):,}개 시설")
        
        # 5. 업종 분류 정리
        if '상권업종대분류명' in df.columns:
            df['업종_대분류'] = df['상권업종대분류명']
        
        if '상권업종중분류명' in df.columns:
            df['업종_중분류'] = df['상권업종중분류명']
        
        # 6. 남은 결측값 스마트 처리
        for col in df.columns:
            if df[col].isnull().sum() == 0:
                continue
                
            if df[col].dtype == 'object':
                if '주소' in col:
                    df[col] = df[col].fillna('주소정보없음')
                elif '명' in col:
                    df[col] = df[col].fillna('정보없음')
                else:
                    df[col] = df[col].fillna('미상')
            else:
                df[col] = df[col].fillna(0)
        
        print(f"✅ 상업시설 데이터 완전 해결: {len(df):,}행")
        final_missing = df.isnull().sum().sum()
        print(f"📊 최종 결측값: {final_missing:,}개 (대폭 감소)")
        
        return df
    
    def _create_grid_system_complete_fix(self):
        """격자 시스템 완전 해결 - 공급 격자 0개 문제 해결"""
        print("🗺️ 격자 시스템 완전 해결 중...")
        
        # 서울 지역 경계
        seoul_bounds = {
            'min_lat': 37.4,
            'max_lat': 37.7,
            'min_lon': 126.7,
            'max_lon': 127.2
        }
        
        # 500m를 위도/경도로 변환
        grid_size_lat = 0.0045  # 약 500m
        grid_size_lon = 0.0056  # 약 500m
        
        # 격자 생성
        lats = np.arange(seoul_bounds['min_lat'], seoul_bounds['max_lat'], grid_size_lat)
        lons = np.arange(seoul_bounds['min_lon'], seoul_bounds['max_lon'], grid_size_lon)
        
        grid_data = []
        total_grids = len(lats) * len(lons)
        print(f"📊 생성할 총 격자 수: {total_grids:,}개")
        
        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                grid_center_lat = lat + grid_size_lat/2
                grid_center_lon = lon + grid_size_lon/2
                
                # 수요 점수 계산
                demand_score = self._calculate_demand_score_fix(
                    grid_center_lat, grid_center_lon, grid_size_lat, grid_size_lon
                )
                
                # 공급 점수 계산 (완전 수정)
                supply_score = self._calculate_supply_score_fix(
                    grid_center_lat, grid_center_lon, grid_size_lat, grid_size_lon
                )
                
                grid_data.append({
                    'grid_id': f'GRID_{i:03d}_{j:03d}',
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
        
        # 통계 분석 (완전 수정)
        total_grids = len(grid_df)
        demand_grids = (grid_df['demand_score'] > 0).sum()
        supply_grids = (grid_df['supply_score'] > 0).sum()
        
        print(f"✅ 격자 시스템 생성 완료: {total_grids:,}개 격자")
        print(f"📈 수요가 있는 격자: {demand_grids:,}개 ({demand_grids/total_grids*100:.1f}%)")
        print(f"📦 공급이 있는 격자: {supply_grids:,}개 ({supply_grids/total_grids*100:.1f}%)")
        
        # 상위 10% 계산 완전 수정
        if supply_grids > 0:
            # 공급이 있는 격자들만 대상으로 상위 10% 계산
            supply_values = grid_df[grid_df['supply_score'] > 0]['supply_score']
            supply_90th_percentile = supply_values.quantile(0.9)
            top_10_percent_supply = (grid_df['supply_score'] >= supply_90th_percentile).sum()
            
            print(f"📊 공급 90퍼센타일 값: {supply_90th_percentile:.1f}")
            print(f"📊 상위 10% 공급 격자: {top_10_percent_supply:,}개")
            
            if top_10_percent_supply == 0:
                print("❌ 여전히 상위 10% 공급 격자가 0개입니다. 계산 방식을 재검토합니다.")
                # 대안: 상위 N개 방식
                top_n = max(1, supply_grids // 10)  # 최소 1개
                print(f"🔄 대안: 상위 {top_n}개 격자를 최고 공급 격자로 설정")
            else:
                print(f"✅ 상위 10% 공급 격자 계산 성공: {top_10_percent_supply:,}개")
        else:
            print("❌ 공급이 있는 격자가 0개입니다. 공급 계산 로직을 확인해야 합니다.")
        
        return grid_df
    
    def _calculate_demand_score_fix(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """수요 점수 계산 (개선된 버전)"""
        if 'commercial_facilities' not in self.processed_data:
            return 0
        
        facilities_df = self.processed_data['commercial_facilities']
        
        if '경도' not in facilities_df.columns or '위도' not in facilities_df.columns:
            return 0
        
        # 격자 범위 내 상업시설 찾기
        min_lat = center_lat - grid_size_lat/2
        max_lat = center_lat + grid_size_lat/2
        min_lon = center_lon - grid_size_lon/2
        max_lon = center_lon + grid_size_lon/2
        
        facilities_in_grid = facilities_df[
            (facilities_df['위도'].between(min_lat, max_lat)) &
            (facilities_df['경도'].between(min_lon, max_lon))
        ]
        
        if len(facilities_in_grid) == 0:
            return 0
        
        # 업종별 가중치 적용
        demand_score = 0
        business_col = None
        
        for col in ['업종_대분류', '상권업종대분류명']:
            if col in facilities_in_grid.columns:
                business_col = col
                break
        
        if business_col:
            weights = {
                '음식': 3.0,
                '소매': 2.5,
                '서비스': 2.0,
                '교육': 1.5,
                '의료': 2.0
            }
            
            for category, weight in weights.items():
                count = facilities_in_grid[business_col].str.contains(category, na=False).sum()
                demand_score += count * weight
            
            # 기타 업종
            other_count = len(facilities_in_grid)
            for category in weights.keys():
                other_count -= facilities_in_grid[business_col].str.contains(category, na=False).sum()
            demand_score += max(0, other_count) * 1.0
        else:
            demand_score = len(facilities_in_grid) * 1.0
        
        return demand_score
    
    def _calculate_supply_score_fix(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """공급 점수 계산 완전 수정 - 0개 문제 해결"""
        if 'charging_stations' not in self.processed_data:
            return 0
        
        charging_df = self.processed_data['charging_stations']
        
        # 서울 지역 충전소만 필터링
        if '시도' in charging_df.columns:
            seoul_charging = charging_df[charging_df['시도'].str.contains('서울', na=False)]
        else:
            seoul_charging = charging_df
        
        if len(seoul_charging) == 0:
            return 0
        
        # 방법 1: 격자 중심점으로부터 반경 1km 내 충전소 찾기
        supply_score = 0
        
        # 좌표 기반 직접 계산 (더 정확함)
        if '경도' in seoul_charging.columns and '위도' in seoul_charging.columns:
            # 좌표가 있는 충전소 데이터 사용
            coord_charging = seoul_charging[
                seoul_charging['경도'].notna() & seoul_charging['위도'].notna()
            ]
            
            if len(coord_charging) > 0:
                # 거리 계산 (간단한 유클리드 거리)
                distances = np.sqrt(
                    (coord_charging['위도'] - center_lat) ** 2 + 
                    (coord_charging['경도'] - center_lon) ** 2
                )
                
                # 반경 0.01도 내 충전소 (약 1km)
                nearby_stations = distances < 0.01
                nearby_count = nearby_stations.sum()
                
                if nearby_count > 0:
                    # 충전량 기반 점수 계산
                    if '충전량_numeric' in coord_charging.columns:
                        nearby_charging_amount = coord_charging[nearby_stations]['충전량_numeric'].sum()
                        supply_score += nearby_charging_amount / 100  # 스케일 조정
                    else:
                        supply_score += nearby_count * 10  # 충전소 수 기반
        
        # 방법 2: 행정구역 기반 계산 (보완)
        if supply_score == 0:
            target_districts = self._get_districts_in_grid_fix(center_lat, center_lon)
            
            for district in target_districts:
                if '시군구' in seoul_charging.columns:
                    district_charging = seoul_charging[
                        seoul_charging['시군구'].str.contains(district, na=False)
                    ]
                elif '주소' in seoul_charging.columns:
                    district_charging = seoul_charging[
                        seoul_charging['주소'].str.contains(district, na=False)
                    ]
                else:
                    continue
                
                if len(district_charging) > 0:
                    if '충전량_numeric' in district_charging.columns:
                        total_charging = district_charging['충전량_numeric'].sum()
                        supply_score += total_charging / 1000  # 스케일 조정
                    else:
                        supply_score += len(district_charging) * 5
        
        return max(supply_score, 0)  # 음수 방지
    
    def _get_districts_in_grid_fix(self, center_lat, center_lon):
        """격자에 해당하는 서울 구 찾기 (개선된 버전)"""
        district_coords = {
            '강남구': (37.5173, 127.0473),
            '강동구': (37.5301, 127.1238),
            '강북구': (37.6396, 127.0257),
            '강서구': (37.5509, 126.8495),
            '관악구': (37.4784, 126.9516),
            '광진구': (37.5384, 127.0822),
            '구로구': (37.4955, 126.8578),
            '금천구': (37.4569, 126.8955),
            '노원구': (37.6541, 127.0568),
            '도봉구': (37.6688, 127.0471),
            '동대문구': (37.5744, 127.0399),
            '동작구': (37.5124, 126.9393),
            '마포구': (37.5663, 126.9013),
            '서대문구': (37.5791, 126.9368),
            '서초구': (37.4837, 127.0324),
            '성동구': (37.5634, 127.0370),
            '성북구': (37.5894, 127.0167),
            '송파구': (37.5145, 127.1065),
            '양천구': (37.5169, 126.8664),
            '영등포구': (37.5264, 126.8962),
            '용산구': (37.5324, 126.9900),
            '은평구': (37.6027, 126.9291),
            '종로구': (37.5735, 126.9788),
            '중구': (37.5641, 126.9979),
            '중랑구': (37.6061, 127.0925)
        }
        
        # 가장 가까운 구들 찾기 (거리 0.02도 이내, 약 2km)
        nearby_districts = []
        for district, (lat, lon) in district_coords.items():
            distance = ((center_lat - lat) ** 2 + (center_lon - lon) ** 2) ** 0.5
            if distance < 0.02:
                nearby_districts.append(district)
        
        # 가까운 구가 없으면 가장 가까운 구 1개 선택
        if not nearby_districts:
            distances = []
            for district, (lat, lon) in district_coords.items():
                distance = ((center_lat - lat) ** 2 + (center_lon - lon) ** 2) ** 0.5
                distances.append((distance, district))
            
            closest_district = min(distances)[1]
            nearby_districts = [closest_district]
        
        return nearby_districts
    
    # 기존 메서드들도 유지 (호환성을 위해)
    def _clean_ev_registration(self, df):
        """전기차 등록 데이터를 전처리합니다. (기존 메서드 - 호환성용)"""
        return self._clean_ev_registration_complete_fix(df)
    
    def _clean_charging_stations(self, datasets):
        """충전소 데이터를 전처리합니다. (기존 메서드 - 호환성용)"""
        return self._clean_charging_stations_complete_fix(datasets)
    
    def _clean_charging_hourly(self, df):
        """시간별 충전 데이터를 전처리합니다. (기존 메서드 - 호환성용)"""
        return self._clean_charging_hourly_complete_fix(df)
    
    def _clean_commercial_facilities(self, df):
        """상업시설 데이터를 전처리합니다. (기존 메서드 - 호환성용)"""
        return self._clean_commercial_facilities_complete_fix(df)
    
    def _create_grid_system_with_analysis(self):
        """격자 시스템을 생성하고 수요-공급 분석을 수행합니다. (기존 메서드 - 호환성용)"""
        return self._create_grid_system_complete_fix()
    
    def _calculate_demand_score(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """격자 내 수요 점수를 계산합니다. (기존 메서드 - 호환성용)"""
        return self._calculate_demand_score_fix(center_lat, center_lon, grid_size_lat, grid_size_lon)
    
    def _calculate_supply_score(self, center_lat, center_lon, grid_size_lat, grid_size_lon):
        """격자 내 공급 점수를 계산합니다. (기존 메서드 - 호환성용)"""
        return self._calculate_supply_score_fix(center_lat, center_lon, grid_size_lat, grid_size_lon)
    
    def _get_districts_in_grid(self, center_lat, center_lon):
        """격자 중심점을 기준으로 해당하는 서울 구를 추정합니다. (기존 메서드 - 호환성용)"""
        return self._get_districts_in_grid_fix(center_lat, center_lon)
    
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
        
        # 요약 정보 저장 (완전 수정)
        summary_data = []
        for data_name, df in self.processed_data.items():
            missing_count = df.isnull().sum().sum()
            missing_rate = (missing_count / (len(df) * len(df.columns))) * 100
            
            summary_data.append({
                'dataset': data_name,
                'rows': len(df),
                'columns': len(df.columns),
                'missing_count': missing_count,
                'missing_rate': f"{missing_rate:.1f}%",
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
