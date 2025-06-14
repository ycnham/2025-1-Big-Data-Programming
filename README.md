
# 🚗 2025-1 Big Data Programming
# 데이터 기반 전기차 충전소 최적 위치 선정 및 추천 시스템 – G조

---

## 개요 (Overview)

전기차 시장의 급속한 성장에 비해 충전 인프라의 공급은 부족하고, 지역 간 불균형이 심화되고 있습니다.  
기존 충전소 입지는 설치 용이성 위주로 결정되어 수요를 제대로 반영하지 못하며,  
충전소 부족은 전기차 보급의 주요 장애 요소로 작용하고 있습니다.

본 프로젝트는 서울시를 대상으로 전기차 충전소의 **최적 입지 선정**을 목표로 하며,  
다양한 데이터를 기반으로 수요 예측과 최적화 모델을 통해 **제한된 설치 수 내 최대 수요를 커버**할 수 있는 전략을 제시합니다.  
이를 통해 효율적인 인프라 배치와 정책적 의사결정 지원을 도모합니다.

---

## 실행 순서 (Quick Start)

### 1. 환경 설정 및 클론
```bash
# 1. 프로젝트 클론
git clone https://github.com/ycnham/2025-1-Big-Data-Programming.git
cd 2025-1-Big-Data-Programming

# 2. 가상환경 생성
conda create -n bigdata python=3.12
conda activate bigdata

# 3. 필요 패키지 설치
pip install -r requirements.txt
```

### 2. 프로젝트 폴더 구조 생성
```bash
# 자동 생성 스크립트 실행
python scripts/setup_directories.py

# 또는 수동 생성
mkdir -p data/raw data/processed data/modeling outputs/eda outputs/plots outputs/maps
```

### 3. 원본 데이터 파일 배치
**[📁 Raw 데이터 파일 위치](#-raw-데이터-파일-위치-data-raw) 섹션을 참고하여 필요한 데이터 파일들을 `data/raw/` 폴더에 배치**

### 4. 전체 파이프라인 실행
```bash
# 방법 1: 통합 노트북 실행
cd scripts
jupyter notebook main.ipynb
# → 셀을 순서대로 실행하면 전체 분석 완료

# 방법 2: 개별 스크립트 실행
python scripts/run_preprocessing.py    # 전처리 실행
cd scripts
jupyter notebook                       # 개별 분석 노트북 실행
```

## 📁 필수 생성 폴더

```
2025-1-Big-Data-Programming/
├── README.md
├── requirements.txt
│
├── data/
│   ├── raw/                     # 원본 데이터 파일 위치 (사용자가 직접 배치)
│   ├── processed/               # 전처리 완료 데이터 (자동 생성)
│   └── modeling/                # 모델링용 파생 데이터 (자동 생성)
│
├── outputs/
│   ├── eda/                     # 탐색적 데이터 분석 결과 (자동 생성)
│   ├── plots/                   # 시각화 결과 (자동 생성)
│   └── maps/                    # MCLP 결과 지도 (자동 생성)
│
├── scripts/
│   ├── run_preprocessing.py     # 전처리 실행 스크립트
│   ├── main.ipynb               # 메인 실행 노트북
│   └── setup_directories.py    # 폴더 구조 자동 생성 스크립트
│
├── src/
│   ├── preprocessing/           # 전처리 모듈
│   ├── analysis/                # 수요 예측 및 클러스터링
│   ├── modeling/                # MCLP 모델 명세
│   ├── utils/                   # 자동화 등 특수 기능
│   └── visualization/           # 시각화
└── 
```

### 폴더 자동 생성 스크립트

다음 스크립트를 `scripts/setup_directories.py`로 저장 & 실행

```python
import os
from pathlib import Path

def create_project_directories():
    """프로젝트 필수 폴더들을 자동 생성합니다."""
    
    # 필수 폴더 목록
    directories = [
        "data/raw",              # 원본 데이터
        "data/processed",        # 전처리 결과
        "data/modeling",         # 모델링 데이터
        "outputs/eda",           # EDA 결과
        "outputs/plots",         # 시각화 결과
        "outputs/maps",          # 지도 파일
        "scripts",               # 실행 스크립트
        "src/preprocessing",     # 전처리 모듈
        "src/analysis",          # 분석 모듈
        "src/modeling",          # 모델링 모듈
        "src/utils",             # 유틸리티
        "src/visualization"      # 시각화 모듈
    ]
    
    project_root = Path.cwd()
    
    print("🔧 프로젝트 폴더 구조 생성 중...")
    
    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"    {directory}")
    
    print(f"\n 총 {len(directories)}개 폴더 생성 완료!")
    print("\n 다음 단계:")
    print("   1. data/raw/ 폴더에 원본 데이터 파일 배치")
    print("   2. scripts/main.ipynb 실행")

if __name__ == "__main__":
    create_project_directories()
```

---

## 📄 Raw 데이터 파일 위치 (data/raw/)

아래의 데이터 파일을 `data/raw/` 폴더에 **정확한 파일명**으로 배치

### 데이터셋 목록

```
📁 data/raw/
├── (참고자료) 한국전력공사_전기차충전서비스운영시스템_고객센터 상담내역_코드표.xlsx
├── 서울시 소유 충전기 일별 시간별 충전현황.xlsx
├── 서울시 자치구 읍면동별 연료별 자동차 등록현황(행정동)(25년04월).xls
├── 소상공인시장진흥공단_상가(상권)정보_서울_202503.csv
├── 월별 소통정보 (구간별-첨두시별).csv
├── 전기차 충전소 충전량 데이터_202501.xlsx
├── 전기차 충전소 충전량 데이터_202502.xlsx  
├── 전기차 충전소 충전량 데이터_202503.xlsx
├── 한국전력공사_전기차충전서비스운영시스템_고객센터 상담 내역_20241231.csv
└── 한국환경공단_전기차 충전소 위치 및 운영정보(충전소 ID 포함)_20230531.csv
```

###  데이터셋 설명
```
`(참고자료) 한국전력공사_전기차충전서비스운영시스템_고객센터 상담내역_코드표.xlsx : 고객 상담 분석`
`서울시 소유 충전기 일별 시간별 충전현황.xlsx : 시간대별 충전 패턴 분석`
`서울시 자치구 읍면동별 연료별 자동차 등록현황(행정동)(25년04월).xls : 전기차 등록 현황`
`소상공인시장진흥공단_상가(상권)정보_서울_202503.csv : 상업지역 밀집도 분석`
`월별 소통정보 (구간별-첨두시별).csv : 교통 패턴 분석`
`전기차 충전소 충전량 데이터_202501.xlsx : 실제 충전량 기반 수요 계산`
`전기차 충전소 충전량 데이터_202502.xlsx : 실제 충전량 기반 수요 계산`
`전기차 충전소 충전량 데이터_202503.xlsx : 실제 충전량 기반 수요 계산`
`한국전력공사_전기차충전서비스운영시스템_고객센터 상담 내역_20241231.csv: 고객 상담 분석`
`한국환경공단_전기차 충전소 위치 및 운영정보(충전소 ID 포함)_20230531.csv : 기존 충전소 위치 분석`
```

## 기능 구조 (Project Structure)

```
2025-1-Big-Data-Programming/
├── README.md
├── requirements.txt
│
├── data/
│   ├── raw/                     # 원본 데이터
│   ├── processed/               # 전처리 완료 데이터
│   └── modeling/                # 모델링용 파생 데이터
│
├── outputs/
│   ├── eda/                     # 탐색적 데이터 분석 결과
│   ├── plots/                   # 시각화 결과
│   └── maps/                    # MCLP 결과 지도
│
├── scripts/
│   ├── run_preprocessing.py     # 전처리 실행 스크립트
│   └── main.ipynb               # 해결 프로세스 합치 노트북
│
├── src/
│   ├── preprocessing/           # 전처리 모듈
│   ├── analysis/                # 수요 예측 및 클러스터링
│   ├── modeling/                # MCLP 모델 명세
│   ├── utils/                   # 자동화 등 특수 기능
│   └── visualization/           # 시각화
└── 
```

---

## 환경 (Environment)

- **Python**: 3.12.2
- **OS**: Windows 11

---

## 패키지 및 사용 버전 (Packages)

### 특수 사용 및 버전:

#### 🔹 데이터 처리
- pandas >= 1.5.0
- numpy >= 1.24.0

#### 🔹 지도 데이터
- geopandas >= 0.13.0

#### 🔹 시각화
- matplotlib >= 3.7.0
- seaborn >= 0.12.0
- folium >= 0.14.0
- plotly >= 5.15.0

#### 🔹 모델링
- scikit-learn >= 1.3.0
- xgboost >= 1.7.0

#### 🔹 최적화
- pulp >= 2.7.0

#### 🔹 파일 처리
- openpyxl >= 3.1.0
- xlrd >= 2.0.0

#### 🔹 웹앱 (optional)
- streamlit >= 1.25.0

---

전처리 실행: `python scripts/run_preprocessing.py`

---

##  모델링 프로세스 요약 (Modeling Pipeline)

본 프로젝트의 분석 및 최적화 흐름은 다음 단계로 구성되어 있습니다:

---

### 1. 데이터 수집 및 전처리

- 서울시·환경공단·한전 등에서 수집한 전기차 등록 현황, 충전소 위치, 충전량, 상권 밀집도 등의 다양한 데이터를 정제
- 모든 데이터를 **500m × 500m 격자 단위**로 통합

---

### 2. 격자화 및 위치 매핑

- 서울시 전체를 **6030개의 격자**로 나누고, 기존 충전소 위치를 해당 격자에 매핑

---

### 3. KMeans 클러스터링

- 격자별 특성(상권, 교통, 인구 등)을 기준으로 KMeans 수행 (**최적 K=2**)
- 고수요 지역을 포함하는 클러스터만 필터링 → **526개 격자 추출**

---

### 4. XGBoost 수요 예측

- 위 526개 격자의 수요를 **회귀 기반**으로 예측
- 사용 특성: 공급 점수, 인구밀도, 교통 접근성, 상업 점포 수 등

#### 예측 성능:

- **MAE**: 12.89  
- **RMSE**: 61.18  
- **R²**: 0.9798

---

### 5. MCLP (Maximum Coverage Location Problem) 최적화

- 목표: **제한된 설치 수(280개)**와 **커버 반경(300m)** 내에서 예측 수요 최대 커버
- **선형계획 최적화(PuLP)** 활용

#### 최적 결과:

- **커버 수요**: 491,258.41 / 전체 예측 수요: 1,026,048.11  
- **커버율**: 47.88%  
- **설치 효율**: 1,754.49

---

### 6. 전략별 비교 분석 및 시각화

- 기존 설치, 랜덤, 클러스터링, MCLP 전략 간 **커버 성능 비교**
- 신규 커버 격자와 상위 수요 지역 중심의 **정량적 평가** 실시


---

##  성능 비교 및 결과 (Evaluation)

| 설치 전략             | 설치 수 | 커버 수요     | 전체 수요     | 커버율 (%) | 설치 효율 |
|----------------------|---------|---------------|---------------|-------------|-------------|
| 기존 충전소 전체      | 1,753   | 978,846.38     | 1,026,048.11   | 95.40%      | 558.38       |
| 랜덤 설치             | 526     | 299,406.57     | 1,026,048.11   | 29.18%      | 569.21       |
| 클러스터 기반 설치     | 526     | 702,554.73     | 1,026,048.11   | 68.47%      | 1,335.66      |
| **MCLP 추천 설치**     | **280** | **491,258.41** | **1,026,048.11** | **47.88%**  | **1,754.49**   |

---

### 📌 해석

- **MCLP 추천 설치 전략**은 설치 수가 가장 적음에도 불구하고 **설치 효율(1,754.49)**이 가장 높아, 정확한 **수요 기반 최적 입지**임을 입증했습니다.
- **클러스터 기반 전략**은 비교적 적은 설치 수로 높은 커버율(68.47%)을 달성했지만, **MCLP보다는 비효율적**입니다.
- **기존 충전소 전략**은 매우 높은 커버율(95.40%)을 보였으나, 많은 설치 수를 요구하며 **설치 효율은 낮았습니다**.
- **랜덤 설치 전략**은 기대 이하의 성능으로, **수요 기반 접근의 중요성**을 반증합니다.
