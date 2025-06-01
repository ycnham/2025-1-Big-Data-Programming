# 2025-1-Big-Data-Programming

데이터 기반 전기차 충전소 최적 위치 선정 및 추천 시스템 - G조

프로젝트 구조

2025-1-Big-Data-Programming/
|
├── README.md # 프로젝트 소개 및 구조
|
├── requirements.txt # Python 패키지 의존성
|
├── data/ # 모든 데이터 파일들
│ ├── raw/ # 원본 데이터 (수정 금지)
│ └── processed/ # 전처리된 데이터 (csv, json)
|
├── src/ # 핵심 소스 코드
│ ├── analysis/ # 분석 관련 모듈  
│ ├── modeling/ # 머신러닝 모델링
| ├── preprocessing/ # 데이터 전처리 모듈
│ ├── utils/ # 데이터 검증 단계 관련 코드
| └── visualization/ # 지리적 분석 단계 관련 코드
|
├── notebooks/ # Jupyter 노트북 파일들
|
├── outputs/ # 결과물 저장소
| ├── eda/ # 탐색적 데이터 분석 결과 (그래프, 요약 등))
│ ├── plots/ # 생성된 그래프들
│ └── maps/ # 지도 시각화 파일들
|
├── docs/ # 프로젝트 문서들
|
└── scripts/ # 전처리 및 분석 프로세스 실행 스크립트
