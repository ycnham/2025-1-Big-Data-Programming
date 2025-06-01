# 데이터 전처리 - 환경 설정 및 라이브러리 임포트

import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import warnings
from datetime import datetime
import os
import glob

# 프로젝트 환경 설정
def setup_environment():
    
    # 한글 폰트 설정
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # 경고 메시지 숨기기
    warnings.filterwarnings('ignore')
    
    # 데이터 출력 옵션 설정
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', None)
    
    print("라이브러리 임포트 완료")
    print("데이터 전처리를 시작합니다.")

# 프로젝트 디렉토리 구조 생성
def create_project_directories():
    directories = ['data/raw', 'data/processed', 'outputs/plots', 'outputs/maps']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    print("프로젝트 디렉토리 구조 생성 완료")

if __name__ == "__main__":
    setup_environment()
    create_project_directories()