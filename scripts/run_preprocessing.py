"""
전체 전처리 및 분석 프로세스 실행 스크립트
"""

import sys
import os
from pathlib import Path
import pandas as pd

# 프로젝트 루트 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

print(f"프로젝트 루트: {project_root}")
print(f"Python 경로에 추가됨: {project_root}")

# 🔍 디버깅 정보 추가
print("\n" + "="*60)
print("🔍 디버깅 정보")
print("="*60)

print(f"현재 작업 디렉토리: {os.getcwd()}")
print(f"스크립트 위치: {current_dir}")
print(f"계산된 프로젝트 루트: {project_root}")

# 중요한 경로들 존재 여부 확인
important_paths = [
    'src',
    'src/analysis',
    'src/analysis/eda.py',
    'src/analysis/__init__.py',
    'src/preprocessing',
    'src/preprocessing/setup.py',
    'src/preprocessing/__init__.py'
]

print(f"\n📁 중요한 경로 존재 여부:")
for path in important_paths:
    full_path = os.path.join(project_root, path)
    exists = os.path.exists(full_path)
    print(f"  {'✅' if exists else '❌'} {path} -> {full_path}")

print(f"\n🐍 Python 경로 (상위 10개):")
for i, path in enumerate(sys.path[:10]):
    print(f"  {i+1}. {path}")

# src 폴더가 인식되는지 확인
src_path = os.path.join(project_root, 'src')
if src_path in sys.path:
    print(f"\n✅ src 폴더가 Python 경로에 있습니다")
else:
    print(f"\n⚠️ src 폴더를 Python 경로에 추가합니다")
    sys.path.insert(0, src_path)

# 실제 import 테스트
print(f"\n🧪 실제 import 테스트:")

# 1. src 모듈 테스트
try:
    import src
    print("✅ src 모듈 import 성공")
    print(f"   src 모듈 위치: {src.__file__ if hasattr(src, '__file__') else 'N/A'}")
except ImportError as e:
    print(f"❌ src 모듈 import 실패: {e}")

# 2. src.analysis 테스트
try:
    import src.analysis
    print("✅ src.analysis 모듈 import 성공")
except ImportError as e:
    print(f"❌ src.analysis 모듈 import 실패: {e}")

# 3. src.analysis.eda 테스트
try:
    import src.analysis.eda
    print("✅ src.analysis.eda 모듈 import 성공")
except ImportError as e:
    print(f"❌ src.analysis.eda 모듈 import 실패: {e}")

# 4. 특정 함수 import 테스트
try:
    from src.analysis.eda import run_complete_analysis
    print("✅ run_complete_analysis 함수 import 성공")
except ImportError as e:
    print(f"❌ run_complete_analysis 함수 import 실패: {e}")

print("="*60)
print("디버깅 정보 끝")
print("="*60)

# 모듈 import (안전한 방식)
def safe_import():
    """안전한 모듈 import"""
    modules = {}
    
    try:
        # 1. 환경 설정 모듈
        from src.preprocessing.setup import setup_environment, create_project_directories
        modules['setup'] = True
        print("✅ setup 모듈 import 성공")
    except ImportError as e:
        print(f"❌ setup 모듈 import 실패: {e}")
        modules['setup'] = False
    
    try:
        # 2. 데이터 로더 모듈
        from src.preprocessing.data_loader import load_all_datasets
        modules['data_loader'] = True
        print("✅ data_loader 모듈 import 성공")
    except ImportError as e:
        print(f"❌ data_loader 모듈 import 실패: {e}")
        modules['data_loader'] = False
    
    try:
        # 3. 데이터 클리너 모듈
        from src.preprocessing.data_cleaner import run_all_preprocessing
        modules['data_cleaner'] = True
        print("✅ data_cleaner 모듈 import 성공")
    except ImportError as e:
        print(f"❌ data_cleaner 모듈 import 실패: {e}")
        modules['data_cleaner'] = False
    
    try:
        # 4. EDA 모듈
        from src.analysis.eda import run_complete_analysis, EDAAnalyzer
        modules['eda'] = True
        print("✅ eda 모듈 import 성공")
    except ImportError as e:
        print(f"❌ eda 모듈 import 실패: {e}")
        modules['eda'] = False
    
    try:
        # 5. 지리적 시각화 모듈
        from src.visualization.geographic_viz import run_geographic_analysis
        modules['geographic_viz'] = True
        print("✅ geographic_viz 모듈 import 성공")
    except ImportError as e:
        print(f"❌ geographic_viz 모듈 import 실패: {e}")
        modules['geographic_viz'] = False
    
    try:
        # 6. 데이터 검증 모듈
        from src.utils.data_validation import run_data_validation
        modules['data_validation'] = True
        print("✅ data_validation 모듈 import 성공")
    except ImportError as e:
        print(f"❌ data_validation 모듈 import 실패: {e}")
        modules['data_validation'] = False
    
    try:
        # 7. 모델링 전처리 모듈 (새로 추가)
        from src.preprocessing.modeling_data_prep import prepare_modeling_data
        modules['modeling_prep'] = True
        print("✅ modeling_prep 모듈 import 성공")
    except ImportError as e:
        print(f"❌ modeling_prep 모듈 import 실패: {e}")
        modules['modeling_prep'] = False
    
    return modules

def run_setup_phase():
    """1단계: 환경 설정"""
    print("\n" + "="*50)
    print("1️⃣ 환경 설정 단계")
    print("="*50)
    
    try:
        from src.preprocessing.setup import setup_environment, create_project_directories
        setup_environment()
        create_project_directories()
        print("✅ 환경 설정 완료")
        return True
    except Exception as e:
        print(f"❌ 환경 설정 실패: {e}")
        return False

def run_data_loading_phase():
    """2단계: 데이터 로딩"""
    print("\n" + "="*50)
    print("2️⃣ 데이터 로딩 단계")
    print("="*50)
    
    try:
        from src.preprocessing.data_loader import load_all_datasets
        datasets = load_all_datasets()
        print("✅ 데이터 로딩 완료")
        return datasets
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        return None

def run_preprocessing_phase():
    """3단계: 데이터 전처리"""
    print("\n" + "="*50)
    print("3️⃣ 데이터 전처리 단계")
    print("="*50)
    
    try:
        from src.preprocessing.data_cleaner import run_all_preprocessing
        processed_data = run_all_preprocessing()
        print("✅ 데이터 전처리 완료")
        return processed_data
    except Exception as e:
        print(f"❌ 데이터 전처리 실패: {e}")
        return None

def run_eda_phase():
    """4단계: 탐색적 데이터 분석 (EDA) - 자동 실행"""
    print("\n" + "="*50)
    print("4️⃣ 탐색적 데이터 분석 (EDA) 단계")
    print("="*50)
    print("📊 전처리 완료 후 자동으로 EDA를 실행합니다...")

    try:
        # 방법 1: run_complete_analysis 함수 사용
        from src.analysis.eda import run_complete_analysis
        run_complete_analysis()
        print("✅ EDA (방법 1) 완료")
        return True
    except Exception as e1:
        print(f"⚠️ EDA 방법 1 실패: {e1}")
        
        try:
            # 방법 2: EDAAnalyzer 클래스 직접 사용
            from src.analysis.eda import EDAAnalyzer
            analyzer = EDAAnalyzer()
            analyzer.run_comprehensive_eda()
            print("✅ EDA (방법 2) 완료")
            return True
        except Exception as e2:
            print(f"❌ EDA 완전 실패:")
            print(f"   방법 1 오류: {e1}")
            print(f"   방법 2 오류: {e2}")
            print("💡 EDA를 수동으로 실행하려면 다음 명령어를 사용하세요:")
            print("   from src.analysis.eda import EDAAnalyzer")
            print("   analyzer = EDAAnalyzer()")
            print("   analyzer.run_comprehensive_eda()")
            return False

def run_geographic_phase():
    """5단계: 지리적 분석"""
    print("\n" + "="*50)
    print("5️⃣ 지리적 분석 단계")
    print("="*50)
    
    try:
        from src.visualization.geographic_viz import run_geographic_analysis
        run_geographic_analysis()
        print("✅ 지리적 분석 완료")
        return True
    except Exception as e:
        print(f"❌ 지리적 분석 실패: {e}")
        return False

def run_modeling_prep_phase():
    """6단계: 모델링 데이터 전처리 (새로 추가)"""
    print("\n" + "="*50)
    print("6️⃣ 모델링 데이터 전처리 단계")
    print("="*50)
    
    try:
        print("📊 모델링용 데이터 준비 중...")
        
        # 모델링 전처리 모듈 import
        from src.preprocessing.modeling_data_prep import prepare_modeling_data
        
        print("✅ 모델링 전처리 모듈 import 성공")
        
        # 모델링 전처리 실행
        prepare_modeling_data()
        
        print("✅ 모델링 데이터 전처리 완료")
        
        # 생성된 파일 확인
        print("\n📋 모델링 파일 생성 확인:")
        processed_dir = Path(project_root) / 'data' / 'processed'
        modeling_files = [
            'grid_features.csv',
            'demand_supply_analysis.csv',
            'optimal_locations.csv'
        ]
        
        modeling_success = True
        for file in modeling_files:
            file_path = processed_dir / file
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    print(f"✅ {file}: {len(df):,}행, {len(df.columns)}컬럼")
                    
                    # 각 파일별 간단한 요약 정보
                    if file == 'grid_features.csv':
                        print(f"   💡 격자 특성 데이터: 수요/공급/상업시설 정보")
                    elif file == 'demand_supply_analysis.csv':
                        print(f"   💡 수요-공급 분석 데이터: 불균형 지역 식별")
                    elif file == 'optimal_locations.csv':
                        print(f"   💡 최적 위치 데이터: 상위 충전소 설치 후보지")
                        
                except Exception as e:
                    print(f"⚠️ {file}: 파일 읽기 오류 - {e}")
                    modeling_success = False
            else:
                print(f"❌ {file}: 생성되지 않음")
                modeling_success = False
        
        if modeling_success:
            print("\n🎉 모든 모델링 데이터 준비 완료!")
            print("💡 다음 단계: 머신러닝 모델 학습 및 예측")
            return True
        else:
            print("\n⚠️ 일부 모델링 파일 생성에 문제가 있습니다.")
            return False
            
    except ImportError as e:
        print(f"❌ 모델링 전처리 모듈 import 실패: {e}")
        print("💡 modeling_data_prep.py 파일이 src/preprocessing/ 폴더에 있는지 확인하세요.")
        return False
        
    except Exception as e:
        print(f"❌ 모델링 전처리 실패: {e}")
        print("상세 오류:")
        import traceback
        print(traceback.format_exc())
        return False

def run_validation_phase():
    """7단계: 데이터 검증"""
    print("\n" + "="*50)
    print("7️⃣ 데이터 검증 단계")
    print("="*50)
    
    try:
        from src.utils.data_validation import run_data_validation
        run_data_validation()
        print("✅ 데이터 검증 완료")
        return True
    except Exception as e:
        print(f"❌ 데이터 검증 실패: {e}")
        return False

def main():
    """전체 프로세스 실행"""
    print("🚀 전기차 충전소 최적 위치 선정 프로젝트 시작!")
    print("📋 실행 순서: 환경설정 → 데이터로딩 → 전처리 → EDA → 지리적분석 → 모델링전처리 → 검증")
    print("="*60)
    
    # 모듈 import 상태 확인
    print("\n📦 모듈 import 상태 확인...")
    modules_status = safe_import()
    
    failed_modules = [name for name, status in modules_status.items() if not status]
    if failed_modules:
        print(f"\n⚠️ 실패한 모듈들: {', '.join(failed_modules)}")
        print("일부 단계가 건너뛰어질 수 있습니다.")
    else:
        print("\n✅ 모든 모듈 import 성공!")
    
    results = {}
    
    # 1. 환경 설정
    if modules_status.get('setup', False):
        results['setup'] = '✅ 성공' if run_setup_phase() else '❌ 실패'
    else:
        print("\n⚠️ 환경 설정 모듈이 없어 건너뜁니다.")
        results['setup'] = '⚠️ 건너뜀'
    
    # 2. 데이터 로딩
    if modules_status.get('data_loader', False):
        data_result = run_data_loading_phase()
        results['data_loading'] = '✅ 성공' if data_result else '❌ 실패'
    else:
        print("\n⚠️ 데이터 로더 모듈이 없어 건너뜁니다.")
        results['data_loading'] = '⚠️ 건너뜀'
    
    # 3. 데이터 전처리
    if modules_status.get('data_cleaner', False):
        preprocessing_result = run_preprocessing_phase()
        results['preprocessing'] = '✅ 성공' if preprocessing_result else '❌ 실패'
    else:
        print("\n⚠️ 데이터 전처리 모듈이 없어 건너뜁니다.")
        results['preprocessing'] = '⚠️ 건너뜀'
    
    # 4. 탐색적 데이터 분석
    if modules_status.get('eda', False):
        print("\n🔄 전처리 완료! 이제 EDA를 자동으로 실행합니다...")
        eda_result = run_eda_phase()
        results['eda'] = '✅ 성공' if eda_result else '❌ 실패'
        
        if eda_result:
            print("\n🎯 EDA 결과 확인:")
            print("   📂 EDA 결과: outputs/eda/")
            print("   📊 시각화 차트: outputs/eda/*.png")
            print("   📝 인사이트 리포트: outputs/eda/eda_insights.txt")
    else:
        print("\n⚠️ EDA 모듈이 없어 건너뜁니다.")
        print("💡 EDA를 수동으로 실행하려면:")
        print("   python -c \"from src.analysis.eda import EDAAnalyzer; EDAAnalyzer().run_comprehensive_eda()\"")
        results['eda'] = '⚠️ 건너뜀'
    
    # 5. 지리적 분석
    if modules_status.get('geographic_viz', False):
        geographic_result = run_geographic_phase()
        results['geographic'] = '✅ 성공' if geographic_result else '❌ 실패'
    else:
        print("\n⚠️ 지리적 분석 모듈이 없어 건너뜁니다.")
        results['geographic'] = '⚠️ 건너뜀'
    
    # 6. 모델링 데이터 전처리 (새로 추가)
    if modules_status.get('modeling_prep', False):
        modeling_result = run_modeling_prep_phase()
        results['modeling_prep'] = '✅ 성공' if modeling_result else '❌ 실패'
    else:
        print("\n⚠️ 모델링 전처리 모듈이 없어 건너뜁니다.")
        print("💡 모델링 전처리를 별도로 실행하려면:")
        print("   python run_modeling_prep.py")
        results['modeling_prep'] = '⚠️ 건너뜀'
    
    # 7. 데이터 검증
    if modules_status.get('data_validation', False):
        validation_result = run_validation_phase()
        results['validation'] = '✅ 성공' if validation_result else '❌ 실패'
    else:
        print("\n⚠️ 데이터 검증 모듈이 없어 건너뜀니다.")
        results['validation'] = '⚠️ 건너뜀'
    
    # 실행 결과 요약 (개선된 버전)
    print("\n" + "="*60)
    print("📋 실행 결과 요약")
    print("="*60)
    
    total_steps = len(results)
    success_count = sum(1 for status in results.values() if '✅' in status)
    
    for step, status in results.items():
        print(f"{step}: {status}")
    
    print(f"\n📊 성공률: {success_count}/{total_steps} ({success_count/total_steps*100:.1f}%)")
    
    if success_count == total_steps:
        print("\n🎉 모든 전처리 단계가 성공적으로 완료되었습니다!")
        print("💡 다음 단계: EDA 분석 결과 확인 및 모델링 진행")
        print("\n📂 생성된 결과물:")
        print("   • 전처리된 데이터: data/processed/")
        print("   • EDA 결과: outputs/eda/")
        print("   • 모델링 데이터: data/processed/grid_features.csv")
        print("   • 최적 위치: data/processed/optimal_locations.csv")
        
    elif success_count >= total_steps * 0.8:  # 80% 이상 성공
        print("\n✅ 대부분의 전처리가 완료되었습니다.")
        print("⚠️ 일부 문제가 있는 단계를 점검해주세요.")
        
        # 성공한 핵심 단계들 강조
        if results.get('preprocessing') == '✅ 성공':
            print("   ✅ 핵심 전처리 완료")
        if results.get('modeling_prep') == '✅ 성공':
            print("   ✅ 모델링 데이터 준비 완료")
    else:
        print("\n⚠️ 일부 프로세스에서 문제가 발생했습니다.")
        print("개별 모듈을 직접 실행해보시기 바랍니다.")
        
        # 실패한 단계별 가이드 제공
        failed_steps = [step for step, status in results.items() if '❌' in status]
        if failed_steps:
            print("\n🔧 실패한 단계별 해결 가이드:")
            for step in failed_steps:
                if step == 'eda':
                    print("   • EDA: python -c \"from src.analysis.eda import EDAAnalyzer; EDAAnalyzer().run_comprehensive_eda()\"")
                elif step == 'modeling_prep':
                    print("   • 모델링 전처리: python run_modeling_prep.py")
                elif step == 'preprocessing':
                    print("   • 데이터 전처리: 개별 데이터셋 확인 필요")
                elif step == 'geographic':
                    print("   • 지리적 분석: geographic_viz 모듈 확인 필요")
    
    processed_dir = Path(project_root) / 'data' / 'processed'
    print(f"\n📁 처리된 데이터 위치: {processed_dir}")
    print("🔍 로그 및 결과물을 확인하여 다음 단계를 진행하세요.")

def check_project_structure():
    """프로젝트 구조 확인"""
    print("📁 프로젝트 구조 확인...")
    
    required_dirs = [
        'src',
        'src/preprocessing',
        'src/analysis',
        'src/visualization',
        'src/utils',
        'data',
        'data/raw',
        'data/processed',
        'outputs',
        'outputs/plots',
        'outputs/maps'
    ]
    
    missing_dirs = []
    for directory in required_dirs:
        if not os.path.exists(directory):
            missing_dirs.append(directory)
    
    if missing_dirs:
        print(f"❌ 누락된 디렉토리: {missing_dirs}")
        print("필요한 디렉토리를 생성하시기 바랍니다.")
    else:
        print("✅ 모든 필수 디렉토리가 존재합니다.")
    
    # __init__.py 파일 확인
    init_files = [
        'src/__init__.py',
        'src/preprocessing/__init__.py',
        'src/analysis/__init__.py',
        'src/visualization/__init__.py',
        'src/utils/__init__.py'
    ]
    
    missing_init = []
    for init_file in init_files:
        if not os.path.exists(init_file):
            missing_init.append(init_file)
    
    if missing_init:
        print(f"⚠️ 누락된 __init__.py 파일: {missing_init}")
        print("빈 __init__.py 파일들을 생성하는 것을 권장합니다.")

# 개별 실행 함수들
def run_only_eda():
    """EDA만 개별 실행하는 함수"""
    print("📊 EDA만 개별 실행합니다...")
    
    try:
        from src.analysis.eda import EDAAnalyzer
        analyzer = EDAAnalyzer()
        result = analyzer.run_comprehensive_eda()
        print("✅ EDA 개별 실행 완료!")
        return result
    except Exception as e:
        print(f"❌ EDA 개별 실행 실패: {e}")
        return None
    
def run_preprocessing_only():
    """전처리만 실행하는 함수"""
    print("🔧 전처리만 실행합니다...")

    try:
        from src.preprocessing.data_cleaner import run_all_preprocessing
        result = run_all_preprocessing()
        print("✅ 전처리 완료!")
        return result
    except Exception as e:
        print(f"❌ 전처리 실패: {e}")
        return None

def run_modeling_only():
    """모델링 전처리만 실행하는 함수"""
    print("🔧 모델링 전처리만 실행합니다...")
    
    try:
        from src.preprocessing.modeling_data_prep import prepare_modeling_data
        result = prepare_modeling_data()
        print("✅ 모델링 전처리 완료!")
        return result
    except Exception as e:
        print(f"❌ 모델링 전처리 실패: {e}")
        return None
    
if __name__ == "__main__":
    # 프로젝트 구조 확인
    check_project_structure()
    
    # 메인 프로세스 실행
    main()
