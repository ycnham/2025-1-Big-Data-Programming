"""
전체 전처리 및 분석 프로세스 실행 스크립트
"""

import sys
import os

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
        from src.analysis.eda import run_complete_analysis
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
    """4단계: 탐색적 데이터 분석"""
    print("\n" + "="*50)
    print("4️⃣ 탐색적 데이터 분석 단계")
    print("="*50)
    
    try:
        from src.analysis.eda import run_complete_analysis
        run_complete_analysis()
        print("✅ EDA 완료")
        return True
    except Exception as e:
        print(f"❌ EDA 실패: {e}")
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

def run_validation_phase():
    """6단계: 데이터 검증"""
    print("\n" + "="*50)
    print("6️⃣ 데이터 검증 단계")
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
        results['setup'] = run_setup_phase()
    else:
        print("\n⚠️ 환경 설정 모듈이 없어 건너뜁니다.")
        results['setup'] = False
    
    # 2. 데이터 로딩
    if modules_status.get('data_loader', False):
        results['data_loading'] = run_data_loading_phase()
    else:
        print("\n⚠️ 데이터 로더 모듈이 없어 건너뜁니다.")
        results['data_loading'] = None
    
    # 3. 데이터 전처리
    if modules_status.get('data_cleaner', False):
        results['preprocessing'] = run_preprocessing_phase()
    else:
        print("\n⚠️ 데이터 전처리 모듈이 없어 건너뜁니다.")
        results['preprocessing'] = None
    
    # 4. 탐색적 데이터 분석
    if modules_status.get('eda', False):
        results['eda'] = run_eda_phase()
    else:
        print("\n⚠️ EDA 모듈이 없어 건너뜁니다.")
        results['eda'] = False
    
    # 5. 지리적 분석
    if modules_status.get('geographic_viz', False):
        results['geographic'] = run_geographic_phase()
    else:
        print("\n⚠️ 지리적 분석 모듈이 없어 건너뜁니다.")
        results['geographic'] = False
    
    # 6. 데이터 검증
    if modules_status.get('data_validation', False):
        results['validation'] = run_validation_phase()
    else:
        print("\n⚠️ 데이터 검증 모듈이 없어 건너뜁니다.")
        results['validation'] = False
    
    # 결과 요약
    print("\n" + "="*60)
    print("📋 실행 결과 요약")
    print("="*60)
    
    success_count = 0
    total_count = 0
    
    for phase, result in results.items():
        total_count += 1
        if result:
            success_count += 1
            status = "✅ 성공"
        else:
            status = "❌ 실패 또는 건너뜀"
        
        print(f"{phase}: {status}")
    
    print(f"\n📊 성공률: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    
    if success_count == total_count:
        print("\n🎉 모든 프로세스가 성공적으로 완료되었습니다!")
    else:
        print(f"\n⚠️ 일부 프로세스에서 문제가 발생했습니다.")
        print("개별 모듈을 직접 실행해보시기 바랍니다.")

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

if __name__ == "__main__":
    # 프로젝트 구조 확인
    check_project_structure()
    
    # 메인 프로세스 실행
    main()
