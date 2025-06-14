import matplotlib.pyplot as plt
import xgboost as xgb
from xgboost import XGBRegressor

def plot_feature_importance(model: XGBRegressor, max_num_features: int = 10, save_path: str = None):
    """
    XGBoost feature importance 시각화 함수

    Parameters:
    - model: 학습된 XGBRegressor 객체
    - max_num_features: 시각화할 feature 개수
    - save_path: 저장할 경로 (None이면 화면 출력만)
    """
    ax = xgb.plot_importance(model, max_num_features=max_num_features)
    plt.title("Feature Importance")
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path)
        print(f"중요도 시각화 저장 완료: {save_path}")
    else:
        plt.show()
