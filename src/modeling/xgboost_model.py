import pandas as pd
import numpy as np
import xgboost as xgb
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

def train_and_predict(
    df: pd.DataFrame,
    features: list,
    label: str = 'demand_score',
    n_estimators: int = 100,
    test_size: float = 0.2,
    random_state: int = 42,
    verbose: bool = True
) -> tuple[pd.DataFrame, dict]:
    """
    XGBoost 회귀 모델을 학습하고, 예측값과 성능 지표를 반환합니다.

    Parameters:
    - df (pd.DataFrame): 전체 입력 데이터
    - features (list): 모델에 사용할 입력 변수(feature) 리스트
    - label (str): 예측할 대상 변수 (기본값: 'demand_score')
    - n_estimators (int): XGBoost의 트리 개수 (기본값: 100)
    - test_size (float): 테스트 데이터셋의 비율 (기본값: 0.2)
    - random_state (int): 데이터 분할 및 모델 재현성을 위한 시드 값
    - verbose (bool): 성능 지표 출력 여부

    Returns:
    - df (pd.DataFrame): 예측 결과가 추가된 원본 데이터프레임
    - metrics (dict): 모델 평가 지표 (MAE, RMSE, R2)
    - model (XGBRegressor): 학습된 XGBoost 모델 객체
    """

    # 결측 제거 후 valid subset
    valid_rows = df[features + [label]].dropna()
    X = valid_rows[features]
    y = valid_rows[label]

    # train/test 분할
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # 모델 학습
    model = XGBRegressor(
        n_estimators=n_estimators,
        random_state=random_state,
        enable_categorical=True
    )
    model.fit(X_train, y_train)

    # 성능 평가
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # 전체 예측 결과 저장
    df['predicted_demand_score'] = np.nan
    df.loc[valid_rows.index, 'predicted_demand_score'] = model.predict(X)

    if verbose:
        print("XGBoost 성능:")
        print(f"MAE: {mae:.2f}")
        print(f"RMSE: {rmse:.2f}")
        print(f"R²: {r2:.4f}")
        
    return df, {
        "MAE": round(mae, 2), 
        "RMSE": round(rmse, 2), 
        "R2": round(r2, 4)
        }, model