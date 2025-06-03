import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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
    XGBoost 회귀 모델 학습 및 예측 수행

    Parameters:
    - df: 전체 데이터프레임
    - features: 입력 변수 컬럼명 리스트
    - label: 예측 대상 컬럼명
    - n_estimators: 트리 개수
    - test_size: 검증 데이터 비율
    - random_state: 랜덤 시드
    - verbose: 평가 지표 출력 여부

    Returns:
    - df: 예측 결과가 추가된 DataFrame
    - metrics: 성능 지표 딕셔너리 (MAE, RMSE, R2)
    """

    # X, y 추출
    X = df[features].copy()
    y = df[label].copy()

    # 결측치 제거
    valid_idx = X.dropna().index
    X = X.loc[valid_idx]
    y = y.loc[valid_idx]

    # train/test 분할
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # 모델 학습
    model = XGBRegressor(n_estimators=n_estimators, random_state=random_state)
    model.fit(X_train, y_train)

    # 예측
    y_pred = model.predict(X_test)

    # 성능 평가
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    metrics = {
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "R2": round(r2, 4)
    }

    if verbose:
        print("XGBoost 성능:")
        for k, v in metrics.items():
            print(f"{k}: {v}")

    # 전체 예측 결과 추가
    df['predicted_demand_score'] = model.predict(X)

    return df, metrics