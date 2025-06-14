import pandas as pd

def evaluate_strategy(label: str, selected_grids: set, df: pd.DataFrame, demand_col: str = 'predicted_demand_score'):
    total_demand = df[demand_col].sum()
    matched_df = df[df['grid_id'].isin(selected_grids)].copy()
    covered_demand = matched_df[demand_col].sum()
    coverage_rate = covered_demand / total_demand * 100
    efficiency = covered_demand / len(selected_grids) if selected_grids else 0

    print(f"\n[{label}]")
    print(f"- 설치 격자 수: {len(selected_grids)}")
    print(f"- 커버 수요: {covered_demand:,.2f}")
    print(f"- 전체 수요: {total_demand:,.2f}")
    print(f"- 커버율: {coverage_rate:.2f}%")
    print(f"- 설치 1개당 커버 수요 (효율): {efficiency:,.2f}")
