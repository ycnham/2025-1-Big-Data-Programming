import pandas as pd

def analyze_new_coverage(features_df, strategy_sets, base_label="기존 충전소 전체", demand_col="predicted_demand_score", verbose=True):
    total_demand = features_df[demand_col].sum()
    uncovered = set(features_df['grid_id']) - strategy_sets[base_label]

    results = {}
    for label, grid_ids in strategy_sets.items():
        if label == base_label:
            continue
        new_coverage = grid_ids & uncovered
        demand = features_df[features_df['grid_id'].isin(new_coverage)][demand_col].sum()
        results[label] = {
            "new_grids": new_coverage,
            "count": len(new_coverage),
            "covered_demand": demand
        }
        if verbose:
            print(f"{label}가 새롭게 커버한 격자 수: {len(new_coverage)}")
            print(f"{label}가 새롭게 커버한 수요: {demand:,.2f}")
    return results, uncovered

def compute_percentiles(features_df, grid_ids, demand_col='predicted_demand_score', label='전략'):
    sorted_df = features_df.sort_values(by=demand_col, ascending=False).reset_index(drop=True)
    sorted_df['rank'] = sorted_df.index + 1
    sorted_df['percentile'] = sorted_df['rank'] / len(sorted_df) * 100

    selected = sorted_df[sorted_df['grid_id'].isin(grid_ids)].copy()
    selected['strategy'] = label
    return selected[['grid_id', demand_col, 'rank', 'percentile', 'strategy']]
