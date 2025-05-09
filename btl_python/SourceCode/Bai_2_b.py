import pandas as pd
import numpy as np
from typing import Dict, List, Union

def convert_age_to_years(age_str: Union[str, float]) -> float:
    if pd.isna(age_str) or not isinstance(age_str, str):
        return np.nan
    
    try:
        years, days = map(int, age_str.split('-'))
        return years + (days / 365.25)
    except (ValueError, AttributeError):
        return np.nan

def load_and_preprocess_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df.replace(['N/A'], np.nan, inplace=True)
    
    if 'Age' in df.columns:
        df['Age'] = df['Age'].apply(convert_age_to_years)
    
    return df

def get_column_categories(df: pd.DataFrame) -> Dict[str, List[str]]:
    non_stat_cols = ["Name", "Nation", "Team", "Position"]
    stat_cols = [col for col in df.columns 
                if col not in non_stat_cols 
                and pd.api.types.is_numeric_dtype(df[col])]
    
    return {'statistical': stat_cols, 'non_statistical': non_stat_cols}

def compute_aggregates(df: pd.DataFrame, stat_cols: List[str], group_col: str = None) -> pd.DataFrame:
    metrics = ['mean', 'median', 'std']
    
    if group_col:
        agg_df = df.groupby(group_col)[stat_cols].agg(metrics)
        agg_df.columns = [f"{metric.capitalize()} of {col}" 
                         for col, metric in agg_df.columns]
    else:
        agg_df = pd.DataFrame(
            {f"{metric.capitalize()} of {col}": df[col].agg(metric)
             for col in stat_cols for metric in metrics},
            index=['All']
        )
    
    return agg_df

def generate_analysis_report(input_file: str, output_file: str) -> None:
    data = load_and_preprocess_data(input_file)
    cols = get_column_categories(data)
    stat_cols = cols['statistical']
    
    num_cols = [col for col in stat_cols if col != 'Age']
    data[num_cols] = data[num_cols].apply(pd.to_numeric, errors='coerce')
    
    global_stats = compute_aggregates(data, stat_cols)
    team_stats = compute_aggregates(data, stat_cols, group_col="Team")
    
    final_report = pd.concat([global_stats, team_stats])
    final_report.index.name = 'Team'
    
    final_report.to_csv(output_file)
    print(f"Analysis report saved to {output_file}")

if __name__ == "__main__":
    generate_analysis_report("results.csv", "results2.csv")