import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union

def parse_age_string(age_str: str) -> Optional[float]:
    if not isinstance(age_str, str):
        return None
    try:
        years, days = map(int, age_str.split('-'))
        return years + days / 365.25 
    except (ValueError, AttributeError):
        return None

def load_and_clean_data(file_path: str) -> pd.DataFrame:
    data = pd.read_csv(file_path)
    data.replace(["N/a"], np.nan, inplace=True)
    return data

def prepare_statistical_data(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()
    df['AgeString'] = df['Age'] 
    df['Age'] = df['Age'].apply(parse_age_string)
    
    non_stat_columns = ["Name", "Team", "Nation", "Position", "AgeString"]
    stat_columns = [col for col in df.columns if col not in non_stat_columns]
    
    for col in stat_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df, non_stat_columns, stat_columns

def initialize_output_format() -> Dict[str, int]:
    return {
        'Name': 28,
        'Nation': 10,
        'Team': 20,
        'Position': 12,
    }

def generate_metric_header(col_widths: Dict[str, int], metric: str) -> str:
    return (
        f"{'Name':<{col_widths['Name']}} "
        f"{'Nation':<{col_widths['Nation']}} "
        f"{'Team':<{col_widths['Team']}} "
        f"{'Position':<{col_widths['Position']}} "
        f"{metric}"
    )


def format_data_row(row: pd.Series, col_widths: Dict[str, int], column: str) -> str:
    display_value = row['AgeString'] if column == 'Age' else f"{row[column]:.2f}"
    return (
        f"{row['Name']:<{col_widths['Name']}} "
        f"{row['Nation']:<{col_widths['Nation']}} "
        f"{row['Team']:<{col_widths['Team']}} "
        f"{row['Position']:<{col_widths['Position']}} "
        f"{display_value}"
    )

def process_metric(
    df: pd.DataFrame,
    metric: str,
    display_cols: List[str],
    col_widths: Dict[str, int]
) -> List[str]:
    output_lines = []
    
    if metric not in df.columns or not pd.api.types.is_numeric_dtype(df[metric]):
        return output_lines
    
    cols_needed = display_cols + ['AgeString'] if metric == 'Age' else display_cols + [metric]
    valid_data = df[cols_needed].dropna(subset=[metric])
    
    if valid_data.empty:
        return output_lines
    
    try:
        top_performers = valid_data.nlargest(3, metric)
        bottom_performers = valid_data.nsmallest(3, metric)
    except Exception as e:
        print(f"Error processing {metric}: {e}")
        return output_lines
    
    output_lines.append(f"╞{'═'*40} {metric} {'═'*40}╡")
    output_lines.append("TOP 3:")
    output_lines.append(generate_metric_header(col_widths, metric))
    output_lines.extend(format_data_row(row, col_widths, metric) for _, row in top_performers.iterrows())
    
    output_lines.append("BOTTOM 3:")
    output_lines.append(generate_metric_header(col_widths, metric))
    output_lines.extend(format_data_row(row, col_widths, metric) for _, row in bottom_performers.iterrows())
    output_lines.append('-' * 98)
    
    return output_lines

def save_report(content: List[str], filename: str) -> bool:
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))
        return True
    except Exception as e:
        print(f"Error saving report: {e}")
        return False

def generate_performance_report(input_file: str, output_file: str) -> None:
    raw_data = load_and_clean_data(input_file)
    processed_data, id_columns, stat_columns = prepare_statistical_data(raw_data)
    
    column_widths = initialize_output_format()
    display_columns = ["Name", "Nation", "Team", "Position", "Age"]
    report_content = []
    
    for metric in stat_columns:
        metric_results = process_metric(
            processed_data,
            metric,
            display_columns,
            column_widths
        )
        if metric_results:
            report_content.extend(metric_results)
            report_content.append('')
    
    if save_report(report_content, output_file):
        print(f"Performance report successfully saved to {output_file}")
    else:
        print("Failed to save performance report")

if __name__ == "__main__":
    generate_performance_report("results.csv", "top_3.txt")