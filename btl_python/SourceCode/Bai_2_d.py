import pandas as pd
from termcolor import colored
import matplotlib.pyplot as plt

def analyze_premier_league_stats(data_path):
    try:
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        print(colored("Lỗi: Không tìm thấy file dữ liệu. Vui lòng kiểm tra đường dẫn.", 'red'))
        return
    
    print(colored("\nPHÂN TÍCH HIỆU SUẤT PREMIER LEAGUE 2024-2025", 'cyan', attrs=['bold']))
    print(colored("="*60, 'cyan'))
    
    performance_analysis = []
    mean_columns = [col for col in df.columns if col.startswith('Mean of')]
    
    weights = {
        'Goals': 1.2,
        'Possession': 0.8,
        'Shots': 1.0,
        'Pass Accuracy': 0.9,
        'Tackles': 0.7
    }
    
    df['Weighted Total Score'] = 0
    
    for col in mean_columns:
        attribute = col.replace('Mean of ', '')
        best_team = df.loc[df[col].idxmax(), 'Team']
        best_value = df[col].max()
        
        performance_analysis.append({
            'Chỉ số': attribute,
            'Đội xuất sắc nhất': best_team,
            'Giá trị trung bình': round(best_value, 2),
            'Đánh giá': '⭐' * int(best_value/20) 
        })
        
        weight = weights.get(attribute, 1.0)
        df['Weighted Total Score'] += df[col] * weight
        
        print(colored(f"🏅 {attribute.upper():<15}", 'yellow') + 
              f": {best_team:<20} ({best_value:.2f})")
    
    best_overall = df.loc[df['Weighted Total Score'].idxmax()]
    print(colored("\n🔥 KẾT QUẢ TỔNG HỢP 🔥", 'magenta', attrs=['bold']))
    print(colored(f"Đội bóng xuất sắc nhất: {best_overall['Team']}", 'green', attrs=['bold']))
    print(colored(f"Điểm tổng hợp có trọng số: {best_overall['Weighted Total Score']:.2f}", 'green'))
    
    report_df = pd.DataFrame(performance_analysis)
    detailed_report = pd.merge(report_df, df, left_on='Đội xuất sắc nhất', right_on='Team')
    
    output_path = 'Detailed_Premier_League_Analysis.csv'
    detailed_report.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(colored(f"\n📊 Báo cáo chi tiết đã được lưu tại: {output_path}", 'blue'))
    
    plot_top_teams(df, mean_columns)
    
    return detailed_report

def plot_top_teams(df, metrics):
    plt.figure(figsize=(12, 8))
    
    top_teams = df.nlargest(5, 'Weighted Total Score')
    
    for metric in metrics[:3]: 
        metric_name = metric.replace('Mean of ', '')
        plt.barh(
            top_teams['Team'] + ' - ' + metric_name,
            top_teams[metric],
            label=metric_name
        )
    
    plt.title('Top 5 Teams Performance Analysis')
    plt.xlabel('Score')
    plt.ylabel('Team and Metric')
    plt.legend()
    plt.tight_layout()
    plt.savefig('top_teams_visualization.png')
    print(colored("🖼️ Biểu đồ so sánh đã được lưu thành hình ảnh.", 'blue'))

if __name__ == "__main__":
    analyze_premier_league_stats('results2.csv')