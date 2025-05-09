import pandas as pd
import matplotlib.pyplot as plt
import os

df = pd.read_csv("results.csv")

selected_stats = ['performance_goals', 'performance_assists', 'creation_sca', 'defense_tackles', 'defense_interceptions', 'miscellaneous_performance_recoveries']

output_dir = "team_histograms"
os.makedirs(output_dir, exist_ok=True)

def plot_all_players_hist(df):
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()
    
    for i, stat in enumerate(selected_stats):
        axes[i].hist(df[stat], bins=20, color='cornflowerblue', edgecolor='black')
        axes[i].set_title(f"All Players - {stat}")
        axes[i].set_xlabel(stat)
        axes[i].set_ylabel("Number of Players")
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/all_players_hist.png")
    plt.show()
    plt.close()

plot_all_players_hist(df)

def plot_each_team_hist(df):
    teams = df['Team'].unique()
    
    for team in teams:
        team_df = df[df['Team'] == team]
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        axes = axes.flatten()

        for i, stat in enumerate(selected_stats):
            axes[i].hist(team_df[stat], bins=10, color='salmon', edgecolor='black')
            axes[i].set_title(f"{team} - {stat}")
            axes[i].set_xlabel(stat)
            axes[i].set_ylabel("Players")

        plt.tight_layout()
        plt.suptitle(f"{team} - Player Stat Distribution", fontsize=16, y=1.02)
        safe_name = team.replace(" ", "_").replace("/", "_")
        plt.savefig(f"{output_dir}/{safe_name}_hist.png")
        plt.show()
        plt.close()

plot_each_team_hist(df)
