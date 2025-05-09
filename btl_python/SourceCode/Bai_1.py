import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import partial

class FootballDataScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        self.session.headers.update(self.headers)
        self.base_url = "https://fbref.com"
        self.player_data = {}
        self.team_count = 0
        self.request_delay = (3, 6)
        
    def fetch_page(self, url, max_retries=3):
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(*self.request_delay))
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                
                if 'text/html' not in response.headers.get('Content-Type', ''):
                    raise ValueError("Invalid content type")
                    
                return response.text
                
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(random.uniform(5, 10))
    
    def initialize_player_record(self, player_name):
        return {
            "info": {
                "name": player_name,
                "team": "N/A",
                "nation": "N/A",
                "position": "N/A",
                "age": "N/A"
            },
            "playing_time": {
                "matches": "N/A",
                "starts": "N/A",
                "minutes": "N/A"
            },
            "performance": {
                "goals": "N/A",
                "assists": "N/A",
                "yellow_cards": "N/A",
                "red_cards": "N/A"
            },
            "expected": {
                "xg": "N/A",
                "xag": "N/A"
            },
            "progression": {
                "prgc": "N/A",
                "prgp": "N/A",
                "prgr": "N/A"
            },
            "per_90": {
                "goals": "N/A",
                "assists": "N/A",
                "xg": "N/A",
                "xag": "N/A"
            },
            "goalkeeping": {
                "ga90": "N/A",
                "save_pct": "N/A",
                "cs_pct": "N/A",
                "pen_save_pct": "N/A"
            },
            "shooting": {
                "sot_pct": "N/A",
                "sot_per90": "N/A",
                "goals_per_shot": "N/A",
                "avg_shot_dist": "N/A"
            },
            "passing": {
                "total": {
                    "completed": "N/A",
                    "completion_pct": "N/A",
                    "total_distance": "N/A"
                },
                "ranges": {
                    "short_pct": "N/A",
                    "medium_pct": "N/A",
                    "long_pct": "N/A"
                },
                "expected": {
                    "key_passes": "N/A",
                    "final_third": "N/A",
                    "penalty_area": "N/A",
                    "crosses": "N/A",
                    "progressive": "N/A"
                }
            },
            "creation": {
                "sca": "N/A",
                "sca90": "N/A",
                "gca": "N/A",
                "gca90": "N/A"
            },
            "defense": {
                "tackles": "N/A",
                "tackles_won": "N/A",
                "challenges": "N/A",
                "challenges_lost": "N/A",
                "blocks": "N/A",
                "shot_blocks": "N/A",
                "pass_blocks": "N/A",
                "interceptions": "N/A"
            },
            "possession": {
                "touches": {
                    "total": "N/A",
                    "def_pen": "N/A",
                    "def_3rd": "N/A",
                    "mid_3rd": "N/A",
                    "att_3rd": "N/A",
                    "att_pen": "N/A"
                },
                "take_ons": {
                    "attempted": "N/A",
                    "success_pct": "N/A",
                    "tackled_pct": "N/A"
                },
                "carries": {
                    "total": "N/A",
                    "prog_distance": "N/A",
                    "progressive": "N/A",
                    "final_third": "N/A",
                    "penalty_area": "N/A",
                    "miscontrols": "N/A",
                    "dispossessed": "N/A"
                },
                "receiving": {
                    "received": "N/A",
                    "progressive": "N/A"
                }
            },
            "miscellaneous": {
                "performance": {
                    "fouls": "N/A",
                    "fouled": "N/A",
                    "offsides": "N/A",
                    "crosses": "N/A",
                    "recoveries": "N/A"
                },
                "aerials": {
                    "won": "N/A",
                    "lost": "N/A",
                    "win_pct": "N/A"
                }
            }
        }
    
    def extract_team_links(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        results_table = soup.find('table', {'id': 'results2024-202591_overall'})
        
        if not results_table:
            raise ValueError("Results table not found")
            
        team_links = {}
        for row in results_table.find('tbody').find_all('tr'):
            team_cell = row.find('td', {'data-stat': 'team'})
            if team_cell and (a_tag := team_cell.find('a')):
                team_links[a_tag.text.strip()] = f"{self.base_url}{a_tag['href']}"
        
        return team_links
    
    def process_team_data(self, team_name, team_url):
        print(f"Processing {team_name}...")
        
        team_html = self.fetch_page(team_url)
        if not team_html:
            print(f"Failed to fetch data for {team_name}")
            return
            
        soup = BeautifulSoup(team_html, 'html.parser')
        
        with ThreadPoolExecutor() as executor:
            executor.submit(self.process_standard_stats, soup, team_name)
            executor.submit(self.process_goalkeeping_stats, soup)
            executor.submit(self.process_shooting_stats, soup)
            executor.submit(self.process_passing_stats, soup)
            executor.submit(self.process_creation_stats, soup)
            executor.submit(self.process_defensive_stats, soup)
            executor.submit(self.process_possession_stats, soup)
            executor.submit(self.process_miscellaneous_stats, soup)
        
        print(f"Completed processing {team_name}")
    
    def process_standard_stats(self, soup, team_name):
        table = soup.find('table', {'id': 'stats_standard_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                self.player_data[player_name] = self.initialize_player_record(player_name)
                
            player = self.player_data[player_name]
            player['info']['team'] = team_name
            
            if (nation := row.find('td', {'data-stat': 'nationality'})):
                nation_text = nation.text.strip()
                player['info']['nation'] = nation_text.split()[-1] if ' ' in nation_text else nation_text
                
            if (position := row.find('td', {'data-stat': 'position'})):
                player['info']['position'] = position.text
                
            if (age := row.find('td', {'data-stat': 'age'})):
                player['info']['age'] = age.text
                
            if (matches := row.find('td', {'data-stat': 'games'})):
                player['playing_time']['matches'] = matches.text
                
            if (starts := row.find('td', {'data-stat': 'games_starts'})):
                player['playing_time']['starts'] = starts.text
                
            if (minutes := row.find('td', {'data-stat': 'minutes'})):
                try:
                    mins = int(minutes.text.strip().replace(',', ''))
                    if mins >= 90:
                        player['playing_time']['minutes'] = mins
                    else:
                        self.player_data.pop(player_name, None)
                        continue
                except (ValueError, AttributeError):
                    self.player_data.pop(player_name, None)
                    continue
                    
            if (goals := row.find('td', {'data-stat': 'goals'})):
                player['performance']['goals'] = goals.text
                
            if (assists := row.find('td', {'data-stat': 'assists'})):
                player['performance']['assists'] = assists.text
                
            if (yellows := row.find('td', {'data-stat': 'cards_yellow'})):
                player['performance']['yellow_cards'] = yellows.text
                
            if (reds := row.find('td', {'data-stat': 'cards_red'})):
                player['performance']['red_cards'] = reds.text
                
            if (xg := row.find('td', {'data-stat': 'xg'})):
                player['expected']['xg'] = xg.text
                
            if (xag := row.find('td', {'data-stat': 'xg_assist'})):
                player['expected']['xag'] = xag.text
                
            if (prgc := row.find('td', {'data-stat': 'progressive_carries'})):
                player['progression']['prgc'] = prgc.text
                
            if (prgp := row.find('td', {'data-stat': 'progressive_passes'})):
                player['progression']['prgp'] = prgp.text
                
            if (prgr := row.find('td', {'data-stat': 'progressive_passes_received'})):
                player['progression']['prgr'] = prgr.text
                
            if (gls90 := row.find('td', {'data-stat': 'goals_per90'})):
                player['per_90']['goals'] = gls90.text
                
            if (ast90 := row.find('td', {'data-stat': 'assists_per90'})):
                player['per_90']['assists'] = ast90.text
                
            if (xg90 := row.find('td', {'data-stat': 'xg_per90'})):
                player['per_90']['xg'] = xg90.text
                
            if (xag90 := row.find('td', {'data-stat': 'xg_assist_per90'})):
                player['per_90']['xag'] = xag90.text
    
    def process_goalkeeping_stats(self, soup):
        table = soup.find('table', {'id': 'stats_keeper_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (ga90 := row.find('td', {'data-stat': 'gk_goals_against_per90'})):
                player['goalkeeping']['ga90'] = ga90.text.strip()
                
            if (save_pct := row.find('td', {'data-stat': 'gk_save_pct'})):
                player['goalkeeping']['save_pct'] = save_pct.text.strip()
                
            if (cs_pct := row.find('td', {'data-stat': 'gk_clean_sheets_pct'})):
                player['goalkeeping']['cs_pct'] = cs_pct.text.strip()
                
            if (pen_save := row.find('td', {'data-stat': 'gk_pens_save_pct'})):
                player['goalkeeping']['pen_save_pct'] = pen_save.text.strip()
    
    def process_shooting_stats(self, soup):
        table = soup.find('table', {'id': 'stats_shooting_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (sot_pct := row.find('td', {'data-stat': 'shots_on_target_pct'})):
                player['shooting']['sot_pct'] = sot_pct.text.strip()
                
            if (sot90 := row.find('td', {'data-stat': 'shots_on_target_per90'})):
                player['shooting']['sot_per90'] = sot90.text.strip()
                
            if (gps := row.find('td', {'data-stat': 'goals_per_shot'})):
                player['shooting']['goals_per_shot'] = gps.text.strip()
                
            if (avg_dist := row.find('td', {'data-stat': 'average_shot_distance'})):
                player['shooting']['avg_shot_dist'] = avg_dist.text.strip()
    
    def process_passing_stats(self, soup):
        table = soup.find('table', {'id': 'stats_passing_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (cmp := row.find('td', {'data-stat': 'passes_completed'})):
                player['passing']['total']['completed'] = cmp.text.strip()
                
            if (cmp_pct := row.find('td', {'data-stat': 'passes_pct'})):
                player['passing']['total']['completion_pct'] = cmp_pct.text.strip()
                
            if (tot_dist := row.find('td', {'data-stat': 'passes_total_distance'})):
                player['passing']['total']['total_distance'] = tot_dist.text.strip()
                
            if (short_pct := row.find('td', {'data-stat': 'passes_pct_short'})):
                player['passing']['ranges']['short_pct'] = short_pct.text.strip()
                
            if (med_pct := row.find('td', {'data-stat': 'passes_pct_medium'})):
                player['passing']['ranges']['medium_pct'] = med_pct.text.strip()
                
            if (long_pct := row.find('td', {'data-stat': 'passes_pct_long'})):
                player['passing']['ranges']['long_pct'] = long_pct.text.strip()
                
            if (kp := row.find('td', {'data-stat': 'assisted_shots'})):
                player['passing']['expected']['key_passes'] = kp.text.strip()
                
            if (final_third := row.find('td', {'data-stat': 'passes_into_final_third'})):
                player['passing']['expected']['final_third'] = final_third.text.strip()
                
            if (pen_area := row.find('td', {'data-stat': 'passes_into_penalty_area'})):
                player['passing']['expected']['penalty_area'] = pen_area.text.strip()
                
            if (crosses := row.find('td', {'data-stat': 'crosses_into_penalty_area'})):
                player['passing']['expected']['crosses'] = crosses.text.strip()
                
            if (prog_passes := row.find('td', {'data-stat': 'progressive_passes'})):
                player['passing']['expected']['progressive'] = prog_passes.text.strip()
    
    def process_creation_stats(self, soup):
        table = soup.find('table', {'id': 'stats_gca_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (sca := row.find('td', {'data-stat': 'sca'})):
                player['creation']['sca'] = sca.text.strip()
                
            if (sca90 := row.find('td', {'data-stat': 'sca_per90'})):
                player['creation']['sca90'] = sca90.text.strip()
                
            if (gca := row.find('td', {'data-stat': 'gca'})):
                player['creation']['gca'] = gca.text.strip()
                
            if (gca90 := row.find('td', {'data-stat': 'gca_per90'})):
                player['creation']['gca90'] = gca90.text.strip()
    
    def process_defensive_stats(self, soup):
        table = soup.find('table', {'id': 'stats_defense_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (tkl := row.find('td', {'data-stat': 'tackles'})):
                player['defense']['tackles'] = tkl.text.strip()
                
            if (tkl_won := row.find('td', {'data-stat': 'tackles_won'})):
                player['defense']['tackles_won'] = tkl_won.text.strip()
                
            if (challenges := row.find('td', {'data-stat': 'challenges'})):
                player['defense']['challenges'] = challenges.text.strip()
                
            if (challenges_lost := row.find('td', {'data-stat': 'challenges_lost'})):
                player['defense']['challenges_lost'] = challenges_lost.text.strip()
                
            if (blocks := row.find('td', {'data-stat': 'blocks'})):
                player['defense']['blocks'] = blocks.text.strip()
                
            if (shot_blocks := row.find('td', {'data-stat': 'blocked_shots'})):
                player['defense']['shot_blocks'] = shot_blocks.text.strip()
                
            if (pass_blocks := row.find('td', {'data-stat': 'blocked_passes'})):
                player['defense']['pass_blocks'] = pass_blocks.text.strip()
                
            if (interceptions := row.find('td', {'data-stat': 'interceptions'})):
                player['defense']['interceptions'] = interceptions.text.strip()
    
    def process_possession_stats(self, soup):
        table = soup.find('table', {'id': 'stats_possession_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (touches := row.find('td', {'data-stat': 'touches'})):
                player['possession']['touches']['total'] = touches.text.strip()
                
            if (def_pen := row.find('td', {'data-stat': 'touches_def_pen_area'})):
                player['possession']['touches']['def_pen'] = def_pen.text.strip()
                
            if (def_3rd := row.find('td', {'data-stat': 'touches_def_3rd'})):
                player['possession']['touches']['def_3rd'] = def_3rd.text.strip()
                
            if (mid_3rd := row.find('td', {'data-stat': 'touches_mid_3rd'})):
                player['possession']['touches']['mid_3rd'] = mid_3rd.text.strip()
                
            if (att_3rd := row.find('td', {'data-stat': 'touches_att_3rd'})):
                player['possession']['touches']['att_3rd'] = att_3rd.text.strip()
                
            if (att_pen := row.find('td', {'data-stat': 'touches_att_pen_area'})):
                player['possession']['touches']['att_pen'] = att_pen.text.strip()
                
            if (take_ons := row.find('td', {'data-stat': 'take_ons'})):
                player['possession']['take_ons']['attempted'] = take_ons.text.strip()
                
            if (success_pct := row.find('td', {'data-stat': 'take_ons_won_pct'})):
                player['possession']['take_ons']['success_pct'] = success_pct.text.strip()
                
            if (tackled_pct := row.find('td', {'data-stat': 'take_ons_tackled_pct'})):
                player['possession']['take_ons']['tackled_pct'] = tackled_pct.text.strip()
                
            if (carries := row.find('td', {'data-stat': 'carries'})):
                player['possession']['carries']['total'] = carries.text.strip()
                
            if (prog_dist := row.find('td', {'data-stat': 'carries_progressive_distance'})):
                player['possession']['carries']['prog_distance'] = prog_dist.text.strip()
                
            if (progressive := row.find('td', {'data-stat': 'progressive_carries'})):
                player['possession']['carries']['progressive'] = progressive.text.strip()
                
            if (final_third := row.find('td', {'data-stat': 'carries_into_final_third'})):
                player['possession']['carries']['final_third'] = final_third.text.strip()
                
            if (pen_area := row.find('td', {'data-stat': 'carries_into_penalty_area'})):
                player['possession']['carries']['penalty_area'] = pen_area.text.strip()
                
            if (miscontrols := row.find('td', {'data-stat': 'miscontrols'})):
                player['possession']['carries']['miscontrols'] = miscontrols.text.strip()
                
            if (dispossessed := row.find('td', {'data-stat': 'dispossessed'})):
                player['possession']['carries']['dispossessed'] = dispossessed.text.strip()
                
            if (received := row.find('td', {'data-stat': 'passes_received'})):
                player['possession']['receiving']['received'] = received.text.strip()
                
            if (prog_received := row.find('td', {'data-stat': 'progressive_passes_received'})):
                player['possession']['receiving']['progressive'] = prog_received.text.strip()
    
    def process_miscellaneous_stats(self, soup):
        table = soup.find('table', {'id': 'stats_misc_9'})
        if not table:
            return
            
        for row in table.find('tbody').find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
                
            player_name = player_cell.find('a').text
            if player_name not in self.player_data:
                continue
                
            player = self.player_data[player_name]
            
            if (fouls := row.find('td', {'data-stat': 'fouls'})):
                player['miscellaneous']['performance']['fouls'] = fouls.text.strip()
                
            if (fouled := row.find('td', {'data-stat': 'fouled'})):
                player['miscellaneous']['performance']['fouled'] = fouled.text.strip()
                
            if (offsides := row.find('td', {'data-stat': 'offsides'})):
                player['miscellaneous']['performance']['offsides'] = offsides.text.strip()
                
            if (crosses := row.find('td', {'data-stat': 'crosses'})):
                player['miscellaneous']['performance']['crosses'] = crosses.text.strip()
                
            if (recoveries := row.find('td', {'data-stat': 'ball_recoveries'})):
                player['miscellaneous']['performance']['recoveries'] = recoveries.text.strip()
            
            if (aerials_won := row.find('td', {'data-stat': 'aerials_won'})):
                player['miscellaneous']['aerials']['won'] = aerials_won.text.strip()
                
            if (aerials_lost := row.find('td', {'data-stat': 'aerials_lost'})):
                player['miscellaneous']['aerials']['lost'] = aerials_lost.text.strip()
                
            if (aerial_win_pct := row.find('td', {'data-stat': 'aerials_won_pct'})):
                player['miscellaneous']['aerials']['win_pct'] = aerial_win_pct.text.strip()
    
    def flatten_player_data(self):
        flat_data = []
        
        for player_name, stats in self.player_data.items():
            flat_record = {
                'Name': player_name,
                'Team': stats['info']['team'],
                'Nation': stats['info']['nation'],
                'Position': stats['info']['position'],
                'Age': stats['info']['age']
            }
            
            for category, values in stats.items():
                if category == 'info':
                    continue
                    
                if isinstance(values, dict):
                    for subcat, subvalues in values.items():
                        if isinstance(subvalues, dict):
                            for key, value in subvalues.items():
                                flat_record[f"{category}_{subcat}_{key}"] = value
                        else:
                            flat_record[f"{category}_{subcat}"] = subvalues
            
            flat_data.append(flat_record)
        
        return flat_data
    
    def export_to_csv(self, filename='results.csv'):
        if not self.player_data:
            print("No player data to export")
            return
            
        flat_data = self.flatten_player_data()
        df = pd.DataFrame(flat_data)
        
        basic_cols = ['Name', 'Team', 'Nation', 'Position', 'Age']
        other_cols = [col for col in df.columns if col not in basic_cols]
        df = df[basic_cols + other_cols]
        
        df = df.sort_values(by='Name')
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Data successfully exported to {filename} (sorted by player name)")
    
    def run(self):
        print("Starting football data scraper...")
        
        main_html = self.fetch_page(self.base_url + "/en/")
        if not main_html:
            print("Failed to fetch main page")
            return
            
        try:
            team_links = self.extract_team_links(main_html)
            print(f"Found {len(team_links)} teams to process")
        except Exception as e:
            print(f"Error extracting team links: {e}")
            return
            
        total_teams = len(team_links)
        for i, (team_name, team_url) in enumerate(team_links.items(), 1):
            print(f"\nProcessing team {i}/{total_teams}: {team_name}")
            self.process_team_data(team_name, team_url)
            
        self.export_to_csv()
        
        print("\nScraping completed!")

if __name__ == "__main__":
    scraper = FootballDataScraper()
    scraper.run()