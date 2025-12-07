import requests
from bs4 import BeautifulSoup

url = "https://www.espn.com/nba/injuries"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

response = requests.get(url, headers=headers, timeout=10)
soup = BeautifulSoup(response.content, 'html.parser')

injury_tables = soup.find_all('div', class_='ResponsiveTable')

if injury_tables:
    table = injury_tables[0]
    
    header_row = table.find('tr')
    if header_row:
        headers_list = [th.text.strip() for th in header_row.find_all('th')]
        print("ESPN Injury Table Headers:")
        for i, h in enumerate(headers_list):
            print(f"  Column {i}: {h}")
    
    rows = table.find_all('tr')[1:]
    if rows:
        print("\nFirst injury row:")
        first_row = rows[0]
        cols = first_row.find_all('td')
        for i, col in enumerate(cols):
            print(f"  Column {i}: {col.text.strip()}")