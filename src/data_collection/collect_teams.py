from nba_api.stats.static import teams as nba_teams
from utils import get_db_connection, rate_limit
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TEAM_LOCATIONS = {
    1610612737: {'city': 'Atlanta', 'state': 'GA', 'arena': 'State Farm Arena', 'altitude': 1050, 'lat': 33.7573, 'lon': -84.3963, 'tz': 'America/New_York'},
    1610612738: {'city': 'Boston', 'state': 'MA', 'arena': 'TD Garden', 'altitude': 10, 'lat': 42.3662, 'lon': -71.0621, 'tz': 'America/New_York'},
    1610612751: {'city': 'Brooklyn', 'state': 'NY', 'arena': 'Barclays Center', 'altitude': 10, 'lat': 40.6826, 'lon': -73.9754, 'tz': 'America/New_York'},
    1610612766: {'city': 'Charlotte', 'state': 'NC', 'arena': 'Spectrum Center', 'altitude': 750, 'lat': 35.2251, 'lon': -80.8392, 'tz': 'America/New_York'},
    1610612741: {'city': 'Chicago', 'state': 'IL', 'arena': 'United Center', 'altitude': 590, 'lat': 41.8807, 'lon': -87.6742, 'tz': 'America/Chicago'},
    1610612739: {'city': 'Cleveland', 'state': 'OH', 'arena': 'Rocket Mortgage FieldHouse', 'altitude': 650, 'lat': 41.4965, 'lon': -81.6882, 'tz': 'America/New_York'},
    1610612742: {'city': 'Dallas', 'state': 'TX', 'arena': 'American Airlines Center', 'altitude': 430, 'lat': 32.7905, 'lon': -96.8103, 'tz': 'America/Chicago'},
    1610612743: {'city': 'Denver', 'state': 'CO', 'arena': 'Ball Arena', 'altitude': 5280, 'lat': 39.7487, 'lon': -105.0077, 'tz': 'America/Denver'},
    1610612765: {'city': 'Detroit', 'state': 'MI', 'arena': 'Little Caesars Arena', 'altitude': 580, 'lat': 42.3410, 'lon': -83.0550, 'tz': 'America/New_York'},
    1610612744: {'city': 'Golden State', 'state': 'CA', 'arena': 'Chase Center', 'altitude': 10, 'lat': 37.7680, 'lon': -122.3877, 'tz': 'America/Los_Angeles'},
    1610612745: {'city': 'Houston', 'state': 'TX', 'arena': 'Toyota Center', 'altitude': 50, 'lat': 29.7508, 'lon': -95.3621, 'tz': 'America/Chicago'},
    1610612754: {'city': 'Indiana', 'state': 'IN', 'arena': 'Gainbridge Fieldhouse', 'altitude': 720, 'lat': 39.7640, 'lon': -86.1555, 'tz': 'America/New_York'},
    1610612746: {'city': 'Los Angeles', 'state': 'CA', 'arena': 'Crypto.com Arena', 'altitude': 300, 'lat': 34.0430, 'lon': -118.2673, 'tz': 'America/Los_Angeles'},
    1610612747: {'city': 'Los Angeles', 'state': 'CA', 'arena': 'Crypto.com Arena', 'altitude': 300, 'lat': 34.0430, 'lon': -118.2673, 'tz': 'America/Los_Angeles'},
    1610612763: {'city': 'Memphis', 'state': 'TN', 'arena': 'FedExForum', 'altitude': 330, 'lat': 35.1382, 'lon': -90.0505, 'tz': 'America/Chicago'},
    1610612748: {'city': 'Miami', 'state': 'FL', 'arena': 'Kaseya Center', 'altitude': 10, 'lat': 25.7814, 'lon': -80.1870, 'tz': 'America/New_York'},
    1610612749: {'city': 'Milwaukee', 'state': 'WI', 'arena': 'Fiserv Forum', 'altitude': 617, 'lat': 43.0451, 'lon': -87.9172, 'tz': 'America/Chicago'},
    1610612750: {'city': 'Minnesota', 'state': 'MN', 'arena': 'Target Center', 'altitude': 830, 'lat': 44.9795, 'lon': -93.2760, 'tz': 'America/Chicago'},
    1610612740: {'city': 'New Orleans', 'state': 'LA', 'arena': 'Smoothie King Center', 'altitude': 10, 'lat': 29.9490, 'lon': -90.0821, 'tz': 'America/Chicago'},
    1610612752: {'city': 'New York', 'state': 'NY', 'arena': 'Madison Square Garden', 'altitude': 30, 'lat': 40.7505, 'lon': -73.9934, 'tz': 'America/New_York'},
    1610612760: {'city': 'Oklahoma City', 'state': 'OK', 'arena': 'Paycom Center', 'altitude': 1200, 'lat': 35.4634, 'lon': -97.5151, 'tz': 'America/Chicago'},
    1610612753: {'city': 'Orlando', 'state': 'FL', 'arena': 'Kia Center', 'altitude': 80, 'lat': 28.5392, 'lon': -81.3839, 'tz': 'America/New_York'},
    1610612755: {'city': 'Philadelphia', 'state': 'PA', 'arena': 'Wells Fargo Center', 'altitude': 30, 'lat': 39.9012, 'lon': -75.1720, 'tz': 'America/New_York'},
    1610612756: {'city': 'Phoenix', 'state': 'AZ', 'arena': 'Footprint Center', 'altitude': 1100, 'lat': 33.4457, 'lon': -112.0712, 'tz': 'America/Phoenix'},
    1610612757: {'city': 'Portland', 'state': 'OR', 'arena': 'Moda Center', 'altitude': 50, 'lat': 45.5316, 'lon': -122.6668, 'tz': 'America/Los_Angeles'},
    1610612758: {'city': 'Sacramento', 'state': 'CA', 'arena': 'Golden 1 Center', 'altitude': 25, 'lat': 38.5802, 'lon': -121.4997, 'tz': 'America/Los_Angeles'},
    1610612759: {'city': 'San Antonio', 'state': 'TX', 'arena': 'Frost Bank Center', 'altitude': 650, 'lat': 29.4270, 'lon': -98.4375, 'tz': 'America/Chicago'},
    1610612761: {'city': 'Toronto', 'state': 'ON', 'arena': 'Scotiabank Arena', 'altitude': 250, 'lat': 43.6435, 'lon': -79.3791, 'tz': 'America/Toronto'},
    1610612762: {'city': 'Utah', 'state': 'UT', 'arena': 'Delta Center', 'altitude': 4226, 'lat': 40.7683, 'lon': -111.9011, 'tz': 'America/Denver'},
    1610612764: {'city': 'Washington', 'state': 'DC', 'arena': 'Capital One Arena', 'altitude': 50, 'lat': 38.8981, 'lon': -77.0209, 'tz': 'America/New_York'},
}

def collect_teams():
    print("Collecting NBA teams data...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    teams = nba_teams.get_teams()
    
    for team in teams:
        team_id = team['id']
        abbr = team['abbreviation']
        full_name = team['full_name']
        
        location = TEAM_LOCATIONS.get(team_id, {})
        
        cur.execute("""
            INSERT INTO teams (team_id, abbreviation, full_name, city, state, arena_name, 
                             arena_altitude, conference, division, latitude, longitude, timezone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id) DO UPDATE SET
                abbreviation = EXCLUDED.abbreviation,
                full_name = EXCLUDED.full_name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                arena_name = EXCLUDED.arena_name,
                arena_altitude = EXCLUDED.arena_altitude,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                timezone = EXCLUDED.timezone
        """, (
            team_id, abbr, full_name,
            location.get('city', ''),
            location.get('state', ''),
            location.get('arena', ''),
            location.get('altitude', 0),
            team.get('conference', ''),
            team.get('division', ''),
            location.get('lat'),
            location.get('lon'),
            location.get('tz', '')
        ))
        
        rate_limit()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Inserted {len(teams)} teams successfully!")

if __name__ == "__main__":
    collect_teams()