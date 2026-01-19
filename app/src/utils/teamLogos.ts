


const ESPN_TEAM_ABBR_MAP: Record<string, string> = {
  'UTA': 'utah',
  'PHX': 'phx',
  'GSW': 'gs',
  'PHI': 'phi',
  'BOS': 'bos',
  'LAL': 'lal',
  'LAC': 'lac',
  'MIA': 'mia',
  'MIL': 'mil',
  'BKN': 'bkn',
  'NYK': 'ny',
  'TOR': 'tor',
  'CHI': 'chi',
  'CLE': 'cle',
  'DEN': 'den',
  'DAL': 'dal',
  'MIN': 'min',
  'NOP': 'no',
  'OKC': 'okc',
  'ORL': 'orl',
  'IND': 'ind',
  'ATL': 'atl',
  'MEM': 'mem',
  'DET': 'det',
  'WAS': 'wsh',
  'SAC': 'sac',
  'POR': 'por',
  'HOU': 'hou',
  'SAS': 'sa',
  'CHA': 'cha',
};


export function getTeamLogoUrl(teamAbbr: string, teamId?: string | number) {
  const espnAbbr = ESPN_TEAM_ABBR_MAP[teamAbbr.toUpperCase()] || teamAbbr.toLowerCase();

  return {
    primary: `https://a.espncdn.com/i/teamlogos/nba/500/${espnAbbr}.png`,
    fallback: teamId ? `https://cdn.nba.com/logos/nba/${teamId}/primary/L/logo.svg` : undefined,
  };
}


export function getPlayerHeadshotUrl(playerId: string | number) {
  return `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${playerId}.png`;
}


export async function convertImageToBase64(url: string): Promise<string> {
  try {
    const response = await fetch(url, {
      mode: 'cors',
      credentials: 'omit',
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const blob = await response.blob();
    return new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onloadend = () => resolve(reader.result as string);
      reader.onerror = reject;
      reader.readAsDataURL(blob);
    });
  } catch (error) {
    
    throw error;
  }
}
