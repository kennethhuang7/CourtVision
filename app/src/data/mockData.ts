import { Game, Prediction, Player, HistoricalGame, ModelPerformance, ContextInfo } from '@/types/nba';

const players: Player[] = [
  { id: '201142', name: 'Kevin Durant', team: 'Phoenix Suns', teamAbbr: 'PHX', position: 'Forward', photoUrl: '/player-placeholder.png',
  { id: '203507', name: 'Giannis Antetokounmpo', team: 'Milwaukee Bucks', teamAbbr: 'MIL', position: 'Forward', photoUrl: '/player-placeholder.png',
  { id: '201566', name: 'Russell Westbrook', team: 'Denver Nuggets', teamAbbr: 'DEN', position: 'Guard', photoUrl: '/player-placeholder.png',
  { id: '203954', name: 'Joel Embiid', team: 'Philadelphia 76ers', teamAbbr: 'PHI', position: 'Center', photoUrl: '/player-placeholder.png',
  { id: '1628369', name: 'Jayson Tatum', team: 'Boston Celtics', teamAbbr: 'BOS', position: 'Forward', photoUrl: '/player-placeholder.png',
  { id: '203081', name: 'Damian Lillard', team: 'Milwaukee Bucks', teamAbbr: 'MIL', position: 'Guard', photoUrl: '/player-placeholder.png',
  { id: '1629029', name: 'Luka Doncic', team: 'Dallas Mavericks', teamAbbr: 'DAL', position: 'Guard', photoUrl: '/player-placeholder.png',
  { id: '203999', name: 'Nikola Jokic', team: 'Denver Nuggets', teamAbbr: 'DEN', position: 'Center', photoUrl: '/player-placeholder.png',
];

const generateStats = () => ({
  points: Math.floor(Math.random() * 20) + 15,
  rebounds: Math.floor(Math.random() * 8) + 3,
  assists: Math.floor(Math.random() * 8) + 2,
  steals: Math.floor(Math.random() * 3),
  blocks: Math.floor(Math.random() * 2),
  turnovers: Math.floor(Math.random() * 4) + 1,
  threePointersMade: Math.floor(Math.random() * 5),
});

export const mockGames: Game[] = [
  {
    id: '1',
    date: new Date().toISOString().split('T')[0],
    homeTeam: 'Phoenix Suns',
    homeTeamAbbr: 'PHX',
    awayTeam: 'Milwaukee Bucks',
    awayTeamAbbr: 'MIL',
    predictions: [
      {
        id: 'p1',
        playerId: '201142',
        player: players[0],
        gameId: '1',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Milwaukee Bucks',
        opponentAbbr: 'MIL',
        isHome: true,
        confidence: 87,
        predictedStats: { points: 28, rebounds: 7, assists: 5, steals: 1, blocks: 1, turnovers: 3, threePointersMade: 3 },
      },
      {
        id: 'p2',
        playerId: '203507',
        player: players[1],
        gameId: '1',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Phoenix Suns',
        opponentAbbr: 'PHX',
        isHome: false,
        confidence: 92,
        predictedStats: { points: 32, rebounds: 12, assists: 6, steals: 1, blocks: 2, turnovers: 4, threePointersMade: 1 },
      },
      {
        id: 'p3',
        playerId: '203081',
        player: players[5],
        gameId: '1',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Phoenix Suns',
        opponentAbbr: 'PHX',
        isHome: false,
        confidence: 78,
        predictedStats: { points: 26, rebounds: 4, assists: 7, steals: 1, blocks: 0, turnovers: 3, threePointersMade: 4 },
      },
    ],
  },
  {
    id: '2',
    date: new Date().toISOString().split('T')[0],
    homeTeam: 'Boston Celtics',
    homeTeamAbbr: 'BOS',
    awayTeam: 'Philadelphia 76ers',
    awayTeamAbbr: 'PHI',
    predictions: [
      {
        id: 'p4',
        playerId: '1628369',
        player: players[4],
        gameId: '2',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Philadelphia 76ers',
        opponentAbbr: 'PHI',
        isHome: true,
        confidence: 85,
        predictedStats: { points: 30, rebounds: 8, assists: 4, steals: 2, blocks: 1, turnovers: 2, threePointersMade: 4 },
      },
      {
        id: 'p5',
        playerId: '203954',
        player: players[3],
        gameId: '2',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Boston Celtics',
        opponentAbbr: 'BOS',
        isHome: false,
        confidence: 65,
        predictedStats: { points: 28, rebounds: 11, assists: 3, steals: 1, blocks: 2, turnovers: 3, threePointersMade: 1 },
      },
    ],
  },
  {
    id: '3',
    date: new Date().toISOString().split('T')[0],
    homeTeam: 'Denver Nuggets',
    homeTeamAbbr: 'DEN',
    awayTeam: 'Dallas Mavericks',
    awayTeamAbbr: 'DAL',
    predictions: [
      {
        id: 'p6',
        playerId: '203999',
        player: players[7],
        gameId: '3',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Dallas Mavericks',
        opponentAbbr: 'DAL',
        isHome: true,
        confidence: 94,
        predictedStats: { points: 26, rebounds: 13, assists: 9, steals: 1, blocks: 1, turnovers: 3, threePointersMade: 2 },
      },
      {
        id: 'p7',
        playerId: '1629029',
        player: players[6],
        gameId: '3',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Denver Nuggets',
        opponentAbbr: 'DEN',
        isHome: false,
        confidence: 88,
        predictedStats: { points: 33, rebounds: 9, assists: 10, steals: 2, blocks: 0, turnovers: 4, threePointersMade: 4 },
      },
      {
        id: 'p8',
        playerId: '201566',
        player: players[2],
        gameId: '3',
        gameDate: new Date().toISOString().split('T')[0],
        opponent: 'Dallas Mavericks',
        opponentAbbr: 'DAL',
        isHome: true,
        confidence: 55,
        predictedStats: { points: 14, rebounds: 5, assists: 7, steals: 1, blocks: 0, turnovers: 4, threePointersMade: 1 },
      },
    ],
  },
];

export const mockHistoricalGames: HistoricalGame[] = Array.from({ length: 20 }, (_, i) => {
  const date = new Date();
  date.setDate(date.getDate() - (i + 1));
  const opponents = ['LAL', 'GSW', 'MIA', 'CHI', 'NYK', 'BKN', 'LAC', 'PHX'];
  
  return {
    id: `hist-${i}`,
    date: date.toISOString().split('T')[0],
    opponent: opponents[i % opponents.length],
    opponentAbbr: opponents[i % opponents.length],
    isHome: Math.random() > 0.5,
    result: Math.random() > 0.4 ? 'W' : 'L',
    score: `${100 + Math.floor(Math.random() * 30)}-${100 + Math.floor(Math.random() * 30)}`,
    stats: generateStats(),
    minutesPlayed: Math.floor(Math.random() * 15) + 25,
  };
});

export const mockModelPerformance: ModelPerformance[] = [
  { stat: 'Points', mae: 3.42, predictions: 1250 },
  { stat: 'Rebounds', mae: 1.87, predictions: 1250 },
  { stat: 'Assists', mae: 1.54, predictions: 1250 },
  { stat: 'Steals', mae: 0.72, predictions: 1250 },
  { stat: 'Blocks', mae: 0.58, predictions: 1250 },
  { stat: 'Turnovers', mae: 0.91, predictions: 1250 },
  { stat: '3PM', mae: 0.94, predictions: 1250 },
  { stat: 'Overall', mae: 1.43, predictions: 1250 },
];

export const mockContextInfo: ContextInfo = {
  opponentDefense: 'MIL ranks 8th in PTS versus SF, a moderately tough setup',
  starPlayersOut: 'No significant injuries reported for either team',
  restDays: "Player's team: 2 days rest | Opponent: 1 day rest",
  playoffExperience: 'Not a playoff game - regular season matchup',
  paceComparison: 'PHX plays at 99.2 pace and MIL plays at 97.8 pace. This is 1.5% lower than league average.',
};

export { players };
