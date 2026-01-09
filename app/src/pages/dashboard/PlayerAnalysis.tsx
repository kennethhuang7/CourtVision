import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Combobox } from '@/components/ui/combobox';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar as CalendarComponent } from '@/components/ui/calendar';
import { useSupabasePredictions } from '@/hooks/useSupabasePredictions';
import { usePlayerHistoricalGames } from '@/hooks/usePlayerHistoricalGames';
import { useSavePick } from '@/hooks/useSavePick';
import { useOpponentDefense, useStarPlayersOut, useRestDays, usePlayoffExperience, usePaceComparison } from '@/hooks/useContextCards';
import { useEnsemble } from '@/contexts/EnsembleContext';
import { useTheme } from '@/contexts/ThemeContext';
import { formatUserDate } from '@/lib/dateUtils';
import { supabase } from '@/lib/supabase';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer, ReferenceLine, Cell, Rectangle } from 'recharts';
import { Shield, Users, Clock, Trophy, Zap, TrendingUp, TrendingDown, Brain, ChevronDown, GripVertical, Calendar, HelpCircle, Image, Save, RefreshCw, AlertCircle } from 'lucide-react';
import { ExportImageModal, ExportOptions } from '@/components/analysis/ExportImageModal';
import { ExportLayout } from '@/components/analysis/ExportLayout';
import { toPng } from 'html-to-image';
import { cn } from '@/lib/utils';
import { GameLogTable } from '@/components/analysis/GameLogTable';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Tooltip, TooltipTrigger, TooltipContent } from '@/components/ui/tooltip';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';
import { cleanNameForMatching, extractTeamName } from '@/lib/nameUtils';
import { Game } from '@/types/nba';
import { format } from 'date-fns';
import { getTeamLogoUrl } from '@/utils/teamLogos';

const statOptions = [
  { value: 'points', label: 'Points' },
  { value: 'rebounds', label: 'Rebounds' },
  { value: 'assists', label: 'Assists' },
  { value: 'steals', label: 'Steals' },
  { value: 'blocks', label: 'Blocks' },
  { value: 'turnovers', label: 'Turnovers' },
  { value: 'threePointersMade', label: '3-Pointers Made' },
] as const;

const timeWindows = ['L5', 'L10', 'L20', 'L50', 'All'] as const;

export default function PlayerAnalysis() {
  const { dateFormat, theme: currentTheme } = useTheme();
  
  
  const [selectedDate, setSelectedDate] = useState<Date>(() => {
    const stored = sessionStorage.getItem('shared-selected-date');
    if (stored) {
      const parsed = new Date(stored);
      if (!isNaN(parsed.getTime())) {
        return parsed;
      }
    }
    return new Date();
  });

  
  useEffect(() => {
    sessionStorage.setItem('shared-selected-date', selectedDate.toISOString());
  }, [selectedDate]);

  
  const [selectedGame, setSelectedGame] = useState<string>(() => {
    const stored = localStorage.getItem('player-analysis-selected-game');
    return stored || '';
  });
  const [selectedPlayer, setSelectedPlayer] = useState<string>(() => {
    const stored = localStorage.getItem('player-analysis-selected-player');
    return stored || '';
  });
  const [selectedStat, setSelectedStat] = useState<string>(() => {
    const stored = localStorage.getItem('player-analysis-selected-stat');
    return stored || 'points';
  });
  
  
  useEffect(() => {
    if (selectedGame) {
      localStorage.setItem('player-analysis-selected-game', selectedGame);
    }
  }, [selectedGame]);
  
  useEffect(() => {
    if (selectedPlayer) {
      localStorage.setItem('player-analysis-selected-player', selectedPlayer);
    }
  }, [selectedPlayer]);
  
  useEffect(() => {
    if (selectedStat) {
      localStorage.setItem('player-analysis-selected-stat', selectedStat);
    }
  }, [selectedStat]);
  
  const [timeWindow, setTimeWindow] = useState<string>('L10');
  const [overUnder, setOverUnder] = useState<'over' | 'under'>('over');
  
  
  const [lineValue, setLineValue] = useState(() => {
    
    const stored = localStorage.getItem('player-analysis-line-value');
    if (stored) {
      const parsed = Number(stored);
      if (!isNaN(parsed)) return parsed;
    }
    
    return 20;
  });
  const [excludeDNP, setExcludeDNP] = useState(true);
  const [minMinutes, setMinMinutes] = useState(0);
  const [h2hOnly, setH2hOnly] = useState(false);

  
  const [exportModalOpen, setExportModalOpen] = useState(false);
  const exportContainerRef = useRef<HTMLDivElement>(null);
  const [currentExportOptions, setCurrentExportOptions] = useState<ExportOptions | null>(null);
  const [homeAwayOnly, setHomeAwayOnly] = useState(false);
  const [currentTeamOnly, setCurrentTeamOnly] = useState(false);
  const [thisSeasonOnly, setThisSeasonOnly] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [optionsOpen, setOptionsOpen] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const chartRef = useRef<HTMLDivElement>(null);

  const { selectedModels } = useEnsemble();
  const { data: games = [], isLoading: isLoadingPredictions } = useSupabasePredictions(
    selectedDate,
    selectedModels
  );

  
  
  useEffect(() => {
    const storedGameId = localStorage.getItem('player-analysis-selected-game');
    if (storedGameId && !isLoadingPredictions) {
      const gameExists = games.find(g => g.id === storedGameId);
      if (!gameExists) {
        
        const findGameDate = async () => {
          const { data: predictionData, error } = await supabase
            .from('predictions')
            .select('prediction_date, game_id')
            .eq('game_id', storedGameId)
            .limit(1)
            .maybeSingle();
          
          
          
        };
        findGameDate();
      }
    }
  }, [games, selectedDate, isLoadingPredictions]);

  
  const selectedGameObj = games.find(g => g.id === selectedGame) || games[0];

  const gameOptions = useMemo(() => {
    return games.map((game) => ({
      id: game.id,
      label: `${game.awayTeamAbbr} @ ${game.homeTeamAbbr}`,
      game: game, 
    }));
  }, [games]);

  const playerOptions = selectedGameObj ? selectedGameObj.predictions.map(p => p.player) : [];

  
  const teamSearchMap: Record<string, string[]> = {
    'PHX': ['phx', 'phoenix', 'suns', 'phoenix suns'],
    'MIL': ['mil', 'milwaukee', 'bucks', 'milwaukee bucks'],
    'BOS': ['bos', 'boston', 'celtics', 'boston celtics'],
    'PHI': ['phi', 'philadelphia', 'sixers', '76ers', 'philadelphia 76ers'],
    'DEN': ['den', 'denver', 'nuggets', 'denver nuggets'],
    'DAL': ['dal', 'dallas', 'mavs', 'mavericks', 'dallas mavericks'],
    'LAL': ['lal', 'los angeles', 'lakers', 'la lakers', 'los angeles lakers'],
    'GSW': ['gsw', 'golden state', 'warriors', 'golden state warriors'],
    'MIA': ['mia', 'miami', 'heat', 'miami heat'],
    'CHI': ['chi', 'chicago', 'bulls', 'chicago bulls'],
    'NYK': ['nyk', 'new york', 'knicks', 'new york knicks'],
    'BKN': ['bkn', 'brooklyn', 'nets', 'brooklyn nets'],
    'LAC': ['lac', 'la clippers', 'clippers', 'los angeles clippers'],
    'ATL': ['atl', 'atlanta', 'hawks', 'atlanta hawks'],
    'CHA': ['cha', 'charlotte', 'hornets', 'charlotte hornets'],
    'CLE': ['cle', 'cleveland', 'cavaliers', 'cavs', 'cleveland cavaliers'],
    'DET': ['det', 'detroit', 'pistons', 'detroit pistons'],
    'IND': ['ind', 'indiana', 'pacers', 'indiana pacers'],
    'MEM': ['mem', 'memphis', 'grizzlies', 'memphis grizzlies'],
    'MIN': ['min', 'minnesota', 'timberwolves', 'wolves', 'minnesota timberwolves'],
    'NO': ['no', 'nop', 'new orleans', 'pelicans', 'new orleans pelicans'],
    'OKC': ['okc', 'oklahoma city', 'thunder', 'oklahoma city thunder'],
    'ORL': ['orl', 'orlando', 'magic', 'orlando magic'],
    'POR': ['por', 'portland', 'trail blazers', 'blazers', 'portland trail blazers'],
    'SAC': ['sac', 'sacramento', 'kings', 'sacramento kings'],
    'SA': ['sa', 'sas', 'san antonio', 'spurs', 'san antonio spurs'],
    'TOR': ['tor', 'toronto', 'raptors', 'toronto raptors'],
    'UTA': ['uta', 'utah', 'jazz', 'utah jazz'],
    'WAS': ['was', 'washington', 'wizards', 'washington wizards'],
  };

  const matchesTeamSearch = (teamAbbr: string, teamFullName: string, teamCity: string | undefined, normalizedSearch: string) => {
    
    const normalizedAbbr = cleanNameForMatching(teamAbbr.toLowerCase());
    if (normalizedAbbr.includes(normalizedSearch) || normalizedSearch.includes(normalizedAbbr)) {
      return true;
    }
    
    
    const aliases = teamSearchMap[teamAbbr] || [];
    for (const alias of aliases) {
      const normalizedAlias = cleanNameForMatching(alias);
      if (normalizedAlias.includes(normalizedSearch) || normalizedSearch.includes(normalizedAlias)) {
        return true;
      }
    }
    
    
    const normalizedFullName = cleanNameForMatching(teamFullName.toLowerCase());
    if (normalizedFullName.includes(normalizedSearch) || normalizedSearch.includes(normalizedFullName)) {
      return true;
    }
    
    
    if (teamCity) {
      const normalizedCity = cleanNameForMatching(teamCity.toLowerCase());
      if (normalizedCity.includes(normalizedSearch) || normalizedSearch.includes(normalizedCity)) {
        return true;
      }
    }
    
    
    const teamName = extractTeamName(teamFullName);
    const normalizedTeamName = cleanNameForMatching(teamName.toLowerCase());
    if (normalizedTeamName.includes(normalizedSearch) || normalizedSearch.includes(normalizedTeamName)) {
      return true;
    }
    
    return false;
  };

  
  const gameFilterFn = useCallback((option: typeof gameOptions[0], searchQuery: string) => {
    const query = searchQuery.trim();
    if (!query) return true;

    const { game } = option;
    const searchTerms = query.split(/\s+/); 

    
    return searchTerms.every(term => {
      const normalizedTerm = cleanNameForMatching(term.toLowerCase());
      
      const matchesAway = matchesTeamSearch(
        game.awayTeamAbbr,
        game.awayTeam,
        game.awayTeamCity,
        normalizedTerm
      );
      
      const matchesHome = matchesTeamSearch(
        game.homeTeamAbbr,
        game.homeTeam,
        game.homeTeamCity,
        normalizedTerm
      );
      
      return matchesAway || matchesHome;
    });
  }, []);

  
  const playerFilterFn = useCallback((player: typeof playerOptions[0], searchQuery: string) => {
    const query = cleanNameForMatching(searchQuery).toLowerCase();
    if (!query) return true;

    const normalizedName = cleanNameForMatching(player.name).toLowerCase();
    const normalizedTeam = player.teamAbbr.toLowerCase();
    
    return normalizedName.includes(query) || normalizedTeam.includes(query);
  }, []);

  const player = playerOptions.find(p => p.id === selectedPlayer) || playerOptions[0];

  
  useEffect(() => {
    
    if (selectedGame) {
      const gameExists = gameOptions.find(g => g.id === selectedGame);
      if (!gameExists && gameOptions[0]) {
        
        
        setSelectedGame(gameOptions[0].id);
      }
    } else if (gameOptions[0]) {
      setSelectedGame(gameOptions[0].id);
    }
  }, [gameOptions, selectedGame]);

  useEffect(() => {
    
    if (selectedPlayer) {
      const playerExists = playerOptions.find(p => p.id === selectedPlayer);
      if (!playerExists && playerOptions[0]) {
        setSelectedPlayer(playerOptions[0].id);
      }
    } else if (playerOptions[0]) {
      setSelectedPlayer(playerOptions[0].id);
    }
  }, [playerOptions, selectedPlayer]);

  const predictionForPlayer = useMemo(() => {
    if (!selectedGameObj || !player) return undefined;
    return selectedGameObj.predictions.find(p => p.player.id === player.id);
  }, [selectedGameObj, player]);

  
  useEffect(() => {
    
    const storedLineValue = localStorage.getItem('player-analysis-line-value');
    if (storedLineValue) {
      const parsed = Number(storedLineValue);
      if (!isNaN(parsed)) {
        setLineValue(parsed);
        localStorage.removeItem('player-analysis-line-value'); 
        return;
      }
    }
    
    
    if (predictionForPlayer && selectedStat) {
      const predictedValue = predictionForPlayer.predictedStats[selectedStat as keyof typeof predictionForPlayer.predictedStats];
      if (predictedValue !== undefined && typeof predictedValue === 'number') {
        setLineValue(Math.round(predictedValue * 2) / 2); 
      }
    }
  }, [predictionForPlayer, selectedStat]);

  
  const [gameInfo, setGameInfo] = useState<{
    season: string | null;
    gameType: string | null;
    gameDate: string | null;
    opponentId: number | null;
    isHome: boolean;
  }>({
    season: null,
    gameType: null,
    gameDate: null,
    opponentId: null,
    isHome: false,
  });
  const [playerInfo, setPlayerInfo] = useState<{
    teamId: number | null;
    position: string | null;
  }>({
    teamId: null,
    position: null,
  });

  const [gameStatus, setGameStatus] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedGameObj?.id || !player?.id) return;
    
    const fetchGameInfo = async () => {
      const { data, error } = await supabase
        .from('games')
        .select('season, game_type, game_date, home_team_id, away_team_id, game_status')
        .eq('game_id', selectedGameObj.id)
        .single();
      
      if (!error && data) {
        const numericPlayerId = Number(player.id);
        const { data: playerData } = await supabase
          .from('players')
          .select('team_id, position')
          .eq('player_id', numericPlayerId)
          .single();

        const playerTeamId = playerData?.team_id || null;
        const isHome = playerTeamId === data.home_team_id;
        const opponentId = isHome ? data.away_team_id : data.home_team_id;

        setGameInfo({
          season: data.season || '2025-26',
          gameType: data.game_type || 'regular_season',
          gameDate: data.game_date ? (() => {
            
            const dateStr = typeof data.game_date === 'string' 
              ? data.game_date 
              : data.game_date.toISOString().split('T')[0];
            
            const [year, month, day] = dateStr.split('-').map(Number);
            const localDate = new Date(year, month - 1, day); 
            return format(localDate, 'yyyy-MM-dd');
          })() : null,
          opponentId,
          isHome,
        });

        setPlayerInfo({
          teamId: playerTeamId,
          position: playerData?.position || null,
        });

        setGameStatus(data.game_status || null);
      }
    };

    fetchGameInfo();
  }, [selectedGameObj?.id, player?.id]);

  const { data: historicalGames = [], isLoading: isLoadingHistoricalGames, isError: isErrorHistoricalGames, error: errorHistoricalGames } = usePlayerHistoricalGames(player?.id || null, 50, selectedDate);
  const savePickMutation = useSavePick();

  
  const handleExport = async (options: ExportOptions) => {
    if (!player || !selectedGameObj) {
      const missingItems = [];
      if (!player) missingItems.push('player');
      if (!selectedGameObj) missingItems.push('game');
      logger.error('Export failed - missing required data', new Error(missingItems.join(', ')));
      throw new Error(`Unable to export: missing ${missingItems.join(', ')}`);
    }

    try {
      
      let exportTheme: 'light' | 'dark' = 'light';
      if (options.theme === 'current') {
        exportTheme = currentTheme === 'dark' ? 'dark' : 'light';
      } else {
        exportTheme = options.theme;
      }


      const resolvedOptions = { ...options, theme: exportTheme };


      let playerPhotoBase64: string | undefined = undefined;
      if (options.includePlayerInfo && player.photoUrl) {
        try {
          if (player.photoUrl.includes('cdn.nba.com') || player.photoUrl.includes('ak-static.cms.nba.com')) {
            const proxyUrl = `https://corsproxy.io/?${encodeURIComponent(player.photoUrl)}`;
            const response = await fetch(proxyUrl);
            if (response.ok) {
              const blob = await response.blob();
              playerPhotoBase64 = await new Promise<string>((resolve) => {
                const reader = new FileReader();
                reader.onloadend = () => resolve(reader.result as string);
                reader.readAsDataURL(blob);
              });
              logger.info('Successfully converted player photo to base64 for export');
            }
          }
        } catch (error) {
          logger.warn('Failed to preload player photo, will use fallback', error as Error);
        }
      }

      setCurrentExportOptions({ ...resolvedOptions, playerPhotoBase64 });


      await new Promise(resolve => setTimeout(resolve, 100));

      
      if (!exportContainerRef.current) {
        logger.error('Export container failed to render', new Error('Container not found'));
        throw new Error('Export container failed to render');
      }

      
      const width = 1200; 
      let height = 20; 

      if (resolvedOptions.includePlayerInfo) height += 140; 
      if (resolvedOptions.includeChart) height += 390; 
      if (resolvedOptions.includeContextCards && exportData?.contextCards.length) height += 75; 
      if (resolvedOptions.includeGameLog && filteredGames.length) {
        const gameCount = Math.min(filteredGames.length, 15);
        height += 70 + (gameCount * 28); 
      }

      
      let sectionCount = 0;
      if (resolvedOptions.includeChart) sectionCount++;
      if (resolvedOptions.includeContextCards) sectionCount++;
      if (resolvedOptions.includeGameLog) sectionCount++;
      height += sectionCount * 20;

      height += 70; 

      
      await new Promise(resolve => setTimeout(resolve, 100));

      
      const container = exportContainerRef.current;
      container.style.position = 'fixed';
      container.style.left = '0';
      container.style.top = '0';
      container.style.zIndex = '9999';
      container.style.display = 'block';

      
      container.style.width = `${width}px`;

      
      await new Promise(resolve => setTimeout(resolve, 1200));

      
      const images = container.querySelectorAll('img');
      const imageConversions: Promise<void>[] = [];

      images.forEach((img) => {
        const src = img.src;

        if (src && !src.startsWith('data:') && !src.startsWith(window.location.origin)) {
          const conversionPromise = (async () => {
            try {

              let fetchUrl = src;
              if (src.includes('cdn.nba.com') || src.includes('ak-static.cms.nba.com')) {
                fetchUrl = `https://corsproxy.io/?${encodeURIComponent(src)}`;
              }

              const response = await fetch(fetchUrl);
              if (!response.ok) throw new Error(`HTTP ${response.status}`);

              const blob = await response.blob();
              const base64 = await new Promise<string>((resolve) => {
                const reader = new FileReader();
                reader.onloadend = () => resolve(reader.result as string);
                reader.readAsDataURL(blob);
              });
              img.src = base64;

            } catch (error) {
              logger.warn('Failed to convert image to base64, triggering onError fallback', { src, error });

              img.dispatchEvent(new Event('error'));
            }
          })();
          imageConversions.push(conversionPromise);
        }
      });



      await Promise.all(imageConversions);


      await new Promise(resolve => setTimeout(resolve, 300));


      const actualHeight = container.scrollHeight;




      const dataUrl = await toPng(container, {
        quality: 1,
        pixelRatio: resolvedOptions.quality,
        width: width,
        cacheBust: false,
        skipFonts: true,
      });

      

      
      const fileName = `${player.name.replace(/\s+/g, '-')}-${selectedStat}-${formatUserDate(selectedDate, dateFormat, true).replace(/\s+/g, '-')}.png`;

      
      if (window.electron) {
        
        const customExportFolder = localStorage.getItem('courtvision-export-folder');

        
        const result = await window.electron.saveImageFile(fileName, dataUrl, customExportFolder || undefined);

        if (result.success && result.filePath) {
          toast.success(`Image saved to ${result.filePath}`);
        } else {
          logger.error('Electron save failed', new Error(result.error || 'Unknown error'));
          toast.error(`Failed to save image: ${result.error || 'Unknown error'}`);
          throw new Error(result.error || 'Unknown error');
        }
      } else {
        
        const link = document.createElement('a');
        link.download = fileName;
        link.href = dataUrl;
        link.click();
        toast.success('Image downloaded successfully');
      }

      
      container.style.display = 'none';
      setCurrentExportOptions(null);
    } catch (error) {
      logger.error('Export failed', error as Error);
      setCurrentExportOptions(null);
      throw error;
    }
  };

  
  const calculateLineValueFromY = useCallback((clientY: number) => {
    if (!chartRef.current) return lineValue;
    
    
    const svg = chartRef.current.querySelector('svg');
    if (!svg) return lineValue;
    
    
    const yAxisTickLabels = Array.from(svg.querySelectorAll('.recharts-cartesian-axis-tick text'));
    
    if (yAxisTickLabels.length >= 2) {
      
      const ticks = yAxisTickLabels.map(tick => {
        const rect = tick.getBoundingClientRect();
        const value = parseFloat(tick.textContent?.replace(/[^0-9.-]/g, '') || '0');
        const y = (rect.top + rect.bottom) / 2; 
        return { value, y };
      });
      
      
      ticks.sort((a, b) => a.y - b.y);
      
      
      let topTick = ticks[0];
      let bottomTick = ticks[ticks.length - 1];
      
      for (let i = 0; i < ticks.length - 1; i++) {
        if (clientY >= ticks[i].y && clientY <= ticks[i + 1].y) {
          topTick = ticks[i];
          bottomTick = ticks[i + 1];
          break;
        }
      }
      
      
      if (clientY < ticks[0].y) {
        topTick = ticks[0];
        bottomTick = ticks.length > 1 ? ticks[1] : ticks[0];
      } else if (clientY > ticks[ticks.length - 1].y) {
        topTick = ticks.length > 1 ? ticks[ticks.length - 2] : ticks[ticks.length - 1];
        bottomTick = ticks[ticks.length - 1];
      }
      
      
      const tickRange = bottomTick.y - topTick.y;
      if (tickRange > 0) {
        const valueRange = bottomTick.value - topTick.value;
        const ratio = (clientY - topTick.y) / tickRange;
        const newValue = topTick.value + ratio * valueRange;
        
        
        const gameValues = historicalGames.map(g => g.stats[selectedStat as keyof typeof g.stats] || 0);
        const maxValue = Math.max(...gameValues, lineValue) * 1.2;
        const minValue = 0;
        
        return Math.round(Math.max(minValue, Math.min(maxValue, newValue)) * 2) / 2;
      }
    }
    
    
    const svgRect = svg.getBoundingClientRect();
    const gameValues = historicalGames.map(g => g.stats[selectedStat as keyof typeof g.stats] || 0);
    const maxValue = Math.max(...gameValues, lineValue) * 1.2;
    const minValue = 0;
    
    const topMargin = 20;
    const bottomMargin = 60;
    const plotHeight = svgRect.height - topMargin - bottomMargin;
    const svgRelativeY = clientY - svgRect.top;
    const plotRelativeY = svgRelativeY - topMargin;
    const clampedY = Math.max(0, Math.min(plotHeight, plotRelativeY));
    
    const valueRange = maxValue - minValue;
    const newValue = maxValue - (clampedY / plotHeight) * valueRange;
    
    return Math.round(newValue * 2) / 2;
  }, [selectedStat, lineValue, historicalGames]);

  
  const handleChartMouseDown = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (!chartRef.current) return;
    setIsDragging(true);
    setLineValue(calculateLineValueFromY(e.clientY));
  }, [calculateLineValueFromY]);

  const handleChartMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (!isDragging) return;
    setLineValue(calculateLineValueFromY(e.clientY));
  }, [isDragging, calculateLineValueFromY]);

  const handleChartMouseUp = useCallback(() => {
    setIsDragging(false);
  }, []);

  
  useEffect(() => {
    if (!isDragging) return;

    const handleGlobalMouseMove = (e: MouseEvent) => {
      setLineValue(calculateLineValueFromY(e.clientY));
    };

    const handleGlobalMouseUp = () => {
      setIsDragging(false);
    };

    window.addEventListener('mousemove', handleGlobalMouseMove);
    window.addEventListener('mouseup', handleGlobalMouseUp);

    return () => {
      window.removeEventListener('mousemove', handleGlobalMouseMove);
      window.removeEventListener('mouseup', handleGlobalMouseUp);
    };
  }, [isDragging, calculateLineValueFromY]);

  
  const filteredGames = useMemo(() => {
    
    let games = [...historicalGames].sort(
      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()
    );

    
    if (excludeDNP) {
      games = games.filter((g) => g.minutesPlayed && g.minutesPlayed > 0);
    }

    
    if (minMinutes > 0) {
      games = games.filter((g) => g.minutesPlayed >= minMinutes);
    }

    
    const currentOpponentAbbr = predictionForPlayer?.opponentAbbr;
    if (h2hOnly && currentOpponentAbbr) {
      games = games.filter((g) => g.opponentAbbr === currentOpponentAbbr);
    }

    
    const currentIsHome = predictionForPlayer?.isHome;
    if (homeAwayOnly && typeof currentIsHome === 'boolean') {
      games = games.filter((g) => g.isHome === currentIsHome);
    }

    
    if (currentTeamOnly && playerInfo.teamId !== null) {
      games = games.filter((g) => g.teamId === playerInfo.teamId);
    }

    
    if (thisSeasonOnly && gameInfo.season) {
      games = games.filter((g) => g.season === gameInfo.season);
    }

    
    const windowMap: Record<string, number> = {
      L5: 5,
      L10: 10,
      L20: 20,
      L50: 50,
    };
    if (timeWindow !== 'All' && windowMap[timeWindow]) {
      games = games.slice(0, windowMap[timeWindow]);
    }

    return games;
  }, [
    historicalGames,
    excludeDNP,
    minMinutes,
    timeWindow,
    h2hOnly,
    homeAwayOnly,
    currentTeamOnly,
    thisSeasonOnly,
    predictionForPlayer?.opponentAbbr,
    predictionForPlayer?.isHome,
    playerInfo.teamId,
    gameInfo.season,
  ]);

  
  const historicalValues = filteredGames.map(g => g.stats[selectedStat as keyof typeof g.stats] || 0);
  const mean = historicalValues.length > 0
    ? historicalValues.reduce((sum, v) => sum + v, 0) / historicalValues.length
    : 20;

  
  const aiPrediction = predictionForPlayer
    ? Number(
        predictionForPlayer.predictedStats[
          selectedStat as keyof typeof predictionForPlayer.predictedStats
        ] || 0
      )
    : Math.round(mean * 10) / 10;

  
  useEffect(() => {
    if (aiPrediction > 0) {
      setLineValue(Math.floor(aiPrediction));
    }
  }, [selectedPlayer, selectedStat, aiPrediction]);

  
  
  const chartData = useMemo(() => {
    
    if (isLoadingHistoricalGames && historicalGames.length === 0) {
      return [];
    }
    
    return [
      ...filteredGames.map(game => ({
        name: `${game.date.split('-').slice(1).join('/')} vs ${game.opponentAbbr}`,
        value: game.stats[selectedStat as keyof typeof game.stats] || 0,
        hit: overUnder === 'over'
          ? (game.stats[selectedStat as keyof typeof game.stats] || 0) >= lineValue
          : (game.stats[selectedStat as keyof typeof game.stats] || 0) < lineValue,
        isAIPrediction: false,
      })).reverse(),
      {
        name: predictionForPlayer
          ? `AI vs ${predictionForPlayer.opponentAbbr}`
          : 'AI Prediction',
        value: aiPrediction,
        hit: overUnder === 'over' ? aiPrediction >= lineValue : aiPrediction < lineValue,
        isAIPrediction: true,
      }
    ];
  }, [filteredGames, selectedStat, overUnder, lineValue, predictionForPlayer, aiPrediction, isLoadingHistoricalGames, historicalGames.length]);

  
  const historicalData = chartData.filter(d => !d.isAIPrediction);
  const hitRate = historicalData.length > 0
    ? Math.round((historicalData.filter(d => d.hit).length / historicalData.length) * 100)
    : 0;

  
  const statNameForHooks = selectedStat === 'threePointersMade' ? 'threePointersMade' : selectedStat;

  
  const opponentDefense = useOpponentDefense(
    gameInfo.opponentId,
    gameInfo.season,
    playerInfo.position,
    statNameForHooks
  );

  const starPlayersOut = useStarPlayersOut(
    player?.id ? Number(player.id) : null,
    playerInfo.teamId,
    gameInfo.season,
    gameInfo.gameDate,
    statNameForHooks
  );

  const restDays = useRestDays(
    playerInfo.teamId,
    gameInfo.opponentId,
    gameInfo.gameDate,
    gameInfo.season
  );

  const playoffExperience = usePlayoffExperience(
    player?.id ? Number(player.id) : null,
    statNameForHooks,
    gameInfo.gameType
  );

  const paceComparison = usePaceComparison(
    playerInfo.teamId,
    gameInfo.opponentId,
    gameInfo.season
  );

  
  const statDisplayNames: Record<string, string> = {
    points: 'points',
    rebounds: 'rebounds',
    assists: 'assists',
    steals: 'steals',
    blocks: 'blocks',
    turnovers: 'turnovers',
    threePointersMade: 'three-pointers made',
  };

  const statDisplayName = statDisplayNames[selectedStat] || selectedStat;

  
  const exportData = useMemo(() => {
    if (!player || !selectedGameObj) return undefined;

    
    const contextCards = [];

    
    if (opponentDefense.data) {
      contextCards.push({
        title: 'Opponent Defense',
        content: `${opponentDefense.data.teamAbbr} ${opponentDefense.data.rankDirection} ${statDisplayName} to ${playerInfo.position || 'players'} (${opponentDefense.data.value.toFixed(1)}/gm)`
      });
    } else {
      contextCards.push({
        title: 'Opponent Defense',
        content: 'Insufficient data'
      });
    }

    
    if (starPlayersOut.data) {
      if (starPlayersOut.data.starsOutNames && starPlayersOut.data.starsOutNames.length > 0) {
        if (starPlayersOut.data.totalGames && starPlayersOut.data.totalGames >= 3 && !starPlayersOut.data.insufficientData) {
          const change = starPlayersOut.data.totalChange || 0;
          const moreLess = change >= 0 ? 'more' : 'less';
          contextCards.push({
            title: 'Star Players Out',
            content: `${starPlayersOut.data.starsOutNames.join(', ')} out. Averages ${Math.abs(change).toFixed(1)} ${moreLess} ${statDisplayName} (${starPlayersOut.data.totalGames} games)`
          });
        } else {
          contextCards.push({
            title: 'Star Players Out',
            content: `${starPlayersOut.data.starsOutNames.join(', ')} out (insufficient data)`
          });
        }
      } else {
        contextCards.push({
          title: 'Star Players Out',
          content: 'No star teammates out'
        });
      }
    } else {
      contextCards.push({
        title: 'Star Players Out',
        content: 'No star teammates out'
      });
    }

    
    if (restDays.data && restDays.data.teamRest !== null && restDays.data.opponentRest !== null) {
      contextCards.push({
        title: 'Rest Days',
        content: `Team has ${restDays.data.teamRest} day${restDays.data.teamRest !== 1 ? 's' : ''} rest. Opponent has ${restDays.data.opponentRest} day${restDays.data.opponentRest !== 1 ? 's' : ''} rest.`
      });
    } else {
      contextCards.push({
        title: 'Rest Days',
        content: 'Insufficient data'
      });
    }

    
    if (playoffExperience.data) {
      if (!playoffExperience.data.isPlayoff) {
        contextCards.push({
          title: 'Playoff Experience',
          content: 'Regular season game - playoff experience has minimal impact'
        });
      } else if (playoffExperience.data.insufficientData) {
        contextCards.push({
          title: 'Playoff Experience',
          content: 'Insufficient playoff data to determine impact'
        });
      } else if (playoffExperience.data.change !== undefined) {
        const change = playoffExperience.data.change;
        contextCards.push({
          title: 'Playoff Experience',
          content: `Averages ${change >= 0 ? '+' : ''}${change.toFixed(1)} ${statDisplayName} in playoffs vs regular season (${playoffExperience.data.playoffGames || 'few'} games)`
        });
      } else {
        contextCards.push({
          title: 'Playoff Experience',
          content: 'Insufficient data'
        });
      }
    } else {
      contextCards.push({
        title: 'Playoff Experience',
        content: 'Insufficient data'
      });
    }

    
    if (paceComparison.data) {
      const teamPace = paceComparison.data.teamPace;
      const oppPace = paceComparison.data.oppPace;
      const avgPace = (teamPace + oppPace) / 2;

      if (paceComparison.data.impact) {
        const paceImpact = paceComparison.data.impact;
        contextCards.push({
          title: 'Pace Comparison',
          content: `${paceImpact.charAt(0).toUpperCase() + paceImpact.slice(1)} pace game. Combined pace: ${avgPace.toFixed(1)} (${teamPace.toFixed(1)} vs ${oppPace.toFixed(1)})`
        });
      } else if (paceComparison.data.pctDiff !== undefined) {
        const pctDiffAbs = Math.abs(paceComparison.data.pctDiff);
        const pctDiffRounded = Math.round(pctDiffAbs * 10) / 10;
        if (pctDiffAbs < 2) {
          contextCards.push({
            title: 'Pace Comparison',
            content: `Neutral pace vs league avg. Combined pace: ${avgPace.toFixed(1)} (${teamPace.toFixed(1)} vs ${oppPace.toFixed(1)})`
          });
        } else {
          const direction = paceComparison.data.pctDiff > 0 ? 'faster' : 'slower';
          contextCards.push({
            title: 'Pace Comparison',
            content: `${pctDiffRounded.toFixed(1)}% ${direction} vs league avg. Combined pace: ${avgPace.toFixed(1)} (${teamPace.toFixed(1)} vs ${oppPace.toFixed(1)})`
          });
        }
      } else {
        contextCards.push({
          title: 'Pace Comparison',
          content: 'Insufficient data'
        });
      }
    } else {
      contextCards.push({
        title: 'Pace Comparison',
        content: 'Insufficient data'
      });
    }

    return {
      playerName: player.name,
      teamName: player.team,
      playerPhoto: player.photoUrl,
      stat: statOptions.find(s => s.value === selectedStat)?.label || selectedStat,
      lineValue,
      prediction: overUnder,
      confidence: predictionForPlayer ? Math.round(predictionForPlayer.confidence * 100) : undefined,
      aiPrediction,
      hitRate,
      chartData,
      filteredGames,
      selectedStat,
      overUnder,
      contextCards,
      statDisplayName,
    };
  }, [player, selectedGameObj, opponentDefense.data, restDays.data, starPlayersOut.data, playoffExperience.data, paceComparison.data, lineValue, overUnder, predictionForPlayer, aiPrediction, hitRate, chartData, filteredGames, selectedStat, statDisplayName]);

  
  const ContextCard = ({ 
    icon: Icon, 
    title, 
    content,
    tooltip
  }: { 
    icon: any; 
    title: string; 
    content: string | React.ReactNode;
    tooltip?: string;
  }) => {
    return (
      <div className="stat-card" style={{ padding: 'var(--density-card-padding, 1rem)' }}>
        <div className="flex items-start gap-2.5">
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-primary/10">
            <Icon className="h-4 w-4 text-primary" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-1.5 mb-0.5">
              <h4 className="text-sm font-semibold text-foreground">{title}</h4>
              {tooltip && (
                <Tooltip delayDuration={200} disableHoverableContent>
                  <TooltipTrigger asChild>
                    <span 
                      className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground transition-colors cursor-help select-none"
                      onPointerDown={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                      }}
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                      }}
                    >
                      <HelpCircle className="h-3.5 w-3.5 pointer-events-none shrink-0" />
                    </span>
                  </TooltipTrigger>
                  <TooltipContent 
                    side="top" 
                    className="max-w-xs"
                    onPointerDown={(e) => e.stopPropagation()}
                    onClick={(e) => e.stopPropagation()}
                  >
                    <p className="text-xs">{tooltip}</p>
                  </TooltipContent>
                </Tooltip>
              )}
            </div>
            <div className="text-sm text-muted-foreground leading-relaxed [&_span]:text-sm">{content}</div>
          </div>
        </div>
      </div>
    );
  };

  
  if (isLoadingPredictions) {
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">Player Analysis</h1>
            <p className="text-muted-foreground">Analyze historical performance and betting lines</p>
          </div>
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                className="gap-2 relative group overflow-hidden transition-all duration-300 hover:border-primary/50"
              >
                <Calendar className="h-4 w-4 relative z-10 shrink-0" />
                <span className="relative z-10 whitespace-nowrap">{formatUserDate(selectedDate, dateFormat, true)}</span>
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="end">
              <CalendarComponent
                mode="single"
                selected={selectedDate}
                onSelect={(date) => date && setSelectedDate(date)}
                disabled={(date) => date > new Date()}
                initialFocus
                className="pointer-events-auto"
              />
            </PopoverContent>
          </Popover>
        </div>
        <div className="flex items-center justify-center py-24">
          <div className="text-center">
            <Brain className="mx-auto h-12 w-12 text-muted-foreground mb-4 animate-pulse shrink-0" />
            <p className="text-muted-foreground">Loading predictions...</p>
          </div>
        </div>
      </div>
    );
  }

  if (games.length === 0 || gameOptions.length === 0) {
    const hasStoredGame = localStorage.getItem('player-analysis-selected-game');
    
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">Player Analysis</h1>
            <p className="text-muted-foreground">Analyze historical performance and betting lines</p>
          </div>
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                className="gap-2 relative group overflow-hidden transition-all duration-300 hover:border-primary/50"
              >
                <Calendar className="h-4 w-4 relative z-10 shrink-0" />
                <span className="relative z-10 whitespace-nowrap">{formatUserDate(selectedDate, dateFormat, true)}</span>
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="end">
              <CalendarComponent
                mode="single"
                selected={selectedDate}
                onSelect={(date) => date && setSelectedDate(date)}
                disabled={(date) => date > new Date()}
                initialFocus
                className="pointer-events-auto"
              />
            </PopoverContent>
          </Popover>
        </div>
        <div className="rounded-xl border border-border bg-card p-12 text-center">
          <Brain className="mx-auto h-12 w-12 text-muted-foreground mb-4 shrink-0" />
          <h3 className="text-lg font-semibold text-foreground mb-2">No Predictions Available</h3>
          <p className="text-muted-foreground mb-4">
            {hasStoredGame 
              ? `No predictions found for ${formatUserDate(selectedDate, dateFormat, true)}. Try selecting a different date that has predictions.`
              : `Predictions have not been generated yet for ${formatUserDate(selectedDate, dateFormat, true)}. Please check back later or select a different date.`
            }
          </p>
          {hasStoredGame && (
            <Button 
              onClick={() => {
                setSelectedDate(new Date());
                localStorage.removeItem('player-analysis-selected-game');
                localStorage.removeItem('player-analysis-selected-player');
                localStorage.removeItem('player-analysis-selected-stat');
              }}
              variant="outline"
              className="mt-4"
            >
              Go to Today's Predictions
            </Button>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in">
      
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0 flex-1">
          <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Player Analysis</h1>
          <p className="text-muted-foreground leading-tight truncate">Analyze historical performance and betting lines</p>
        </div>

        
        <Popover>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              className="gap-2 relative group overflow-hidden transition-all duration-300 hover:border-primary/50 shrink-0"
            >
              <div className="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/10 to-primary/0 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 blur-xl bg-primary/20" />
              <Calendar className="h-4 w-4 relative z-10 shrink-0" />
              <span className="relative z-10 whitespace-nowrap">{formatUserDate(selectedDate, dateFormat, true)}</span>
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-auto p-0" align="end">
            <CalendarComponent
              mode="single"
              selected={selectedDate}
              onSelect={(date) => date && setSelectedDate(date)}
              disabled={(date) => date > new Date()}
              initialFocus
              className="pointer-events-auto"
            />
          </PopoverContent>
        </Popover>
      </div>

      
      <div className="grid gap-4 md:grid-cols-3 rounded-xl bg-card/50 p-4 border border-border">
        <div className="space-y-2">
          <Label>Game</Label>
          <Combobox
            options={gameOptions}
            value={selectedGame}
            onValueChange={setSelectedGame}
            getLabel={(g) => g.label}
            getValue={(g) => g.id}
            placeholder="Select game"
            searchPlaceholder="Search games (e.g., DET, Detroit, Pistons, BOS)..."
            emptyMessage="No games found"
            disabled={isLoadingPredictions || !gameOptions.length}
            filterFn={gameFilterFn}
          />
        </div>

        <div className="space-y-2">
          <Label>Player</Label>
          <Combobox
            options={playerOptions}
            value={selectedPlayer}
            onValueChange={setSelectedPlayer}
            getLabel={(p) => `${p.name} (${p.teamAbbr})`}
            getValue={(p) => p.id}
            placeholder="Select player"
            searchPlaceholder="Search players..."
            emptyMessage="No players found"
            disabled={!playerOptions.length}
            filterFn={playerFilterFn}
          />
        </div>

        <div className="space-y-2">
          <Label>Statistic</Label>
          <Select value={selectedStat} onValueChange={setSelectedStat}>
            <SelectTrigger>
              <SelectValue placeholder="Select stat" />
            </SelectTrigger>
            <SelectContent>
              {statOptions.map(stat => (
                <SelectItem key={stat.value} value={stat.value}>
                  {stat.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        
        <div className="space-y-3">
          
          {player && (
            <div className="stat-card overflow-hidden">
              <div className="relative h-32 bg-gradient-to-br from-secondary to-muted">
                <img
                  src={player.photoUrl}
                  alt={player.name}
                  className="absolute right-0 bottom-0 h-36 w-auto object-cover"
                  onError={(e) => {
                    e.currentTarget.src = '/player-placeholder.png';
                  }}
                />
              </div>
              <div className="p-4">
                <h2 className="text-xl font-bold text-foreground">{player.name}</h2>
                <p className="text-muted-foreground">{player.position} · {player.team}</p>
              </div>
            </div>
          )}

          
          <Collapsible open={optionsOpen} onOpenChange={setOptionsOpen}>
            <div className="stat-card">
              <CollapsibleTrigger className="flex items-center justify-between w-full">
                <h3 className="font-semibold text-foreground">Options</h3>
                <ChevronDown className={cn(
                  "h-4 w-4 text-muted-foreground transition-transform duration-200",
                  optionsOpen && "rotate-180"
                )} />
              </CollapsibleTrigger>
              <CollapsibleContent className="space-y-4 pt-4">
                <div className="space-y-2">
                  <Label className="text-muted-foreground">Time Window</Label>
                  <RadioGroup value={timeWindow} onValueChange={setTimeWindow} className="flex flex-wrap gap-2">
                    {timeWindows.map(tw => (
                      <div key={tw} className="flex items-center">
                        <RadioGroupItem value={tw} id={tw} className="peer sr-only" />
                        <Label
                          htmlFor={tw}
                          className={cn(
                            'cursor-pointer rounded-lg border border-border px-3 py-1.5 text-sm transition-all',
                            timeWindow === tw
                              ? 'bg-primary text-primary-foreground border-primary'
                              : 'hover:bg-secondary'
                          )}
                        >
                          {tw}
                        </Label>
                      </div>
                    ))}
                  </RadioGroup>
                </div>

                <div className="space-y-2">
                  <Label className="text-muted-foreground">Over/Under</Label>
                  <RadioGroup value={overUnder} onValueChange={(v) => setOverUnder(v as 'over' | 'under')} className="flex gap-2">
                    <div className="flex items-center">
                      <RadioGroupItem value="over" id="over" className="peer sr-only" />
                      <Label
                        htmlFor="over"
                        className={cn(
                          'cursor-pointer rounded-lg border px-4 py-2 text-sm transition-all flex items-center gap-2',
                          overUnder === 'over'
                            ? 'bg-success/20 text-success border-success/40'
                            : 'border-border hover:bg-secondary'
                        )}
                      >
                        <TrendingUp className="h-4 w-4 shrink-0" />
                        <span className="whitespace-nowrap">Over</span>
                      </Label>
                    </div>
                    <div className="flex items-center">
                      <RadioGroupItem value="under" id="under" className="peer sr-only" />
                      <Label
                        htmlFor="under"
                        className={cn(
                          'cursor-pointer rounded-lg border px-4 py-2 text-sm transition-all flex items-center gap-2',
                          overUnder === 'under'
                            ? 'bg-destructive/20 text-destructive border-destructive/40'
                            : 'border-border hover:bg-secondary'
                        )}
                      >
                        <TrendingDown className="h-4 w-4 shrink-0" />
                        <span className="whitespace-nowrap">Under</span>
                      </Label>
                    </div>
                  </RadioGroup>
                </div>
              </CollapsibleContent>
            </div>
          </Collapsible>

          
          <Collapsible open={filtersOpen} onOpenChange={setFiltersOpen}>
            <div className="stat-card">
              <CollapsibleTrigger className="flex items-center justify-between w-full">
                <h3 className="font-semibold text-foreground">Filters</h3>
                <ChevronDown className={cn(
                  "h-4 w-4 text-muted-foreground transition-transform duration-200",
                  filtersOpen && "rotate-180"
                )} />
              </CollapsibleTrigger>
              <CollapsibleContent className="space-y-4 pt-4">
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <Checkbox id="excludeDNP" checked={excludeDNP} onCheckedChange={(c) => setExcludeDNP(!!c)} />
                    <Label htmlFor="excludeDNP" className="text-sm cursor-pointer">Exclude DNP (0 min)</Label>
                  </div>
                <div className="flex items-center gap-2">
                    <Checkbox id="h2h" checked={h2hOnly} onCheckedChange={(c) => setH2hOnly(!!c)} />
                    <Label htmlFor="h2h" className="text-sm cursor-pointer">H2H Only</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="homeAway"
                      checked={homeAwayOnly}
                      onCheckedChange={(c) => setHomeAwayOnly(!!c)}
                      disabled={!predictionForPlayer}
                    />
                    <Label
                      htmlFor="homeAway"
                      className="text-sm cursor-pointer"
                    >
                      {predictionForPlayer?.isHome ? 'Home Games Only' : 'Away Games Only'}
                    </Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="currentTeamOnly"
                      checked={currentTeamOnly}
                      onCheckedChange={(c) => setCurrentTeamOnly(!!c)}
                      disabled={!playerInfo.teamId}
                    />
                    <Label htmlFor="currentTeamOnly" className="text-sm cursor-pointer">Current Team Only</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="thisSeasonOnly"
                      checked={thisSeasonOnly}
                      onCheckedChange={(c) => setThisSeasonOnly(!!c)}
                      disabled={!gameInfo.season}
                    />
                    <Label htmlFor="thisSeasonOnly" className="text-sm cursor-pointer">This Season Only</Label>
                  </div>
                </div>

                <div className="space-y-2">
                  <Label className="text-muted-foreground">Min. Minutes</Label>
                  <Select value={minMinutes.toString()} onValueChange={(v) => setMinMinutes(parseInt(v))}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {[0, 5, 10, 15, 20, 25, 30].map(m => (
                        <SelectItem key={m} value={m.toString()}>{m} min</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </CollapsibleContent>
            </div>
          </Collapsible>

          
          {opponentDefense.isError ? (
            <ContextCard
              icon={Shield}
              title="Opponent Defense"
              content="Error loading opponent defense data."
            />
          ) : opponentDefense.data ? (
            <ContextCard
              icon={Shield}
              title="Opponent Defense"
              content={
                <span>
                  {opponentDefense.data.teamAbbr} <span className="font-semibold text-foreground">{opponentDefense.data.rankDirection}</span> {selectedStat === 'turnovers' ? 'turnovers' : statDisplayName} per game to {playerInfo.position || 'players'} in the league (<span className="font-semibold text-foreground">{opponentDefense.data.value.toFixed(1)}</span> per game).
                </span>
              }
              tooltip={gameInfo.gameDate && new Date(gameInfo.gameDate) < new Date() ? "Based on current season data, not historical ranking at time of game" : undefined}
            />
          ) : (
            <ContextCard
              icon={Shield}
              title="Opponent Defense"
              content="Insufficient data to determine opponent's position defense ranking."
            />
          )}

          {starPlayersOut.isError ? (
            <ContextCard
              icon={Users}
              title="Star Players Out"
              content="Error loading star players data."
            />
          ) : starPlayersOut.data ? (
            starPlayersOut.data.insufficientData ? (
              <ContextCard
                icon={Users}
                title="Star Players Out"
                content={
                  <span>
                    Star teammates out: <span className="font-semibold text-foreground">{starPlayersOut.data.starsOutNames.join(', ')}</span>. Insufficient data to determine impact (at least 3 games without the star player are required).
                  </span>
                }
              />
            ) : starPlayersOut.data.totalGames && starPlayersOut.data.totalGames >= 3 ? (
              <ContextCard
                icon={Users}
                title="Star Players Out"
                content={
                  <span>
                    Star teammates out: <span className="font-semibold text-foreground">{starPlayersOut.data.starsOutNames.join(', ')}</span>. When these players are out, {player?.name || 'Player'} averages <span className="font-semibold text-foreground">{Math.abs(starPlayersOut.data.totalChange || 0).toFixed(1)}</span> {starPlayersOut.data.totalChange && starPlayersOut.data.totalChange >= 0 ? 'more' : 'less'} {statDisplayName} per game ({starPlayersOut.data.totalGames} games).
                  </span>
                }
              />
            ) : (
              <ContextCard
                icon={Users}
                title="Star Players Out"
                content={
                  <span>
                    Star teammates out: <span className="font-semibold text-foreground">{starPlayersOut.data.starsOutNames.join(', ')}</span>. Insufficient data to determine impact (at least 3 games without the star player are required).
                  </span>
                }
              />
            )
          ) : (
            <ContextCard
              icon={Users}
              title="Star Players Out"
              content="No star teammates currently out."
            />
          )}

          {restDays.isError ? (
            <ContextCard
              icon={Clock}
              title="Rest Days"
              content="Error loading rest days data."
            />
          ) : restDays.data && restDays.data.teamRest !== null && restDays.data.opponentRest !== null ? (
            <ContextCard
              icon={Clock}
              title="Rest Days"
              content={
                <span>
                  Team: <span className="font-semibold text-foreground">{restDays.data.teamRest}</span> days rest<br />
                  Opponent: <span className="font-semibold text-foreground">{restDays.data.opponentRest}</span> days rest
                </span>
              }
            />
          ) : (
            <ContextCard
              icon={Clock}
              title="Rest Days"
              content="Insufficient data to determine rest days."
            />
          )}

          {playoffExperience.isError ? (
            <ContextCard
              icon={Trophy}
              title="Playoff Experience"
              content="Error loading playoff experience data."
            />
          ) : playoffExperience.data ? (
            !playoffExperience.data.isPlayoff ? (
              <ContextCard
                icon={Trophy}
                title="Playoff Experience"
                content="The current game is not a playoff game, so playoff experience has negligible impact."
              />
            ) : playoffExperience.data.insufficientData ? (
              <ContextCard
                icon={Trophy}
                title="Playoff Experience"
                content={`${player?.name || 'Player'} does not have enough playoff experience to determine post-season influence on ${statDisplayName}.`}
              />
            ) : (
              <ContextCard
                icon={Trophy}
                title="Playoff Experience"
                content={
                  <span>
                    In playoff games, {player?.name || 'Player'} averages <span className="font-semibold text-foreground">{playoffExperience.data.change && playoffExperience.data.change >= 0 ? '+' : ''}{playoffExperience.data.change?.toFixed(1)}</span> {statDisplayName} per game compared to the regular season ({playoffExperience.data.playoffGames} playoff games).
                  </span>
                }
              />
            )
          ) : (
            <ContextCard
              icon={Trophy}
              title="Playoff Experience"
              content="Loading playoff experience data..."
            />
          )}

          {paceComparison.isError ? (
            <ContextCard
              icon={Zap}
              title="Pace Comparison"
              content="Error loading pace comparison data."
            />
          ) : paceComparison.data ? (
            (() => {
              const pctDiffAbs = Math.abs(paceComparison.data.pctDiff);
              const pctDiffRounded = Math.round(pctDiffAbs * 10) / 10;
              const isHistorical = gameInfo.gameDate && new Date(gameInfo.gameDate) < new Date();
              const tooltipText = isHistorical ? "Based on current season data, not historical pace at time of game" : undefined;
              
              if (pctDiffAbs < 0.01 || pctDiffRounded === 0.0) {
                return (
                  <ContextCard
                    icon={Zap}
                    title="Pace Comparison"
                    content={
                      <span>
                        {paceComparison.data.teamName} plays at a pace of <span className="font-semibold text-foreground">{paceComparison.data.teamPace.toFixed(1)}</span> and {paceComparison.data.oppName} plays at a pace of <span className="font-semibold text-foreground">{paceComparison.data.oppPace.toFixed(1)}</span>. This matches the league average pace, so expect a normal number of possessions.
                      </span>
                    }
                    tooltip={tooltipText}
                  />
                );
              } else {
                const direction = paceComparison.data.pctDiff > 0 ? 'higher' : 'lower';
                const moreLess = paceComparison.data.pctDiff > 0 ? 'more' : 'less';
                return (
                  <ContextCard
                    icon={Zap}
                    title="Pace Comparison"
                    content={
                      <span>
                        {paceComparison.data.teamName} plays at a pace of <span className="font-semibold text-foreground">{paceComparison.data.teamPace.toFixed(1)}</span> and {paceComparison.data.oppName} plays at a pace of <span className="font-semibold text-foreground">{paceComparison.data.oppPace.toFixed(1)}</span>. This is <span className="font-semibold text-foreground">{pctDiffRounded.toFixed(1)}%</span> {direction} than the league average pace, which may result in {moreLess} possessions.
                      </span>
                    }
                    tooltip={tooltipText}
                  />
                );
              }
            })()
          ) : (
            <ContextCard
              icon={Zap}
              title="Pace Comparison"
              content="Insufficient data to determine pace comparison."
            />
          )}
        </div>

        
        <div className="lg:col-span-2 space-y-4">
          
          <div className="stat-card">
            <div className="grid gap-6 md:grid-cols-[auto_1fr_auto_auto] items-center">
              
              <div className="space-y-2">
                <Label className="text-sm font-medium text-muted-foreground">Line Value</Label>
                <Input
                  type="number"
                  step={0.5}
                  value={lineValue}
                  onChange={(e) => setLineValue(parseFloat(e.target.value))}
                  className="w-28 text-lg font-semibold"
                />
              </div>

              
              <div className="grid grid-cols-2 gap-6">
                <div>
                  <p className="text-xs font-medium text-muted-foreground mb-1">Hit Rate</p>
                  <p className={cn(
                    'text-3xl font-bold',
                    hitRate >= 60 ? 'text-success' : hitRate >= 40 ? 'text-warning' : 'text-destructive'
                  )}>
                    {hitRate}%
                  </p>
                </div>
                <div>
                  <p className="text-xs font-medium text-muted-foreground mb-1">AI Prediction</p>
                  <p className="text-3xl font-bold text-primary">
                    {aiPrediction.toFixed(1)}
                  </p>
                </div>
              </div>

              
              <div className="flex gap-2 md:col-span-2 md:col-start-3 md:row-start-1">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      onClick={() => {
                        if (!predictionForPlayer) {
                          toast.error('No AI prediction found for this player/game.');
                          return;
                        }
                        if (gameStatus === 'completed') {
                          toast.error('Cannot save picks for completed games.');
                          return;
                        }
                        savePickMutation.mutate(
                          {
                            playerId: predictionForPlayer.playerId,
                            gameId: predictionForPlayer.gameId,
                            statName: selectedStat,
                            lineValue,
                            overUnder,
                          },
                          {
                            onSuccess: () => {
                              toast.success(
                                `Saved pick: ${predictionForPlayer.player.name} ${selectedStat} ${overUnder} ${lineValue.toFixed(1)}`
                              );
                            },
                            onError: (error: any) => {
                              const message =
                                error?.message || 'Could not save pick. Please try again.';
                              toast.error(message);
                            },
                          }
                        );
                      }}
                      disabled={savePickMutation.isPending || !predictionForPlayer || gameStatus === 'completed'}
                      className="gap-2"
                      size="default"
                    >
                      <Save className="h-4 w-4 shrink-0" />
                      <span className="whitespace-nowrap">{savePickMutation.isPending ? 'Saving...' : 'Save Pick'}</span>
                    </Button>
                  </TooltipTrigger>
                  {gameStatus === 'completed' && (
                    <TooltipContent>
                      <p>Cannot save picks for completed games</p>
                    </TooltipContent>
                  )}
                </Tooltip>

                <Button
                  onClick={() => setExportModalOpen(true)}
                  variant="outline"
                  className="gap-2"
                  size="default"
                >
                  <Image className="h-4 w-4 shrink-0" />
                  <span className="whitespace-nowrap">Export</span>
                </Button>
              </div>
            </div>
          </div>

          
          <div className="stat-card">
            <div className="flex items-center gap-2 mb-4">
              <h3 className="font-semibold text-foreground">
                Historical Performance: {statOptions.find(s => s.value === selectedStat)?.label}
              </h3>
              <div className="flex items-center gap-1.5 ml-auto text-xs text-muted-foreground">
                <div className="w-3 h-3 rounded-sm bg-success" />
                <span>Hit</span>
                <div className="w-3 h-3 rounded-sm bg-destructive ml-2" />
                <span>Miss</span>
                <div className="w-3 h-3 rounded-sm bg-primary ml-2" />
                <span>AI</span>
              </div>
            </div>
            <div className="flex gap-2">
              <div 
                ref={chartRef}
                className={cn(
                  "h-[300px] flex-1",
                  isDragging ? "cursor-ns-resize" : "cursor-crosshair"
                )}
                onMouseDown={handleChartMouseDown}
                onMouseMove={handleChartMouseMove}
                onMouseUp={handleChartMouseUp}
                onMouseLeave={handleChartMouseUp}
              >
              {isLoadingHistoricalGames && historicalGames.length === 0 ? (
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto mb-2"></div>
                    <p className="text-sm">Loading historical games...</p>
                  </div>
                </div>
              ) : isErrorHistoricalGames ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center max-w-md p-6 rounded-xl border border-border bg-card">
                    <AlertCircle className="mx-auto h-10 w-10 text-muted-foreground mb-3 shrink-0" />
                    <h3 className="text-base font-semibold text-foreground mb-2">Error loading historical games</h3>
                    <p className="text-sm text-muted-foreground mb-4">
                      {errorHistoricalGames?.message || 'There was a problem fetching historical data. Please try again.'}
                    </p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => window.location.reload()}
                      className="gap-2"
                    >
                      <RefreshCw className="h-4 w-4 shrink-0" />
                      Try Again
                    </Button>
                  </div>
                </div>
              ) : (
                <>
              <style>{`
                .recharts-tooltip-wrapper .recharts-default-tooltip {
                  background-color: hsl(var(--card)) !important;
                  border: 1px solid hsl(var(--border)) !important;
                  border-radius: 8px !important;
                }
                .recharts-bar-rectangle {
                  transition: filter 0.2s ease;
                }
              `}</style>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 20, right: 10, left: 0, bottom: 60 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                  <XAxis 
                    dataKey="name" 
                    tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                  />
                  <YAxis tick={{ fill: 'hsl(var(--muted-foreground))' }} />
                  <RechartsTooltip
                    cursor={false}
                    contentStyle={{
                      backgroundColor: 'hsl(var(--card))',
                      border: '1px solid hsl(var(--border))',
                      borderRadius: '8px',
                      color: 'hsl(var(--foreground))',
                    }}
                    labelStyle={{ color: 'hsl(var(--foreground))' }}
                    itemStyle={{ color: 'hsl(var(--foreground))' }}
                    formatter={(value: number, name: string, props: any) => {
                      const entry = props.payload;
                      const label = entry.isAIPrediction ? 'AI Prediction' : (entry.hit ? 'Hit' : 'Miss');
                      return [`${value} (${label})`, statOptions.find(s => s.value === selectedStat)?.label || 'Value'];
                    }}
                  />
                  <ReferenceLine 
                    y={lineValue} 
                    stroke="hsl(var(--primary))" 
                    strokeDasharray="5 5" 
                    strokeWidth={2}
                    className={isDragging ? "opacity-100" : ""}
                  />
                  <Bar 
                    dataKey="value" 
                    radius={[4, 4, 0, 0]}
                    shape={(props: any) => {
                      const { x, y, width, height, payload } = props;
                      const isAI = payload.isAIPrediction;
                      const fill = isAI 
                        ? 'hsl(var(--primary))' 
                        : payload.hit 
                          ? 'hsl(var(--success))' 
                          : 'hsl(var(--destructive))';
                      const glowColor = isAI 
                        ? 'hsl(var(--primary))' 
                        : payload.hit 
                          ? 'hsl(var(--success))' 
                          : 'hsl(var(--destructive))';
                      
                      return (
                        <g className="group cursor-pointer">
                          <defs>
                            <filter id={`glow-${props.index}`} x="-50%" y="-50%" width="200%" height="200%">
                              <feGaussianBlur stdDeviation="4" result="coloredBlur"/>
                              <feMerge>
                                <feMergeNode in="coloredBlur"/>
                                <feMergeNode in="SourceGraphic"/>
                              </feMerge>
                            </filter>
                          </defs>
                          <Rectangle 
                            {...props}
                            fill={fill}
                            className="transition-all duration-200 hover:brightness-125"
                            style={{
                              filter: 'none',
                            }}
                            onMouseEnter={(e: any) => {
                              e.target.style.filter = `drop-shadow(0 0 8px ${glowColor})`;
                            }}
                            onMouseLeave={(e: any) => {
                              e.target.style.filter = 'none';
                            }}
                          />
                        </g>
                      );
                    }}
                  >
                    {chartData.map((entry, index) => (
                      <Cell 
                        key={`cell-${index}`} 
                        fill={entry.isAIPrediction 
                          ? 'hsl(var(--primary))' 
                          : entry.hit 
                            ? 'hsl(var(--success))' 
                            : 'hsl(var(--destructive))'
                        } 
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
                </>
              )}
              </div>
            </div>
          </div>



          <GameLogTable
            games={filteredGames}
            selectedStat={selectedStat}
            lineValue={lineValue}
            overUnder={overUnder}
            isLoading={isLoadingHistoricalGames}
            isError={isErrorHistoricalGames}
            error={errorHistoricalGames}
          />
        </div>
      </div>

      
      <ExportImageModal
        open={exportModalOpen}
        onOpenChange={setExportModalOpen}
        onExport={handleExport}
        exportData={exportData}
      />

      
      {currentExportOptions && player && (
        <div ref={exportContainerRef} style={{ display: 'none' }}>
          <ExportLayout
            playerName={currentExportOptions.includePlayerInfo ? player.name : undefined}
            teamName={currentExportOptions.includePlayerInfo ? player.team : undefined}
            playerPhoto={currentExportOptions.includePlayerInfo ? ((currentExportOptions as any).playerPhotoBase64 || player.photoUrl) : undefined}
            stat={currentExportOptions.includePlayerInfo ? statOptions.find(s => s.value === selectedStat)?.label : undefined}
            lineValue={currentExportOptions.includePlayerInfo ? lineValue : undefined}
            prediction={currentExportOptions.includePlayerInfo ? overUnder : undefined}
            hitRate={currentExportOptions.includePlayerInfo ? hitRate : undefined}
            aiPrediction={currentExportOptions.includePlayerInfo ? aiPrediction : undefined}
            theme={currentExportOptions.theme as 'light' | 'dark'}
            width={1200}
            height={(() => {
              let height = 20;

              if (currentExportOptions.includePlayerInfo) height += 140;
              if (currentExportOptions.includeChart) height += 390;
              if (currentExportOptions.includeContextCards && exportData?.contextCards.length) height += 75;
              if (currentExportOptions.includeGameLog && filteredGames.length) {
                const gameCount = Math.min(filteredGames.length, 15);
                height += 70 + (gameCount * 28);
              }

              let sectionCount = 0;
              if (currentExportOptions.includeChart) sectionCount++;
              if (currentExportOptions.includeContextCards) sectionCount++;
              if (currentExportOptions.includeGameLog) sectionCount++;
              height += sectionCount * 20;

              height += 70;

              return height;
            })()}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              
              {currentExportOptions.includeChart && chartData.length > 0 && (
                <div style={{
                  background: currentExportOptions.theme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                  borderRadius: '16px',
                  padding: '24px',
                  border: `1px solid ${currentExportOptions.theme === 'light' ? '#e5e7eb' : '#374151'}`,
                }}>
                  <div style={{
                    fontSize: '18px',
                    fontWeight: 600,
                    marginBottom: '16px',
                    color: currentExportOptions.theme === 'light' ? '#1f2937' : '#f9fafb'
                  }}>
                    Historical Performance
                  </div>
                  <div style={{ height: '320px' }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={chartData} margin={{ top: 20, right: 10, left: 0, bottom: 60 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={currentExportOptions.theme === 'light' ? '#e5e7eb' : '#374151'} />
                        <XAxis
                          dataKey="name"
                          tick={{ fill: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: 10 }}
                          angle={-45}
                          textAnchor="end"
                          height={60}
                        />
                        <YAxis tick={{ fill: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af' }} />
                        <ReferenceLine
                          y={lineValue}
                          stroke={currentExportOptions.theme === 'light' ? '#667eea' : '#93c5fd'}
                          strokeDasharray="5 5"
                          strokeWidth={2}
                        />
                        <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                          {chartData.map((entry, index) => (
                            <Cell
                              key={`cell-${index}`}
                              fill={entry.isAIPrediction
                                ? (currentExportOptions.theme === 'light' ? '#667eea' : '#93c5fd')
                                : entry.hit
                                  ? (currentExportOptions.theme === 'light' ? '#10b981' : '#34d399')
                                  : (currentExportOptions.theme === 'light' ? '#ef4444' : '#f87171')
                              }
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}

              
              {currentExportOptions.includeContextCards && exportData && exportData.contextCards.length > 0 && (
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(5, 1fr)',
                  gap: '10px'
                }}>
                  {exportData.contextCards.map((card, idx) => (
                    <div
                      key={idx}
                      style={{
                        background: currentExportOptions.theme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                        borderRadius: '10px',
                        padding: '10px',
                        border: `1px solid ${currentExportOptions.theme === 'light' ? '#e5e7eb' : '#374151'}`,
                      }}
                    >
                      <div style={{
                        fontSize: '11px',
                        fontWeight: 600,
                        marginBottom: '6px',
                        color: currentExportOptions.theme === 'light' ? '#1f2937' : '#f9fafb'
                      }}>
                        {card.title}
                      </div>
                      <div style={{
                        fontSize: '11px',
                        color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af',
                        lineHeight: '1.5'
                      }}>
                        {card.content}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              
              {currentExportOptions.includeGameLog && filteredGames.length > 0 && (
                <div style={{
                  background: currentExportOptions.theme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                  borderRadius: '16px',
                  padding: '20px',
                  border: `1px solid ${currentExportOptions.theme === 'light' ? '#e5e7eb' : '#374151'}`,
                }}>
                  <div style={{ fontSize: '15px', fontWeight: 600, marginBottom: '12px', color: currentExportOptions.theme === 'light' ? '#1f2937' : '#f9fafb' }}>
                    Recent Games (Last {Math.min(filteredGames.length, 15)})
                  </div>
                  <table style={{ width: '100%', fontSize: '10px', borderCollapse: 'collapse' }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${currentExportOptions.theme === 'light' ? '#e5e7eb' : '#374151'}` }}>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>Hit</th>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>Date</th>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>Versus</th>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>Result</th>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>{statOptions.find(s => s.value === selectedStat)?.label}</th>
                        <th style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af', fontSize: '10px' }}>MIN</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredGames.slice(0, 15).map((game, idx) => {
                        const statValue = game.stats[selectedStat as keyof typeof game.stats] || 0;
                        const hit = overUnder === 'over' ? statValue >= lineValue : statValue < lineValue;
                        const colors = {
                          success: currentExportOptions.theme === 'light' ? '#10b981' : '#34d399',
                          destructive: currentExportOptions.theme === 'light' ? '#ef4444' : '#f87171',
                          muted: currentExportOptions.theme === 'light' ? '#6b7280' : '#9ca3af',
                        };
                        return (
                          <tr key={idx} style={{ borderBottom: `1px solid ${currentExportOptions.theme === 'light' ? '#f3f4f6' : '#1f2937'}` }}>
                            <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                                {hit ? (
                                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={colors.success} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                                    <polyline points="22 4 12 14.01 9 11.01"></polyline>
                                  </svg>
                                ) : (
                                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={colors.destructive} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <circle cx="12" cy="12" r="10"></circle>
                                    <line x1="15" y1="9" x2="9" y2="15"></line>
                                    <line x1="9" y1="9" x2="15" y2="15"></line>
                                  </svg>
                                )}
                              </div>
                            </td>
                            <td style={{ padding: '6px 8px', textAlign: 'center', color: currentExportOptions.theme === 'light' ? '#1f2937' : '#f9fafb', fontSize: '10px', fontWeight: 500 }}>{game.date.split('-').slice(1).join('/')}</td>
                            <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                <span style={{ fontSize: '9px', color: colors.muted }}>
                                  {game.isHome ? 'vs' : '@'}
                                </span>
                                <span style={{ color: currentExportOptions.theme === 'light' ? '#1f2937' : '#f9fafb', fontSize: '10px', fontWeight: 500 }}>{game.opponentAbbr}</span>
                                {game.opponentTeamId && (
                                  <img
                                    src={getTeamLogoUrl(game.opponentAbbr, game.opponentTeamId).primary}
                                    alt={game.opponentAbbr}
                                    style={{ height: '14px', width: '14px', objectFit: 'contain' }}
                                    crossOrigin="anonymous"
                                    onError={(e) => {
                                      const target = e.target as HTMLImageElement;
                                      const fallback = getTeamLogoUrl(game.opponentAbbr, game.opponentTeamId).fallback;
                                      if (fallback && target.src !== fallback) {
                                        target.src = fallback;
                                      }
                                    }}
                                  />
                                )}
                              </div>
                            </td>
                            <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                              <span style={{
                                color: game.result === 'W' ? colors.success : colors.destructive,
                                fontWeight: 500,
                                fontSize: '10px'
                              }}>
                                {game.result} · {game.score}
                              </span>
                            </td>
                            <td style={{ padding: '6px 8px', textAlign: 'center', fontWeight: 700, color: hit ? colors.success : colors.destructive, fontSize: '10px' }}>{statValue}</td>
                            <td style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>{game.minutesPlayed}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </ExportLayout>
        </div>
      )}
    </div>
  );
}
