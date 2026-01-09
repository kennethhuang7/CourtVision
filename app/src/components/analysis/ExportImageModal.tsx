import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Download, Loader2, Eye } from 'lucide-react';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';
import { ExportLayout } from './ExportLayout';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer, ReferenceLine, Cell } from 'recharts';
import { useTheme } from '@/contexts/ThemeContext';
import { getTeamLogoUrl } from '@/utils/teamLogos';

export interface ExportOptions {
  includePlayerInfo: boolean;
  includeChart: boolean;
  includeGameLog: boolean;
  includeContextCards: boolean;
  theme: 'light' | 'dark' | 'current';
  quality: number;
}

interface ExportData {
  playerName: string;
  teamName: string;
  playerPhoto?: string;
  stat: string;
  lineValue: number;
  prediction: 'over' | 'under';
  confidence?: number;
  aiPrediction: number;
  hitRate: number;
  chartData: any[];
  filteredGames: any[];
  selectedStat: string;
  overUnder: 'over' | 'under';
  contextCards: Array<{ title: string; content: string }>;
  statDisplayName: string;
}

interface ExportImageModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onExport: (options: ExportOptions) => Promise<void>;
  exportData?: ExportData;
}

export function ExportImageModal({
  open,
  onOpenChange,
  onExport,
  exportData
}: ExportImageModalProps) {
  const [isExporting, setIsExporting] = useState(false);
  const { theme: currentTheme } = useTheme();
  const [playerPhotoBase64, setPlayerPhotoBase64] = useState<string | null>(null);
  const [isLoadingPhoto, setIsLoadingPhoto] = useState(false);

  const [options, setOptions] = useState<ExportOptions>({
    includePlayerInfo: true,
    includeChart: true,
    includeGameLog: false,
    includeContextCards: true,
    theme: 'current',
    quality: 2,
  });

  
  useEffect(() => {
    const loadPlayerPhoto = async () => {
      if (!exportData?.playerPhoto || !open) {
        setPlayerPhotoBase64(null);
        setIsLoadingPhoto(false);
        return;
      }


      if (exportData.playerPhoto.includes('cdn.nba.com') || exportData.playerPhoto.includes('ak-static.cms.nba.com')) {
        setIsLoadingPhoto(true);
        try {
          const proxyUrl = `https://corsproxy.io/?${encodeURIComponent(exportData.playerPhoto)}`;
          const response = await fetch(proxyUrl);
          const blob = await response.blob();
          const base64 = await new Promise<string>((resolve) => {
            const reader = new FileReader();
            reader.onloadend = () => resolve(reader.result as string);
            reader.readAsDataURL(blob);
          });
          setPlayerPhotoBase64(base64);
        } catch (error) {
          logger.warn('Failed to load player photo for preview', error as Error);
          setPlayerPhotoBase64(null);
        } finally {
          setIsLoadingPhoto(false);
        }
      } else {

        setPlayerPhotoBase64(exportData.playerPhoto);
        setIsLoadingPhoto(false);
      }
    };

    loadPlayerPhoto();
  }, [exportData?.playerPhoto, open]);

  const handleExport = async () => {
    setIsExporting(true);
    try {
      await onExport(options);
      toast.success('Image exported successfully!');
      onOpenChange(false);
    } catch (error) {
      logger.error('Export failed', error as Error);
      toast.error('Failed to export image. Please try again.');
    } finally {
      setIsExporting(false);
    }
  };

  
  const calculateDimensions = () => {
    let height = 20; 

    if (options.includePlayerInfo) height += 140; 
    if (options.includeChart) height += 390; 
    if (options.includeContextCards && exportData?.contextCards.length) height += 75; 
    if (options.includeGameLog && exportData?.filteredGames.length) {
      const gameCount = Math.min(exportData.filteredGames.length, 15);
      height += 70 + (gameCount * 28); 
    }

    
    let sectionCount = 0;
    if (options.includeChart) sectionCount++;
    if (options.includeContextCards) sectionCount++;
    if (options.includeGameLog) sectionCount++;
    height += sectionCount * 20;

    height += 70; 

    const width = 1200; 
    return { width, height };
  };

  const { width, height } = calculateDimensions();

  
  const maxPreviewWidth = 600;
  const previewScale = maxPreviewWidth / width;
  const previewWidth = width * previewScale;
  const previewHeight = height * previewScale;

  
  const previewTheme: 'light' | 'dark' = options.theme === 'current'
    ? (currentTheme === 'dark' ? 'dark' : 'light')
    : options.theme;

  
  const getThemeColors = (theme: 'light' | 'dark') => ({
    primary: theme === 'light' ? '#667eea' : '#93c5fd',
    success: theme === 'light' ? '#10b981' : '#34d399',
    destructive: theme === 'light' ? '#ef4444' : '#f87171',
    muted: theme === 'light' ? '#6b7280' : '#9ca3af',
    border: theme === 'light' ? '#e5e7eb' : '#374151',
  });

  const colors = getThemeColors(previewTheme);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-hidden">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Eye className="h-5 w-5 shrink-0" />
            <span className="truncate">Export Preview</span>
          </DialogTitle>
          <DialogDescription className="truncate">
            {exportData ? (
              <>Customize your export for <span className="font-semibold whitespace-nowrap">{exportData.playerName} - {exportData.stat}</span></>
            ) : (
              'Customize your export'
            )}
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-[1fr_280px] gap-6 py-4">
          <div className="flex flex-col items-center justify-center bg-muted/30 rounded-lg p-6 min-h-[500px]">
            <div className="flex flex-col items-center gap-4">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Eye className="h-4 w-4 shrink-0" />
                <span className="whitespace-nowrap">Live Preview • {width} × {height}px</span>
              </div>

              <div
                className="relative rounded-lg shadow-2xl border border-border overflow-auto"
                style={{
                  maxWidth: `${previewWidth}px`,
                  maxHeight: '600px',
                  background: 'white'
                }}
              >
                {exportData ? (
                  <div style={{
                    width: `${previewWidth}px`,
                    height: `${previewHeight}px`,
                  }}>
                    <div style={{
                      transform: `scale(${previewScale})`,
                      transformOrigin: 'top left',
                      width: `${width}px`,
                    }}>
                    <ExportLayout
                      playerName={options.includePlayerInfo ? exportData.playerName : undefined}
                      teamName={options.includePlayerInfo ? exportData.teamName : undefined}
                      playerPhoto={options.includePlayerInfo && !isLoadingPhoto ? (playerPhotoBase64 || exportData.playerPhoto) : undefined}
                      stat={options.includePlayerInfo ? exportData.stat : undefined}
                      lineValue={options.includePlayerInfo ? exportData.lineValue : undefined}
                      prediction={options.includePlayerInfo ? exportData.prediction : undefined}
                      hitRate={options.includePlayerInfo ? exportData.hitRate : undefined}
                      aiPrediction={options.includePlayerInfo ? exportData.aiPrediction : undefined}
                      theme={previewTheme}
                      width={width}
                      height={height}
                    >
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                        {options.includeChart && exportData.chartData.length > 0 && (
                          <div style={{
                            background: previewTheme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                            borderRadius: '16px',
                            padding: '24px',
                            border: `1px solid ${colors.border}`,
                          }}>
                            <div style={{
                              fontSize: '18px',
                              fontWeight: 600,
                              marginBottom: '16px',
                              color: previewTheme === 'light' ? '#1f2937' : '#f9fafb'
                            }}>
                              Historical Performance
                            </div>
                            <div style={{ height: '320px' }}>
                              <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={exportData.chartData} margin={{ top: 20, right: 10, left: 0, bottom: 60 }}>
                                  <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
                                  <XAxis
                                    dataKey="name"
                                    tick={{ fill: colors.muted, fontSize: 10 }}
                                    angle={-45}
                                    textAnchor="end"
                                    height={60}
                                  />
                                  <YAxis tick={{ fill: colors.muted }} />
                                  <ReferenceLine
                                    y={exportData.lineValue}
                                    stroke={colors.primary}
                                    strokeDasharray="5 5"
                                    strokeWidth={2}
                                  />
                                  <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                                    {exportData.chartData.map((entry, index) => (
                                      <Cell
                                        key={`cell-${index}`}
                                        fill={entry.isAIPrediction
                                          ? colors.primary
                                          : entry.hit
                                            ? colors.success
                                            : colors.destructive
                                        }
                                      />
                                    ))}
                                  </Bar>
                                </BarChart>
                              </ResponsiveContainer>
                            </div>
                          </div>
                        )}

                        {options.includeContextCards && exportData.contextCards.length > 0 && (
                          <div style={{
                            display: 'grid',
                            gridTemplateColumns: 'repeat(5, 1fr)',
                            gap: '10px'
                          }}>
                            {exportData.contextCards.map((card, idx) => (
                              <div
                                key={idx}
                                style={{
                                  background: previewTheme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                                  borderRadius: '10px',
                                  padding: '10px',
                                  border: `1px solid ${colors.border}`,
                                }}
                              >
                                <div style={{
                                  fontSize: '11px',
                                  fontWeight: 600,
                                  marginBottom: '6px',
                                  color: previewTheme === 'light' ? '#1f2937' : '#f9fafb'
                                }}>
                                  {card.title}
                                </div>
                                <div style={{
                                  fontSize: '11px',
                                  color: previewTheme === 'light' ? '#6b7280' : '#9ca3af',
                                  lineHeight: '1.5'
                                }}>
                                  {card.content}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}

                        {options.includeGameLog && exportData.filteredGames.length > 0 && (
                          <div style={{
                            background: previewTheme === 'light' ? 'rgba(255, 255, 255, 0.95)' : 'rgba(30, 30, 46, 0.95)',
                            borderRadius: '16px',
                            padding: '20px',
                            border: `1px solid ${colors.border}`,
                          }}>
                            <div style={{ fontSize: '15px', fontWeight: 600, marginBottom: '12px', color: previewTheme === 'light' ? '#1f2937' : '#f9fafb' }}>
                              Recent Games (Last {Math.min(exportData.filteredGames.length, 15)})
                            </div>
                            <table style={{ width: '100%', fontSize: '10px', borderCollapse: 'collapse' }}>
                              <thead>
                                <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>Hit</th>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>Date</th>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>Versus</th>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>Result</th>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>{exportData.stat}</th>
                                  <th style={{ padding: '6px 8px', textAlign: 'center', color: colors.muted, fontSize: '10px' }}>MIN</th>
                                </tr>
                              </thead>
                              <tbody>
                                {exportData.filteredGames.slice(0, 15).map((game, idx) => {
                                  const statValue = game.stats[exportData.selectedStat] || 0;
                                  const hit = exportData.overUnder === 'over' ? statValue >= exportData.lineValue : statValue < exportData.lineValue;
                                  return (
                                    <tr key={idx} style={{ borderBottom: `1px solid ${previewTheme === 'light' ? '#f3f4f6' : '#1f2937'}` }}>
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
                                      <td style={{ padding: '6px 8px', textAlign: 'center', color: previewTheme === 'light' ? '#1f2937' : '#f9fafb', fontSize: '10px', fontWeight: 500 }}>{game.date.split('-').slice(1).join('/')}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                          <span style={{ fontSize: '9px', color: colors.muted }}>
                                            {game.isHome ? 'vs' : '@'}
                                          </span>
                                          <span style={{ color: previewTheme === 'light' ? '#1f2937' : '#f9fafb', fontSize: '10px', fontWeight: 500 }}>{game.opponentAbbr}</span>
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
                  </div>
                ) : (
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    <div className="text-center">
                      <Eye className="h-12 w-12 mx-auto mb-2 opacity-20 shrink-0" />
                      <p className="text-sm whitespace-nowrap">Preview will appear here</p>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div className="space-y-5 overflow-y-auto max-h-[600px] pr-2">
            <div className="space-y-3">
              <Label className="text-sm font-semibold">Content</Label>
              <div className="space-y-2.5 pl-1">
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="player-info"
                    checked={options.includePlayerInfo}
                    onCheckedChange={(checked) =>
                      setOptions({ ...options, includePlayerInfo: checked as boolean })
                    }
                  />
                  <Label htmlFor="player-info" className="font-normal cursor-pointer text-sm">
                    Player Info
                  </Label>
                </div>

                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="chart"
                    checked={options.includeChart}
                    onCheckedChange={(checked) =>
                      setOptions({ ...options, includeChart: checked as boolean })
                    }
                  />
                  <Label htmlFor="chart" className="font-normal cursor-pointer text-sm">
                    Chart
                  </Label>
                </div>

                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="context-cards"
                    checked={options.includeContextCards}
                    onCheckedChange={(checked) =>
                      setOptions({ ...options, includeContextCards: checked as boolean })
                    }
                  />
                  <Label htmlFor="context-cards" className="font-normal cursor-pointer text-sm">
                    Context Cards
                  </Label>
                </div>

                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="game-log"
                    checked={options.includeGameLog}
                    onCheckedChange={(checked) =>
                      setOptions({ ...options, includeGameLog: checked as boolean })
                    }
                  />
                  <Label htmlFor="game-log" className="font-normal cursor-pointer text-sm">
                    Game Log (max 15)
                  </Label>
                </div>
              </div>
            </div>

            <div className="border-t border-border" />

            <div className="space-y-2.5">
              <Label className="text-sm font-semibold">Theme</Label>
              <RadioGroup
                value={options.theme}
                onValueChange={(value) => setOptions({ ...options, theme: value as any })}
                className="pl-1 space-y-2"
              >
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="current" id="theme-current" />
                  <Label htmlFor="theme-current" className="font-normal cursor-pointer text-sm">
                    Current
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="light" id="theme-light" />
                  <Label htmlFor="theme-light" className="font-normal cursor-pointer text-sm">
                    Light
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="dark" id="theme-dark" />
                  <Label htmlFor="theme-dark" className="font-normal cursor-pointer text-sm">
                    Dark
                  </Label>
                </div>
              </RadioGroup>
            </div>

            <div className="border-t border-border" />

            <div className="space-y-2.5">
              <Label className="text-sm font-semibold">Quality</Label>
              <RadioGroup
                value={options.quality.toString()}
                onValueChange={(value) => setOptions({ ...options, quality: parseInt(value) })}
                className="pl-1 space-y-2"
              >
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="1" id="quality-standard" />
                  <Label htmlFor="quality-standard" className="font-normal cursor-pointer text-sm">
                    Standard (1x)
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="2" id="quality-high" />
                  <Label htmlFor="quality-high" className="font-normal cursor-pointer text-sm">
                    High (2x)
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="3" id="quality-ultra" />
                  <Label htmlFor="quality-ultra" className="font-normal cursor-pointer text-sm">
                    Ultra (3x)
                  </Label>
                </div>
              </RadioGroup>
            </div>
          </div>
        </div>

        <DialogFooter className="border-t border-border pt-4">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isExporting}>
            <span className="whitespace-nowrap">Cancel</span>
          </Button>
          <Button onClick={handleExport} disabled={isExporting}>
            {isExporting ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 shrink-0 animate-spin" />
                <span className="whitespace-nowrap">Exporting...</span>
              </>
            ) : (
              <>
                <Download className="mr-2 h-4 w-4 shrink-0" />
                <span className="whitespace-nowrap">Export Image</span>
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
