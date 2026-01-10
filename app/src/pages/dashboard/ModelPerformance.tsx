import { useState, useMemo, useEffect } from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Button } from '@/components/ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { logger } from '@/lib/logger';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { AlertCircle, TrendingDown, Activity, Loader2, Palette, RefreshCw } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useEnsemble, type ModelId } from '@/contexts/EnsembleContext';
import { useModelPerformance } from '@/hooks/useModelPerformance';

const modelOptions = [
  { value: 'xgboost', label: 'XGBoost' },
  { value: 'lightgbm', label: 'LightGBM' },
  { value: 'random_forest', label: 'Random Forest' },
  { value: 'catboost', label: 'CatBoost' },
];

const timeOptions = [
  { value: '7', label: 'Last 7 Days' },
  { value: '30', label: 'Last 30 Days' },
  { value: '90', label: 'Last 90 Days' },
  { value: '180', label: 'Last 180 Days' },
  { value: 'all', label: 'All Time' },
];


const defaultModelColors: Record<ModelId | 'ensemble', string> = {
  ensemble: '#3b82f6', 
  xgboost: '#10b981', 
  lightgbm: '#f59e0b', 
  random_forest: '#ef4444', 
  catboost: '#8b5cf6', 
};

export default function ModelPerformance() {
  const { selectedModels, toggleModel } = useEnsemble();
  
  
  const [timePeriod, setTimePeriod] = useState(() => {
    const stored = localStorage.getItem('model-performance-time-period');
    return stored || '30';
  });
  const [groupBy, setGroupBy] = useState(() => {
    const stored = localStorage.getItem('model-performance-group-by');
    return stored || 'daily';
  });
  const [selectedStatForTimeSeries, setSelectedStatForTimeSeries] = useState(() => {
    const stored = localStorage.getItem('model-performance-time-series-stat');
    return stored || 'overall';
  });
  
  
  
  const [modelsToCompare, setModelsToCompare] = useState<(ModelId | 'ensemble')[]>(() => {
    const stored = localStorage.getItem('model-performance-compare');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);


        const filtered = parsed.filter((m: ModelId | 'ensemble') =>
          m !== 'ensemble' || selectedModels.length > 0
        );
        return filtered.length > 0 ? filtered : (selectedModels.length > 0 ? ['ensemble'] : []);
      } catch {

      }
    }
    return selectedModels.length > 0 ? ['ensemble'] : [];
  });
  
  
  useEffect(() => {
    localStorage.setItem('model-performance-time-period', timePeriod);
  }, [timePeriod]);
  
  useEffect(() => {
    localStorage.setItem('model-performance-group-by', groupBy);
  }, [groupBy]);
  
  useEffect(() => {
    localStorage.setItem('model-performance-time-series-stat', selectedStatForTimeSeries);
  }, [selectedStatForTimeSeries]);
  
  
  useEffect(() => {
    localStorage.setItem('model-performance-compare', JSON.stringify(modelsToCompare));
  }, [modelsToCompare]);
  
  
  
  const [modelForMAE, setModelForMAE] = useState<ModelId | 'ensemble'>(() => {
    const stored = localStorage.getItem('model-performance-mae-model');
    if (stored) {
      try {
        const parsed = stored as ModelId | 'ensemble';
        
        return parsed;
      } catch {
        
      }
    }
    return 'ensemble';
  });
  
  
  useEffect(() => {
    localStorage.setItem('model-performance-mae-model', modelForMAE);
  }, [modelForMAE]);
  
  
  
  
  
  const [modelColors, setModelColors] = useState<Record<ModelId | 'ensemble', string>>(() => {
    const stored = localStorage.getItem('model-performance-colors');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        
        const merged = { ...defaultModelColors, ...parsed };
        
        
        const oldDefaults: Record<string, string> = {
          xgboost: '#3b82f6', 
          lightgbm: '#10b981', 
          random_forest: '#f59e0b', 
          catboost: '#ef4444', 
        };
        
        
        Object.keys(oldDefaults).forEach(model => {
          if (parsed[model] === oldDefaults[model]) {
            merged[model] = defaultModelColors[model as ModelId];
          }
        });
        
        
        if (!parsed.ensemble) {
          merged.ensemble = defaultModelColors.ensemble;
        }
        
        
        localStorage.setItem('model-performance-colors', JSON.stringify(merged));
        
        return merged;
      } catch {
        return defaultModelColors;
      }
    }
    return defaultModelColors;
  });

  
  const statOptionsForTimeSeries = [
    { value: 'overall', label: 'Overall' },
  { value: 'points', label: 'Points' },
  { value: 'rebounds', label: 'Rebounds' },
  { value: 'assists', label: 'Assists' },
  { value: 'steals', label: 'Steals' },
  { value: 'blocks', label: 'Blocks' },
  { value: 'turnovers', label: 'Turnovers' },
  { value: 'threePointers', label: 'Three Pointers' },
];



  const xgboostQuery = useModelPerformance(
    timePeriod,
    selectedStatForTimeSeries,
    ['xgboost'],
    { enabled: modelsToCompare.includes('xgboost') }
  );
  const lightgbmQuery = useModelPerformance(
    timePeriod,
    selectedStatForTimeSeries,
    ['lightgbm'],
    { enabled: modelsToCompare.includes('lightgbm') }
  );
  const randomForestQuery = useModelPerformance(
    timePeriod,
    selectedStatForTimeSeries,
    ['random_forest'],
    { enabled: modelsToCompare.includes('random_forest') }
  );
  const catboostQuery = useModelPerformance(
    timePeriod,
    selectedStatForTimeSeries,
    ['catboost'],
    { enabled: modelsToCompare.includes('catboost') }
  );

  const ensembleQuery = useModelPerformance(
    timePeriod,
    selectedStatForTimeSeries,
    selectedModels,
    { enabled: modelsToCompare.includes('ensemble' as any) }
  );


  const allQueries = {
    ensemble: ensembleQuery,
    xgboost: xgboostQuery,
    lightgbm: lightgbmQuery,
    random_forest: randomForestQuery,
    catboost: catboostQuery,
  };


  const allStatsQueries = allQueries;

  
  const performanceQueries = modelsToCompare.map(model => ({
    model,
    query: allQueries[model],
  }));

  
  const isLoading = (modelsToCompare.includes('ensemble' as any) && ensembleQuery.isLoading) ||
    (performanceQueries.length > 0 && performanceQueries.some(q => q.query.isLoading));
  const isError = (modelsToCompare.includes('ensemble' as any) && ensembleQuery.isError) ||
    (performanceQueries.length > 0 && performanceQueries.some(q => q.query.isError));
  const error = (modelsToCompare.includes('ensemble' as any) && ensembleQuery.error) ||
    performanceQueries.find(q => q.query.error)?.query.error;
  
  
  if (isError && error) {
    logger.error('Model Performance Error', error as Error);
  }

  
  const groupTimeSeriesData = (
    data: Array<{ date: string; fullDate?: string; predictions?: number; [key: string]: any }>,
    groupBy: string
  ): Array<{ date: string; fullDate?: string; predictions?: number; [key: string]: number }> => {
    if (groupBy === 'daily') {
      return data;
    }


    const groupedMap = new Map<string, {
      displayDate: string;
      sortKey: string;
      values: Record<string, { sum: number; count: number }>;
      predictions: number;
    }>();

    data.forEach(point => {
      const dateStr = point.fullDate || point.date;
      const date = new Date(dateStr);
      let groupKey: string;
      let displayDate: string;
      let sortKey: string;

      if (groupBy === 'weekly') {
        
        const weekStart = new Date(date);
        weekStart.setDate(date.getDate() - date.getDay());
        weekStart.setHours(0, 0, 0, 0);
        groupKey = weekStart.toISOString().split('T')[0];
        sortKey = groupKey;
        
        displayDate = `Week of ${weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;
      } else if (groupBy === 'monthly') {
        
        const year = date.getFullYear();
        const month = date.getMonth();
        groupKey = `${year}-${String(month + 1).padStart(2, '0')}`;
        sortKey = groupKey;
        
        const monthStart = new Date(year, month, 1);
        displayDate = monthStart.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
      } else {
        return; 
      }

      const existing = groupedMap.get(groupKey);
      
      if (!existing) {
        groupedMap.set(groupKey, {
          displayDate,
          sortKey,
          values: {},
          predictions: 0,
        });
      }
      
      const group = groupedMap.get(groupKey)!;
      
      
      if (point.predictions !== undefined) {
        group.predictions += point.predictions;
      }
      
      
      Object.keys(point).forEach(key => {
        if (key === 'date' || key === 'predictions') return;
        const value = point[key];
        if (typeof value === 'number') {
          if (!group.values[key]) {
            group.values[key] = { sum: 0, count: 0 };
          }
          group.values[key].sum += value;
          group.values[key].count += 1;
        }
      });
    });

    
    return Array.from(groupedMap.values())
      .map(group => {
        const result: Record<string, number | string | undefined> = { 
          date: group.displayDate,
          predictions: group.predictions > 0 ? group.predictions : undefined,
    };
        Object.keys(group.values).forEach(modelKey => {
          const { sum, count } = group.values[modelKey];
          result[modelKey] = count > 0 ? sum / count : 0;
        });
        return {
          ...result,
          _sortKey: group.sortKey, 
        } as { date: string; predictions?: number; [key: string]: number | string | undefined };
      })
      .sort((a, b) => {
        
        return (a._sortKey as string).localeCompare(b._sortKey as string);
      })
      .map(({ _sortKey, ...rest }) => rest) as Array<{ date: string; predictions?: number; [key: string]: number }>;
  };

  
  const combinedData = useMemo(() => {
    
    if (modelsToCompare.length === 0) {
      return {
        overallMetrics: [],
        timeSeriesData: [],
      };
    }

    
    if (modelsToCompare.length === 1) {
      const singleQuery = performanceQueries[0];
      const data = singleQuery.query.data;
      if (!data) {
        return {
          overallMetrics: [],
          timeSeriesData: [],
        };
      }
      
      
      const timeSeriesWithModelName = data.timeSeriesData?.map(point => ({
        ...point,
        [singleQuery.model]: point.error,
      })) || [];
      
      
      const groupedTimeSeries = groupTimeSeriesData(timeSeriesWithModelName, groupBy);
      
      return {
        overallMetrics: data.overallMetrics || [],
        timeSeriesData: groupedTimeSeries,
      };
    }

    
    const timeSeriesMap = new Map<string, Record<string, number> & { predictions?: number; fullDate?: string }>();
    
    
    if (modelsToCompare.includes('ensemble' as any) && selectedModels.length > 0) {
      const ensembleData = ensembleQuery.data;
      if (ensembleData && ensembleData.timeSeriesData) {
        ensembleData.timeSeriesData.forEach(point => {
          const existing = timeSeriesMap.get(point.date) || {};
          existing.ensemble = point.error;
          if (point.predictions !== undefined) {
            existing.predictions = point.predictions;
          }
          if (point.fullDate !== undefined) {
            existing.fullDate = point.fullDate;
          }
          timeSeriesMap.set(point.date, existing);
        });
      }
    }


    performanceQueries.forEach(({ model, query }) => {
      const data = query.data;
      if (!data || !data.timeSeriesData) return;

      data.timeSeriesData.forEach(point => {
        const existing = timeSeriesMap.get(point.date) || {};
        existing[model] = point.error;

        if (point.predictions !== undefined && existing.predictions === undefined) {
          existing.predictions = point.predictions;
        }
        if (point.fullDate !== undefined && existing.fullDate === undefined) {
          existing.fullDate = point.fullDate;
        }
        timeSeriesMap.set(point.date, existing);
      });
    });


    const combinedTimeSeries = Array.from(timeSeriesMap.entries())
      .map(([date, data]) => ({
        date,
        ...data,
      }))
      .sort((a, b) => {

        const dateStrA = a.fullDate || a.date;
        const dateStrB = b.fullDate || b.date;


        const dateA = dateStrA.includes('-') ? new Date(dateStrA) : new Date(dateStrA);
        const dateB = dateStrB.includes('-') ? new Date(dateStrB) : new Date(dateStrB);
        return dateA.getTime() - dateB.getTime();
      });

    
    const groupedTimeSeries = groupTimeSeriesData(combinedTimeSeries, groupBy);

    
    
    const maeQuery = allQueries[modelForMAE];
    const maeData = maeQuery?.data;
    const maeMetrics = maeData?.overallMetrics || [];
    
    return {
      overallMetrics: maeMetrics,
      timeSeriesData: groupedTimeSeries,
    };
  }, [performanceQueries, modelsToCompare, groupBy, selectedModels, ensembleQuery, modelForMAE, allQueries]);

  const overallMetrics = combinedData?.overallMetrics || [];
  const timeSeriesData = combinedData?.timeSeriesData || [];
  
  
  const yAxisDomain = useMemo(() => {
    if (timeSeriesData.length === 0) {
      
      return { min: 0, max: 2, ticks: [0, 0.5, 1, 1.5, 2] };
    }
    
    
    const allErrors: number[] = [];
    timeSeriesData.forEach(point => {
      Object.keys(point).forEach(key => {
        if (key !== 'date' && key !== 'predictions' && typeof point[key] === 'number') {
          allErrors.push(point[key] as number);
        }
      });
    });
    
    if (allErrors.length === 0) {
      return { min: 0, max: 2, ticks: [0, 0.5, 1, 1.5, 2] };
    }
    
    const minError = Math.min(...allErrors);
    const maxError = Math.max(...allErrors);
    
    
    const range = maxError - minError;
    const padding = range * 0.1 || 0.1; 
    const paddedMin = Math.max(0, minError - padding);
    const paddedMax = maxError + padding;
    
    
    const roundedMin = Math.floor(paddedMin * 10) / 10;
    const roundedMax = Math.ceil(paddedMax * 10) / 10;
    
    
    const tickCount = 5;
    const tickStep = (roundedMax - roundedMin) / (tickCount - 1);
    const ticks: number[] = [];
    for (let i = 0; i < tickCount; i++) {
      ticks.push(Math.round((roundedMin + tickStep * i) * 100) / 100);
    }
    
    return { min: roundedMin, max: roundedMax, ticks };
  }, [timeSeriesData]);

  
  const getMAEColor = (stat: string, mae: number): string => {
    const thresholds: Record<string, { good: number; warning: number }> = {
      'Points': { good: 3.5, warning: 5.5 },      
      'Rebounds': { good: 1.5, warning: 2.5 },    
      'Assists': { good: 1.2, warning: 2.0 },     
      'Steals': { good: 0.6, warning: 1.0 },      
      'Blocks': { good: 0.5, warning: 0.8 },      
      'Turnovers': { good: 0.8, warning: 1.3 },   
      '3PM': { good: 0.8, warning: 1.2 },        
      'Overall': { good: 1.3, warning: 1.8 },    
    };
    
    const threshold = thresholds[stat] || { good: 1.5, warning: 2.5 };
    if (mae < threshold.good) return 'text-success';
    if (mae < threshold.warning) return 'text-warning';
    return 'text-destructive';
  };

  const MetricCard = ({ stat, mae }: { stat: string; mae: number }) => (
    <div className="stat-card">
      <p className="data-label">{stat}</p>
      <p className={cn('text-3xl font-bold', getMAEColor(stat, mae))}>
        {mae.toFixed(2)}
      </p>
      <p className="text-xs text-muted-foreground mt-1">MAE</p>
    </div>
  );

  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());

  useEffect(() => {
    setLastUpdated(new Date());
  }, [combinedData]);

  const getTimeAgo = (date: Date) => {
    const seconds = Math.floor((new Date().getTime() - date.getTime()) / 1000);
    if (seconds < 60) return 'just now';
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ago`;
  };

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="min-w-0 flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          <h1 className="text-3xl font-bold text-foreground leading-tight truncate">Model Performance</h1>
          <div className="flex items-center gap-2 mt-1">
            <p className="text-muted-foreground text-sm leading-tight truncate">Analyze prediction accuracy and model metrics</p>
            <span className="text-xs text-muted-foreground whitespace-nowrap">• Updated {getTimeAgo(lastUpdated)}</span>
          </div>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            ensembleQuery.refetch();
            performanceQueries.forEach(q => q.query.refetch());
            setLastUpdated(new Date());
          }}
          className="flex-shrink-0"
        >
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <div className="stat-card">
        <h3 className="font-semibold text-foreground mb-4">Ensemble Configuration</h3>
        <div className="flex flex-wrap gap-4">
          {modelOptions.map(model => (
            <div key={model.value} className="flex items-center gap-2">
              <Checkbox
                id={model.value}
                checked={selectedModels.includes(model.value)}
                onCheckedChange={() => toggleModel(model.value)}
              />
              <Label htmlFor={model.value} className="cursor-pointer">{model.label}</Label>
            </div>
          ))}
        </div>
        {selectedModels.length === 0 && (
          <div className="mt-4 flex items-center gap-2 text-warning">
            <AlertCircle className="h-4 w-4" />
            <span className="text-sm">Please select at least one model</span>
          </div>
        )}
        {selectedModels.length > 0 && (
          <p className="mt-4 text-sm text-muted-foreground">
            Ensemble using: {selectedModels.map(m => modelOptions.find(o => o.value === m)?.label).join(', ')}
          </p>
        )}
      </div>

      <div className="flex flex-wrap gap-4 rounded-xl bg-card/50 p-4 border border-border">
        <div className="space-y-2">
          <Label>Time Period</Label>
          <Select value={timePeriod} onValueChange={setTimePeriod}>
            <SelectTrigger className="w-[150px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {timeOptions.map(opt => (
                <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <Label>Group By</Label>
          <Select value={groupBy} onValueChange={setGroupBy}>
            <SelectTrigger className="w-[150px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="daily">Daily</SelectItem>
              <SelectItem value="weekly">Weekly</SelectItem>
              <SelectItem value="monthly">Monthly</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <Label>Stat Type</Label>
          <Select value={selectedStatForTimeSeries} onValueChange={setSelectedStatForTimeSeries}>
            <SelectTrigger className="w-[150px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {statOptionsForTimeSeries.map(stat => (
                <SelectItem key={stat.value} value={stat.value}>{stat.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <Label>Compare Models</Label>
          <div className="flex flex-wrap gap-3">
            {selectedModels.length > 0 && (() => {
              const isEnsembleSelected = modelsToCompare.includes('ensemble' as any);
              return (
                <div 
                  key="ensemble"
                  className={cn(
                    "flex items-center justify-between px-3 h-10 rounded-md border transition-colors min-w-[150px]",
                    isEnsembleSelected 
                      ? "bg-background border-input" 
                      : "bg-background border-input"
                  )}
                >
                  <div className="flex items-center gap-2 flex-1 min-w-0">
                    <Checkbox
                      id="compare-ensemble"
                      checked={isEnsembleSelected}
                      onCheckedChange={() => {
                        if (isEnsembleSelected) {
                          setModelsToCompare(prev => prev.filter(m => m !== 'ensemble' as any));
                        } else {
                          setModelsToCompare(prev => ['ensemble' as any, ...prev]);
                        }
                      }}
                    />
                    <Label 
                      htmlFor="compare-ensemble" 
                      className="cursor-pointer text-sm whitespace-nowrap"
                      onClick={(e) => {
                        if ((e.target as HTMLElement).closest('.color-picker-trigger')) {
                          e.preventDefault();
                        }
                      }}
                    >
                      Ensemble
                    </Label>
                  </div>
                  {isEnsembleSelected && (
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0 hover:bg-muted relative color-picker-trigger flex-shrink-0 ml-2"
                          title="Change color"
                          onClick={(e) => {
                            e.stopPropagation();
                          }}
                        >
                          <Palette className="h-3.5 w-3.5 text-muted-foreground" />
                          <div 
                            className="absolute h-2 w-2 rounded-sm border border-background shadow-sm"
                            style={{ 
                              backgroundColor: modelColors['ensemble'],
                              bottom: '0.5px',
                              right: '0.5px',
                            }}
                          />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-64" onClick={(e) => e.stopPropagation()}>
                        <Label className="text-sm font-medium mb-3 block">Color for Ensemble</Label>
                        <div className="grid grid-cols-8 gap-2 mb-4">
                          {[
                            '#3b82f6', '#10b981', '#f59e0b', '#ef4444',
                            '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16',
                            '#f97316', '#6366f1', '#14b8a6', '#eab308',
                            '#dc2626', '#7c3aed', '#be185d', '#0d9488',
                          ].map(color => (
                            <button
                              key={color}
                              type="button"
                              className={cn(
                                "h-8 w-8 rounded border-2 transition-all hover:scale-110",
                                modelColors['ensemble'] === color 
                                  ? "border-foreground scale-110 ring-2 ring-primary/20" 
                                  : "border-border hover:border-foreground/50"
                              )}
                              style={{ backgroundColor: color }}
                              onClick={() => {
                                const newColors = { ...modelColors, ensemble: color };
                                setModelColors(newColors);
                                localStorage.setItem('model-performance-colors', JSON.stringify(newColors));
                              }}
                            />
                          ))}
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground mb-2 block">Custom Color</Label>
                          <input
                            type="color"
                            value={modelColors['ensemble']}
                            onChange={(e) => {
                              const newColors = { ...modelColors, ensemble: e.target.value };
                              setModelColors(newColors);
                              localStorage.setItem('model-performance-colors', JSON.stringify(newColors));
                            }}
                            className="w-full h-10 rounded border border-border cursor-pointer"
                          />
                        </div>
                      </PopoverContent>
                    </Popover>
                  )}
                </div>
              );
            })()}
            {modelOptions.map(model => {
              const isSelected = modelsToCompare.includes(model.value);
              return (
                <div 
                  key={model.value} 
                  className={cn(
                    "flex items-center justify-between px-3 h-10 rounded-md border transition-colors min-w-[150px]",
                    isSelected 
                      ? "bg-background border-input" 
                      : "bg-background border-input"
                  )}
                >
                  <div className="flex items-center gap-2 flex-1 min-w-0">
                    <Checkbox
                      id={`compare-${model.value}`}
                      checked={isSelected}
                      onCheckedChange={() => {
                        if (isSelected) {
                          setModelsToCompare(prev => prev.filter(m => m !== model.value));
                        } else {
                          setModelsToCompare(prev => [...prev, model.value]);
                        }
                      }}
                    />
                    <Label 
                      htmlFor={`compare-${model.value}`} 
                      className="cursor-pointer text-sm whitespace-nowrap"
                      onClick={(e) => {
                        
                        if ((e.target as HTMLElement).closest('.color-picker-trigger')) {
                          e.preventDefault();
                        }
                      }}
                    >
                      {model.label}
                    </Label>
                  </div>
                  {isSelected && (
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0 hover:bg-muted relative color-picker-trigger flex-shrink-0 ml-2"
                          title="Change color"
                          onClick={(e) => {
                            e.stopPropagation();
                          }}
                        >
                          <Palette className="h-3.5 w-3.5 text-muted-foreground" />
                          <div 
                            className="absolute h-2 w-2 rounded-sm border border-background shadow-sm"
                            style={{ 
                              backgroundColor: modelColors[model.value],
                              bottom: '0.5px',
                              right: '0.5px',
                            }}
                          />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-64" onClick={(e) => e.stopPropagation()}>
                        <Label className="text-sm font-medium mb-3 block">Color for {model.label}</Label>
                        <div className="grid grid-cols-8 gap-2 mb-4">
                          {[
                            '#3b82f6', '#10b981', '#f59e0b', '#ef4444',
                            '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16',
                            '#f97316', '#6366f1', '#14b8a6', '#eab308',
                            '#dc2626', '#7c3aed', '#be185d', '#0d9488',
                          ].map(color => (
                            <button
                              key={color}
                              type="button"
                              className={cn(
                                "h-8 w-8 rounded border-2 transition-all hover:scale-110",
                                modelColors[model.value] === color 
                                  ? "border-foreground scale-110 ring-2 ring-primary/20" 
                                  : "border-border hover:border-foreground/50"
                              )}
                              style={{ backgroundColor: color }}
                              onClick={() => {
                                const newColors = { ...modelColors, [model.value]: color };
                                setModelColors(newColors);
                                localStorage.setItem('model-performance-colors', JSON.stringify(newColors));
                              }}
                            />
                          ))}
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground mb-2 block">Custom Color</Label>
                          <input
                            type="color"
                            value={modelColors[model.value]}
                            onChange={(e) => {
                              const newColors = { ...modelColors, [model.value]: e.target.value };
                              setModelColors(newColors);
                              localStorage.setItem('model-performance-colors', JSON.stringify(newColors));
                            }}
                            className="w-full h-10 rounded border border-border cursor-pointer"
                          />
                        </div>
                      </PopoverContent>
                    </Popover>
                  )}
                </div>
              );
            })}
          </div>
          {modelsToCompare.length === 0 && (
            <p className="text-xs text-muted-foreground mt-2">Select at least one model to compare</p>
          )}
        </div>
      </div>

      <div>
        <div className="mb-4 flex items-center justify-between flex-wrap gap-4">
          <h3 className="flex items-center gap-2 font-semibold text-foreground">
          <TrendingDown className="h-5 w-5 text-primary" />
          Mean Absolute Error (MAE)
        </h3>
          <div className="flex items-center gap-3 flex-wrap">
            <Label className="text-sm text-muted-foreground">Model for MAE:</Label>
            <RadioGroup
              value={modelForMAE}
              onValueChange={(value) => setModelForMAE(value as ModelId | 'ensemble')}
              className="flex flex-wrap gap-4"
            >
              {selectedModels.length > 0 && (
                <div className="flex items-center gap-2">
                  <RadioGroupItem value="ensemble" id="mae-ensemble" />
                  <Label htmlFor="mae-ensemble" className="cursor-pointer text-sm">Ensemble</Label>
                </div>
              )}
              {modelOptions.map(model => (
                <div key={model.value} className="flex items-center gap-2">
                  <RadioGroupItem value={model.value} id={`mae-${model.value}`} />
                  <Label htmlFor={`mae-${model.value}`} className="cursor-pointer text-sm">
                    {model.label}
                  </Label>
                </div>
              ))}
            </RadioGroup>
          </div>
        </div>
        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <span className="ml-3 text-muted-foreground">Loading performance metrics...</span>
          </div>
        ) : isError ? (
          <div className="flex flex-col items-center justify-center py-12 text-destructive">
            <AlertCircle className="h-5 w-5 mb-2" />
            <span>Failed to load performance data</span>
            {error && (
              <span className="text-xs text-muted-foreground mt-2 max-w-md text-center">
                {error.message || String(error)}
              </span>
            )}
          </div>
        ) : overallMetrics.length === 0 ? (
          <div className="flex items-center justify-center py-12 text-muted-foreground">
            <AlertCircle className="h-5 w-5 mr-2" />
            <span>No completed games found for the selected time period</span>
          </div>
        ) : (
        <div className="grid gap-4 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-8">
            {overallMetrics.map(perf => (
              <MetricCard key={perf.stat} stat={perf.stat} mae={perf.mae} />
          ))}
        </div>
        )}
      </div>

      <div className="stat-card">
        <h3 className="mb-4 flex items-center gap-2 font-semibold text-foreground">
          <Activity className="h-5 w-5 text-primary" />
          Performance Over Time
        </h3>
        {isLoading ? (
          <div className="flex items-center justify-center h-[300px]">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
          </div>
        ) : modelsToCompare.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            <span>Please select at least one model to compare</span>
          </div>
        ) : timeSeriesData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            <span>No time series data available</span>
          </div>
        ) : (
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={timeSeriesData} margin={{ top: 20, right: 30, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="date" 
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
              />
                <YAxis 
                  domain={[yAxisDomain.min, yAxisDomain.max]}
                  tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
                  ticks={yAxisDomain.ticks}
                />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))' }}
                  labelFormatter={(label) => {
                    
                    const dataPoint = timeSeriesData.find(d => d.date === label);
                    const predictions = dataPoint?.predictions;
                    if (predictions !== undefined) {
                      return `${label} (${predictions} predictions)`;
                    }
                    return label;
                  }}
                  formatter={(value: number, name: string) => {
                    const modelLabel = name === 'ensemble' 
                      ? 'Ensemble' 
                      : modelOptions.find(o => o.value === name)?.label || name;
                    return [`${Number(value).toFixed(4)}`, modelLabel];
                  }}
                />
                {modelsToCompare.map(model => (
              <Line 
                    key={model}
                    type="linear"
                    dataKey={model}
                    name={model === 'ensemble' ? 'Ensemble' : modelOptions.find(o => o.value === model)?.label}
                    stroke={modelColors[model]}
                strokeWidth={2}
                    dot={{ fill: modelColors[model], strokeWidth: 0, r: 3 }}
                activeDot={{ r: 6, strokeWidth: 0 }}
              />
                ))}
                {modelsToCompare.length > 1 && <Legend />}
            </LineChart>
          </ResponsiveContainer>
        </div>
        )}
      </div>

      <div className="stat-card">
        <h3 className="mb-4 flex items-center gap-2 font-semibold text-foreground">
          <TrendingDown className="h-5 w-5 text-primary" />
          Model Comparison Table
        </h3>
        {(() => {
          
          const allStatsLoading = Object.values(allStatsQueries).some(q => q.isLoading);
          const allStatsError = Object.values(allStatsQueries).find(q => q.isError);
          
          if (allStatsLoading) {
            return (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
                <span className="ml-3 text-muted-foreground">Loading comparison data...</span>
        </div>
            );
          }
          
          if (allStatsError) {
            return (
              <div className="flex flex-col items-center justify-center py-12 text-destructive">
                <AlertCircle className="h-5 w-5 mb-2" />
                <span>Failed to load comparison data</span>
            </div>
            );
          }
          
          
          const comparisonData: Record<string, Record<string, number>> = {};
          const statOrder = ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', '3PM', 'Overall'];
          
          
          statOrder.forEach(stat => {
            comparisonData[stat] = {};
          });
          
          
          Object.entries(allStatsQueries).forEach(([modelKey, query]) => {
            const data = query.data;
            if (!data || !data.overallMetrics) return;
            
            const modelLabel = modelKey === 'ensemble' 
              ? 'Ensemble' 
              : modelOptions.find(o => o.value === modelKey)?.label || modelKey;
            
            data.overallMetrics.forEach(metric => {
              if (comparisonData[metric.stat]) {
                comparisonData[metric.stat][modelLabel] = metric.mae;
              }
            });
          });
          
          
          const availableModels = new Set<string>();
          Object.values(comparisonData).forEach(stats => {
            Object.keys(stats).forEach(model => availableModels.add(model));
          });
          const modelList = Array.from(availableModels).sort((a, b) => {
            
            if (a === 'Ensemble') return -1;
            if (b === 'Ensemble') return 1;
            return a.localeCompare(b);
          });
          
          if (modelList.length === 0) {
            return (
              <div className="flex items-center justify-center py-12 text-muted-foreground">
                <AlertCircle className="h-5 w-5 mr-2" />
                <span>No comparison data available</span>
          </div>
            );
          }
          
          return (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-center">Stat</TableHead>
                    {modelList.map(model => (
                      <TableHead key={model} className="text-center">
                        {model}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {statOrder.map(stat => {
                    const statData = comparisonData[stat];
                    if (!statData || Object.keys(statData).length === 0) return null;
                    
                    
                    const maeValues = Object.values(statData);
                    const bestMAE = maeValues.length > 0 ? Math.min(...maeValues) : Infinity;
                    
                    return (
                      <TableRow key={stat}>
                        <TableCell className="font-medium text-center">{stat}</TableCell>
                        {modelList.map(model => {
                          const mae = statData[model];
                          if (mae === undefined) {
                            return <TableCell key={model} className="text-center text-muted-foreground">—</TableCell>;
                          }
                          
                          const isBest = mae === bestMAE && maeValues.length > 1;
                          const colorClass = getMAEColor(stat, mae);
                          
                          return (
                            <TableCell 
                              key={model} 
                              className={cn(
                                "text-center font-medium",
                                colorClass
                              )}
                            >
                              {mae.toFixed(2)}
                              {isBest && <span className="ml-1 text-xs">★</span>}
                            </TableCell>
                          );
                        })}
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          );
        })()}
      </div>

    </div>
  );
}
