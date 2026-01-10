import { useState, useMemo } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Trash2, Calendar, HardDrive, Clock, ChevronUp, ChevronDown, BarChart3 } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { toast } from 'sonner';
import { useTheme } from '@/contexts/ThemeContext';
import { formatTableDate } from '@/lib/dateUtils';

export interface CacheEntry {
  date: string;
  type: 'prediction' | 'pickFinder' | 'trends';
  size: number;
  cachedAt: number;
  models?: string;
}

export interface ModelPerformanceEntry {
  cacheKey: string;
  timePeriod: string;
  stat: string;
  models: string[];
  size: number;
  cachedAt: number;
}

interface CacheManagementModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  entries: CacheEntry[];
  modelPerformanceEntries: ModelPerformanceEntry[];
  onDelete: (dates: string[]) => Promise<void>;
  onDeleteModelPerformance: (cacheKeys: string[]) => Promise<void>;
  onRefresh: () => void;
}

type PredictionSortField = 'date' | 'type' | 'size' | 'cachedAt' | 'daysAgo';
type ModelPerfSortField = 'timePeriod' | 'stat' | 'models' | 'size' | 'cachedAt';
type SortDirection = 'asc' | 'desc';

export function CacheManagementModal({
  open,
  onOpenChange,
  entries,
  modelPerformanceEntries,
  onDelete,
  onDeleteModelPerformance,
  onRefresh,
}: CacheManagementModalProps) {
  const { dateFormat } = useTheme();

  // Predictions tab state
  const [selectedPredictions, setSelectedPredictions] = useState<Set<string>>(new Set());
  const [predictionSortField, setPredictionSortField] = useState<PredictionSortField>('date');
  const [predictionSortDirection, setPredictionSortDirection] = useState<SortDirection>('desc');
  const [isDeletingPredictions, setIsDeletingPredictions] = useState(false);

  // Model performance tab state
  const [selectedModelPerf, setSelectedModelPerf] = useState<Set<string>>(new Set());
  const [modelPerfSortField, setModelPerfSortField] = useState<ModelPerfSortField>('cachedAt');
  const [modelPerfSortDirection, setModelPerfSortDirection] = useState<SortDirection>('desc');
  const [isDeletingModelPerf, setIsDeletingModelPerf] = useState(false);

  // Process prediction entries (group by date)
  const predictionEntriesWithDaysAgo = useMemo(() => {
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const groupedByDate = new Map<string, {
      baseDate: string;
      type: 'prediction';
      totalSize: number;
      cachedAt: number;
      modelCombinations: number;
      originalKeys: string[];
    }>();

    entries.forEach(entry => {
      let baseDate = entry.date;
      if (entry.date.includes('|models:')) {
        baseDate = entry.date.split('|models:')[0];
      }
      const key = `${baseDate}-${entry.type}`;

      const modelCount = entry.models ? entry.models.split('|').length : 0;

      if (groupedByDate.has(key)) {
        const existing = groupedByDate.get(key)!;
        existing.totalSize += entry.size;
        existing.cachedAt = Math.max(existing.cachedAt, entry.cachedAt);
        existing.modelCombinations = Math.max(existing.modelCombinations, modelCount);
        existing.originalKeys.push(entry.date);
      } else {
        groupedByDate.set(key, {
          baseDate,
          type: 'prediction',
          totalSize: entry.size,
          cachedAt: entry.cachedAt,
          modelCombinations: modelCount,
          originalKeys: [entry.date],
        });
      }
    });

    return Array.from(groupedByDate.values()).map(group => {
      let entryDate: Date;
      try {
        if (group.baseDate.includes('-')) {
          const parts = group.baseDate.split('-');
          if (parts.length === 3) {
            const year = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10);
            const day = parseInt(parts[2], 10);

            if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
              entryDate = new Date(year, month - 1, day);
              entryDate.setHours(0, 0, 0, 0);
            } else {
              entryDate = new Date(group.baseDate);
            }
          } else {
            entryDate = new Date(group.baseDate);
          }
        } else {
          entryDate = new Date(group.baseDate);
        }

        if (isNaN(entryDate.getTime())) {
          entryDate = new Date();
        }
      } catch {
        entryDate = new Date();
      }

      const diffTime = today.getTime() - entryDate.getTime();
      const daysAgo = Math.floor(diffTime / (1000 * 60 * 60 * 24));

      return {
        date: group.baseDate,
        baseDate: group.baseDate,
        type: group.type,
        size: group.totalSize,
        cachedAt: group.cachedAt,
        daysAgo: isNaN(daysAgo) ? 0 : daysAgo,
        modelCount: group.modelCombinations,
        originalKeys: group.originalKeys,
      };
    });
  }, [entries]);

  // Sort prediction entries
  const sortedPredictions = useMemo(() => {
    const sorted = [...predictionEntriesWithDaysAgo].sort((a, b) => {
      let comparison = 0;

      switch (predictionSortField) {
        case 'date':
          comparison = a.baseDate.localeCompare(b.baseDate);
          break;
        case 'type':
          comparison = a.type.localeCompare(b.type);
          break;
        case 'size':
          comparison = a.size - b.size;
          break;
        case 'cachedAt':
          comparison = a.cachedAt - b.cachedAt;
          break;
        case 'daysAgo':
          comparison = a.daysAgo - b.daysAgo;
          break;
      }

      return predictionSortDirection === 'asc' ? comparison : -comparison;
    });

    return sorted;
  }, [predictionEntriesWithDaysAgo, predictionSortField, predictionSortDirection]);

  // Sort model performance entries
  const sortedModelPerf = useMemo(() => {
    const sorted = [...modelPerformanceEntries].sort((a, b) => {
      let comparison = 0;

      switch (modelPerfSortField) {
        case 'timePeriod':
          // Sort by numeric value for time periods
          const aVal = a.timePeriod === 'all' ? 999999 : parseInt(a.timePeriod);
          const bVal = b.timePeriod === 'all' ? 999999 : parseInt(b.timePeriod);
          comparison = aVal - bVal;
          break;
        case 'stat':
          comparison = a.stat.localeCompare(b.stat);
          break;
        case 'models':
          comparison = a.models.length - b.models.length;
          break;
        case 'size':
          comparison = a.size - b.size;
          break;
        case 'cachedAt':
          comparison = a.cachedAt - b.cachedAt;
          break;
      }

      return modelPerfSortDirection === 'asc' ? comparison : -comparison;
    });

    return sorted;
  }, [modelPerformanceEntries, modelPerfSortField, modelPerfSortDirection]);

  // Prediction handlers
  const handlePredictionSort = (field: PredictionSortField) => {
    if (predictionSortField === field) {
      setPredictionSortDirection(predictionSortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setPredictionSortField(field);
      setPredictionSortDirection('desc');
    }
  };

  const handleSelectAllPredictions = () => {
    if (selectedPredictions.size === sortedPredictions.length) {
      setSelectedPredictions(new Set());
    } else {
      setSelectedPredictions(new Set(sortedPredictions.map(e => e.baseDate)));
    }
  };

  const togglePrediction = (baseDate: string) => {
    const newSelected = new Set(selectedPredictions);
    if (newSelected.has(baseDate)) {
      newSelected.delete(baseDate);
    } else {
      newSelected.add(baseDate);
    }
    setSelectedPredictions(newSelected);
  };

  const handleDeletePredictions = async () => {
    if (selectedPredictions.size === 0) return;

    setIsDeletingPredictions(true);
    try {
      const keysToDelete: string[] = [];
      sortedPredictions.forEach(entry => {
        if (selectedPredictions.has(entry.baseDate)) {
          keysToDelete.push(...entry.originalKeys);
        }
      });

      await onDelete(keysToDelete);
      setSelectedPredictions(new Set());
      onRefresh();
      toast.success(`Deleted ${selectedPredictions.size} date${selectedPredictions.size === 1 ? '' : 's'} (${keysToDelete.length} cache ${keysToDelete.length === 1 ? 'entry' : 'entries'})`);
    } catch (error) {
      toast.error('Failed to delete cache entries');
    } finally {
      setIsDeletingPredictions(false);
    }
  };

  // Model performance handlers
  const handleModelPerfSort = (field: ModelPerfSortField) => {
    if (modelPerfSortField === field) {
      setModelPerfSortDirection(modelPerfSortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setModelPerfSortField(field);
      setModelPerfSortDirection('desc');
    }
  };

  const handleSelectAllModelPerf = () => {
    if (selectedModelPerf.size === sortedModelPerf.length) {
      setSelectedModelPerf(new Set());
    } else {
      setSelectedModelPerf(new Set(sortedModelPerf.map(e => e.cacheKey)));
    }
  };

  const toggleModelPerf = (cacheKey: string) => {
    const newSelected = new Set(selectedModelPerf);
    if (newSelected.has(cacheKey)) {
      newSelected.delete(cacheKey);
    } else {
      newSelected.add(cacheKey);
    }
    setSelectedModelPerf(newSelected);
  };

  const handleDeleteModelPerf = async () => {
    if (selectedModelPerf.size === 0) return;

    setIsDeletingModelPerf(true);
    try {
      await onDeleteModelPerformance(Array.from(selectedModelPerf));
      setSelectedModelPerf(new Set());
      onRefresh();
      toast.success(`Deleted ${selectedModelPerf.size} model performance ${selectedModelPerf.size === 1 ? 'entry' : 'entries'}`);
    } catch (error) {
      toast.error('Failed to delete model performance entries');
    } finally {
      setIsDeletingModelPerf(false);
    }
  };

  // Helper functions
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
  };

  const formatCacheDate = (dateStr: string) => {
    const formatted = formatTableDate(dateStr, dateFormat);
    return formatted.replace(/-/g, '/');
  };

  const formatTimePeriod = (timePeriod: string) => {
    if (timePeriod === 'all') return 'All time';
    const days = parseInt(timePeriod);
    return `Last ${days} days`;
  };

  const formatStat = (stat: string) => {
    const statMap: Record<string, string> = {
      points: 'Points',
      rebounds: 'Rebounds',
      assists: 'Assists',
      steals: 'Steals',
      blocks: 'Blocks',
      turnovers: 'Turnovers',
      threePointers: '3-Pointers',
      overall: 'Overall',
    };
    return statMap[stat] || stat;
  };

  const PredictionSortIcon = ({ field }: { field: PredictionSortField }) => {
    if (predictionSortField !== field) return null;
    return predictionSortDirection === 'asc' ?
      <ChevronUp className="h-4 w-4" /> :
      <ChevronDown className="h-4 w-4" />;
  };

  const ModelPerfSortIcon = ({ field }: { field: ModelPerfSortField }) => {
    if (modelPerfSortField !== field) return null;
    return modelPerfSortDirection === 'asc' ?
      <ChevronUp className="h-4 w-4" /> :
      <ChevronDown className="h-4 w-4" />;
  };

  const totalPredictionSize = sortedPredictions.reduce((sum, entry) => sum + entry.size, 0);
  const selectedPredictionSize = sortedPredictions
    .filter(e => selectedPredictions.has(e.baseDate))
    .reduce((sum, entry) => sum + entry.size, 0);

  const totalModelPerfSize = sortedModelPerf.reduce((sum, entry) => sum + entry.size, 0);
  const selectedModelPerfSize = sortedModelPerf
    .filter(e => selectedModelPerf.has(e.cacheKey))
    .reduce((sum, entry) => sum + entry.size, 0);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-5xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Manage Cached Data</DialogTitle>
          <DialogDescription>
            View and delete cached data. Total: {formatBytes(totalPredictionSize + totalModelPerfSize)} across {sortedPredictions.length + sortedModelPerf.length} entries.
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="predictions" className="flex-1 flex flex-col overflow-hidden">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="predictions">
              <Calendar className="h-4 w-4 mr-2" />
              Predictions ({sortedPredictions.length})
            </TabsTrigger>
            <TabsTrigger value="modelPerformance">
              <BarChart3 className="h-4 w-4 mr-2" />
              Model Performance ({sortedModelPerf.length})
            </TabsTrigger>
          </TabsList>

          {/* Predictions Tab */}
          <TabsContent value="predictions" className="flex-1 flex flex-col overflow-hidden mt-4">
            <div className="flex items-center justify-between py-3 border-b">
              <div className="flex items-center gap-3">
                <Checkbox
                  checked={selectedPredictions.size === sortedPredictions.length && sortedPredictions.length > 0}
                  onCheckedChange={handleSelectAllPredictions}
                  id="select-all-predictions"
                />
                <label htmlFor="select-all-predictions" className="text-sm font-medium cursor-pointer">
                  Select All ({sortedPredictions.length})
                </label>
                {selectedPredictions.size > 0 && (
                  <span className="text-sm text-muted-foreground">
                    • {selectedPredictions.size} selected ({formatBytes(selectedPredictionSize)})
                  </span>
                )}
              </div>

              <Button
                variant="destructive"
                size="sm"
                onClick={handleDeletePredictions}
                disabled={selectedPredictions.size === 0 || isDeletingPredictions}
                className="gap-2"
              >
                <Trash2 className="h-4 w-4" />
                Delete Selected
              </Button>
            </div>

            <div className="flex-1 overflow-auto">
              <table className="w-full">
                <thead className="sticky top-0 bg-background border-b">
                  <tr className="text-left">
                    <th className="w-12 p-3"></th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handlePredictionSort('date')}
                    >
                      <div className="flex items-center gap-2">
                        <Calendar className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Date</span>
                        <PredictionSortIcon field="date" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handlePredictionSort('daysAgo')}
                    >
                      <div className="flex items-center gap-2">
                        <Clock className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Age</span>
                        <PredictionSortIcon field="daysAgo" />
                      </div>
                    </th>
                    <th className="p-3">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Type</span>
                      </div>
                    </th>
                    <th className="p-3">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Models</span>
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handlePredictionSort('size')}
                    >
                      <div className="flex items-center gap-2">
                        <HardDrive className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Size</span>
                        <PredictionSortIcon field="size" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handlePredictionSort('cachedAt')}
                    >
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Cached</span>
                        <PredictionSortIcon field="cachedAt" />
                      </div>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedPredictions.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="p-8 text-center text-muted-foreground">
                        No cached predictions found
                      </td>
                    </tr>
                  ) : (
                    sortedPredictions.map((entry) => (
                      <tr
                        key={entry.baseDate}
                        className="border-b hover:bg-accent/50 transition-colors cursor-pointer"
                        onClick={() => togglePrediction(entry.baseDate)}
                      >
                        <td className="p-3">
                          <Checkbox
                            checked={selectedPredictions.has(entry.baseDate)}
                            onCheckedChange={() => togglePrediction(entry.baseDate)}
                          />
                        </td>
                        <td className="p-3 font-medium">
                          {formatCacheDate(entry.baseDate)}
                        </td>
                        <td className="p-3 text-muted-foreground text-sm">
                          {isNaN(entry.daysAgo) || entry.daysAgo < 0 ? (
                            formatDistanceToNow(new Date(entry.baseDate), { addSuffix: true })
                          ) : entry.daysAgo === 0 ? (
                            'Today'
                          ) : entry.daysAgo === 1 ? (
                            'Yesterday'
                          ) : (
                            `${entry.daysAgo} days ago`
                          )}
                        </td>
                        <td className="p-3">
                          <span className="px-2 py-1 rounded text-xs bg-primary/10 text-primary">
                            Prediction
                          </span>
                        </td>
                        <td className="p-3 text-sm text-muted-foreground">
                          {entry.modelCount > 0 ? (
                            <span className="px-2 py-1 rounded text-xs bg-accent/50 border border-border">
                              {entry.modelCount} {entry.modelCount === 1 ? 'model' : 'models'}
                            </span>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </td>
                        <td className="p-3 font-mono text-sm">
                          {formatBytes(entry.size)}
                        </td>
                        <td className="p-3 text-sm text-muted-foreground">
                          {formatDistanceToNow(entry.cachedAt, { addSuffix: true })}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </TabsContent>

          {/* Model Performance Tab */}
          <TabsContent value="modelPerformance" className="flex-1 flex flex-col overflow-hidden mt-4">
            <div className="flex items-center justify-between py-3 border-b">
              <div className="flex items-center gap-3">
                <Checkbox
                  checked={selectedModelPerf.size === sortedModelPerf.length && sortedModelPerf.length > 0}
                  onCheckedChange={handleSelectAllModelPerf}
                  id="select-all-modelperf"
                />
                <label htmlFor="select-all-modelperf" className="text-sm font-medium cursor-pointer">
                  Select All ({sortedModelPerf.length})
                </label>
                {selectedModelPerf.size > 0 && (
                  <span className="text-sm text-muted-foreground">
                    • {selectedModelPerf.size} selected ({formatBytes(selectedModelPerfSize)})
                  </span>
                )}
              </div>

              <Button
                variant="destructive"
                size="sm"
                onClick={handleDeleteModelPerf}
                disabled={selectedModelPerf.size === 0 || isDeletingModelPerf}
                className="gap-2"
              >
                <Trash2 className="h-4 w-4" />
                Delete Selected
              </Button>
            </div>

            <div className="flex-1 overflow-auto">
              <table className="w-full">
                <thead className="sticky top-0 bg-background border-b">
                  <tr className="text-left">
                    <th className="w-12 p-3"></th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handleModelPerfSort('timePeriod')}
                    >
                      <div className="flex items-center gap-2">
                        <Clock className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Time Period</span>
                        <ModelPerfSortIcon field="timePeriod" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handleModelPerfSort('stat')}
                    >
                      <div className="flex items-center gap-2">
                        <BarChart3 className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Statistic</span>
                        <ModelPerfSortIcon field="stat" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handleModelPerfSort('models')}
                    >
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Models</span>
                        <ModelPerfSortIcon field="models" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handleModelPerfSort('size')}
                    >
                      <div className="flex items-center gap-2">
                        <HardDrive className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium">Size</span>
                        <ModelPerfSortIcon field="size" />
                      </div>
                    </th>
                    <th
                      className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                      onClick={() => handleModelPerfSort('cachedAt')}
                    >
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Cached</span>
                        <ModelPerfSortIcon field="cachedAt" />
                      </div>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedModelPerf.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="p-8 text-center text-muted-foreground">
                        No cached model performance data found
                      </td>
                    </tr>
                  ) : (
                    sortedModelPerf.map((entry) => (
                      <tr
                        key={entry.cacheKey}
                        className="border-b hover:bg-accent/50 transition-colors cursor-pointer"
                        onClick={() => toggleModelPerf(entry.cacheKey)}
                      >
                        <td className="p-3">
                          <Checkbox
                            checked={selectedModelPerf.has(entry.cacheKey)}
                            onCheckedChange={() => toggleModelPerf(entry.cacheKey)}
                          />
                        </td>
                        <td className="p-3 font-medium">
                          {formatTimePeriod(entry.timePeriod)}
                        </td>
                        <td className="p-3">
                          <span className="px-2 py-1 rounded text-xs bg-blue-500/10 text-blue-600 dark:text-blue-400">
                            {formatStat(entry.stat)}
                          </span>
                        </td>
                        <td className="p-3 text-sm">
                          <div className="flex flex-wrap gap-1">
                            {entry.models.map(model => (
                              <span key={model} className="px-2 py-0.5 rounded text-xs bg-accent border border-border">
                                {model}
                              </span>
                            ))}
                          </div>
                        </td>
                        <td className="p-3 font-mono text-sm">
                          {formatBytes(entry.size)}
                        </td>
                        <td className="p-3 text-sm text-muted-foreground">
                          {formatDistanceToNow(entry.cachedAt, { addSuffix: true })}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </TabsContent>
        </Tabs>

        <div className="flex justify-end gap-2 pt-3 border-t">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
