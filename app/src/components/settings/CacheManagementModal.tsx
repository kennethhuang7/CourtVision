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
import { Trash2, Calendar, HardDrive, Clock, ChevronUp, ChevronDown } from 'lucide-react';
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

interface CacheManagementModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  entries: CacheEntry[];
  onDelete: (dates: string[]) => Promise<void>;
  onRefresh: () => void;
}

type SortField = 'date' | 'type' | 'size' | 'cachedAt' | 'daysAgo';
type SortDirection = 'asc' | 'desc';

export function CacheManagementModal({
  open,
  onOpenChange,
  entries,
  onDelete,
  onRefresh,
}: CacheManagementModalProps) {
  const { dateFormat } = useTheme();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [sortField, setSortField] = useState<SortField>('date');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [isDeleting, setIsDeleting] = useState(false);

  
  const entriesWithDaysAgo = useMemo(() => {
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

  
  const sortedEntries = useMemo(() => {
    const sorted = [...entriesWithDaysAgo].sort((a, b) => {
      let comparison = 0;

      switch (sortField) {
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

      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return sorted;
  }, [entriesWithDaysAgo, sortField, sortDirection]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const handleSelectAll = () => {
    if (selected.size === sortedEntries.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(sortedEntries.map(e => e.baseDate)));
    }
  };

  const toggleEntry = (baseDate: string) => {
    const newSelected = new Set(selected);
    if (newSelected.has(baseDate)) {
      newSelected.delete(baseDate);
    } else {
      newSelected.add(baseDate);
    }
    setSelected(newSelected);
  };

  const handleDelete = async () => {
    if (selected.size === 0) return;

    setIsDeleting(true);
    try {
      
      const keysToDelete: string[] = [];
      sortedEntries.forEach(entry => {
        if (selected.has(entry.baseDate)) {
          keysToDelete.push(...entry.originalKeys);
        }
      });

      await onDelete(keysToDelete);
      setSelected(new Set());
      onRefresh();
      toast.success(`Deleted ${selected.size} date${selected.size === 1 ? '' : 's'} (${keysToDelete.length} cache ${keysToDelete.length === 1 ? 'entry' : 'entries'})`);
    } catch (error) {
      toast.error('Failed to delete cache entries');
    } finally {
      setIsDeleting(false);
    }
  };

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

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null;
    return sortDirection === 'asc' ?
      <ChevronUp className="h-4 w-4" /> :
      <ChevronDown className="h-4 w-4" />;
  };

  const totalSize = sortedEntries.reduce((sum, entry) => sum + entry.size, 0);
  const selectedSize = sortedEntries
    .filter(e => selected.has(e.baseDate))
    .reduce((sum, entry) => sum + entry.size, 0);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Manage Cached Data</DialogTitle>
          <DialogDescription>
            View and delete cached predictions. Total: {formatBytes(totalSize)} across {sortedEntries.length} entries.
          </DialogDescription>
        </DialogHeader>

        <div className="flex items-center justify-between py-3 border-b">
          <div className="flex items-center gap-3">
            <Checkbox
              checked={selected.size === sortedEntries.length && sortedEntries.length > 0}
              onCheckedChange={handleSelectAll}
              id="select-all"
            />
            <label htmlFor="select-all" className="text-sm font-medium cursor-pointer">
              Select All ({sortedEntries.length})
            </label>
            {selected.size > 0 && (
              <span className="text-sm text-muted-foreground">
                • {selected.size} selected ({formatBytes(selectedSize)})
              </span>
            )}
          </div>

          <Button
            variant="destructive"
            size="sm"
            onClick={handleDelete}
            disabled={selected.size === 0 || isDeleting}
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
                  onClick={() => handleSort('date')}
                >
                  <div className="flex items-center gap-2">
                    <Calendar className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium">Date</span>
                    <SortIcon field="date" />
                  </div>
                </th>
                <th
                  className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => handleSort('daysAgo')}
                >
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium">Age</span>
                    <SortIcon field="daysAgo" />
                  </div>
                </th>
                <th
                  className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => handleSort('type')}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-medium">Type</span>
                    <SortIcon field="type" />
                  </div>
                </th>
                <th className="p-3">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">Models</span>
                  </div>
                </th>
                <th
                  className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => handleSort('size')}
                >
                  <div className="flex items-center gap-2">
                    <HardDrive className="h-4 w-4 text-muted-foreground" />
                    <span className="font-medium">Size</span>
                    <SortIcon field="size" />
                  </div>
                </th>
                <th
                  className="p-3 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => handleSort('cachedAt')}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-medium">Cached</span>
                    <SortIcon field="cachedAt" />
                  </div>
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedEntries.length === 0 ? (
                <tr>
                  <td colSpan={7} className="p-8 text-center text-muted-foreground">
                    No cached data found
                  </td>
                </tr>
              ) : (
                sortedEntries.map((entry) => (
                  <tr
                    key={entry.baseDate}
                    className="border-b hover:bg-accent/50 transition-colors cursor-pointer"
                    onClick={() => toggleEntry(entry.baseDate)}
                  >
                    <td className="p-3">
                      <Checkbox
                        checked={selected.has(entry.baseDate)}
                        onCheckedChange={() => toggleEntry(entry.baseDate)}
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

        <div className="flex justify-end gap-2 pt-3 border-t">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
