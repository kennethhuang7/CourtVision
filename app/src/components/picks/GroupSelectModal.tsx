import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Input } from '@/components/ui/input';
import { Search, Users } from 'lucide-react';
import { getInitials } from '@/lib/utils';

interface Group {
  id: string;
  name: string;
  description?: string | null;
  group_picture_url?: string | null;
  member_count?: number;
}

interface GroupSelectModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  groups: Group[];
  selectedGroupIds: string[];
  onConfirm: (groupIds: string[]) => void;
}

export function GroupSelectModal({
  open,
  onOpenChange,
  groups,
  selectedGroupIds,
  onConfirm,
}: GroupSelectModalProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set(selectedGroupIds));
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (open) {
      setSelected(new Set(selectedGroupIds));
      setSearchQuery('');
    }
  }, [open, selectedGroupIds]);

  const filteredGroups = groups.filter(group => {
    const query = searchQuery.toLowerCase();
    return group.name.toLowerCase().includes(query) ||
           (group.description && group.description.toLowerCase().includes(query));
  });

  const toggleGroup = (groupId: string) => {
    const newSelected = new Set(selected);
    if (newSelected.has(groupId)) {
      newSelected.delete(groupId);
    } else {
      newSelected.add(groupId);
    }
    setSelected(newSelected);
  };

  const handleSelectAll = () => {
    if (selected.size === groups.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(groups.map(g => g.id)));
    }
  };

  const handleConfirm = () => {
    onConfirm(Array.from(selected));
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Select Groups</DialogTitle>
          <DialogDescription>
            Choose which groups can see this pick
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search groups..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>

          <div className="flex items-center justify-between p-3 rounded-lg border">
            <div className="flex items-center gap-2">
              <Checkbox
                checked={selected.size === groups.length && groups.length > 0}
                onCheckedChange={handleSelectAll}
                id="select-all-groups"
              />
              <label htmlFor="select-all-groups" className="text-sm font-medium cursor-pointer">
                Select All ({groups.length})
              </label>
            </div>
            <span className="text-xs text-muted-foreground">
              {selected.size} selected
            </span>
          </div>

          <div className="max-h-[400px] overflow-y-auto space-y-2">
            {filteredGroups.length === 0 ? (
              <p className="text-center text-sm text-muted-foreground py-8">
                {searchQuery ? 'No groups found' : "You're not in any groups yet"}
              </p>
            ) : (
              filteredGroups.map((group) => {
                const isSelected = selected.has(group.id);

                return (
                  <div
                    key={group.id}
                    className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent/50 cursor-pointer transition-colors"
                    onClick={() => toggleGroup(group.id)}
                  >
                    <Checkbox
                      checked={isSelected}
                      onCheckedChange={() => toggleGroup(group.id)}
                    />
                    <Avatar className="h-10 w-10">
                      {group.group_picture_url ? (
                        <AvatarImage src={group.group_picture_url} alt={group.name} />
                      ) : (
                        <AvatarFallback className="bg-gradient-to-br from-primary to-accent text-white">
                          <Users className="h-5 w-5" />
                        </AvatarFallback>
                      )}
                    </Avatar>
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-foreground truncate">{group.name}</p>
                      {group.description && (
                        <p className="text-xs text-muted-foreground truncate">{group.description}</p>
                      )}
                      {group.member_count !== undefined && (
                        <p className="text-xs text-muted-foreground">
                          {group.member_count} {group.member_count === 1 ? 'member' : 'members'}
                        </p>
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={selected.size === 0}>
            Confirm ({selected.size})
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
