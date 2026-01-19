import * as React from "react";
import { Check, ChevronsUpDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

interface ComboboxProps<T> {
  options: T[];
  value?: string;
  onValueChange: (value: string) => void;
  getLabel: (option: T) => string;
  getValue: (option: T) => string;
  placeholder?: string;
  searchPlaceholder?: string;
  emptyMessage?: string;
  disabled?: boolean;
  filterFn?: (option: T, searchQuery: string) => boolean;
}

export function Combobox<T>({
  options,
  value,
  onValueChange,
  getLabel,
  getValue,
  placeholder = "Select...",
  searchPlaceholder = "Search...",
  emptyMessage = "No results found.",
  disabled = false,
  filterFn,
}: ComboboxProps<T>) {
  const [open, setOpen] = React.useState(false);
  const [searchQuery, setSearchQuery] = React.useState("");

  const selectedOption = options.find((opt) => getValue(opt) === value);

  
  const filteredOptions = React.useMemo(() => {
    if (!searchQuery.trim()) return options;
    
    const query = searchQuery.toLowerCase();
    if (filterFn) {
      return options.filter((opt) => filterFn(opt, searchQuery));
    }
    
    
    return options.filter((opt) => {
      const label = getLabel(opt).toLowerCase();
      return label.includes(query);
    });
  }, [options, searchQuery, filterFn, getLabel]);

  
  React.useEffect(() => {
    if (!open) {
      setSearchQuery("");
    }
  }, [open]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
          disabled={disabled}
        >
          {selectedOption ? getLabel(selectedOption) : placeholder}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[var(--radix-popover-trigger-width)] p-0">
        <Command shouldFilter={false}>
          <CommandInput 
            placeholder={searchPlaceholder}
            value={searchQuery}
            onValueChange={setSearchQuery}
          />
          <CommandList>
            <CommandEmpty>{emptyMessage}</CommandEmpty>
            <CommandGroup>
              {filteredOptions.map((option) => {
                const optionValue = getValue(option);
                const optionLabel = getLabel(option);
                const isSelected = value === optionValue;
                return (
                  <CommandItem
                    key={optionValue}
                    value={optionLabel}
                    onSelect={() => {
                      onValueChange(optionValue);
                      setOpen(false);
                      setSearchQuery("");
                    }}
                  >
                    <Check
                      className={cn(
                        "mr-2 h-4 w-4",
                        isSelected ? "opacity-100" : "opacity-0"
                      )}
                    />
                    {optionLabel}
                  </CommandItem>
                );
              })}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

