import * as React from "react";
import { Calendar, ChevronLeft, ChevronRight } from "lucide-react";
import { isSameDay } from "date-fns";
import { DayPicker } from "react-day-picker";
import { cn } from "@/lib/utils";
import { buttonVariants } from "@/components/ui/button";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { formatUserDate } from "@/lib/dateUtils";
import { useTheme } from "@/contexts/ThemeContext";

interface DatePickerProps {
  selectedDate: Date;
  onDateChange: (date: Date) => void;
  disabled?: (date: Date) => boolean;
  className?: string;
  buttonClassName?: string;
}

export function DatePicker({
  selectedDate,
  onDateChange,
  disabled,
  className,
  buttonClassName,
}: DatePickerProps) {
  const { dateFormat } = useTheme();
  const [isOpen, setIsOpen] = React.useState(false);
  const [month, setMonth] = React.useState(selectedDate);
  const today = new Date();

  // Update month when selectedDate changes (when navigating with prev/next buttons)
  React.useEffect(() => {
    setMonth(selectedDate);
  }, [selectedDate]);

  const handleToday = () => {
    if (!isSelectedToday && (!disabled || !disabled(today))) {
      onDateChange(today);
      setIsOpen(false);
    }
  };

  const isSelectedToday = isSameDay(selectedDate, today);

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className={cn(
            "gap-2 relative group overflow-hidden transition-all duration-300 hover:border-primary/50 shrink-0",
            buttonClassName
          )}
        >
          <div className="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/10 to-primary/0 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
          <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 blur-xl bg-primary/20" />
          <Calendar className="h-4 w-4 relative z-10 shrink-0" />
          <span className="relative z-10 whitespace-nowrap">
            {formatUserDate(selectedDate, dateFormat, true)}
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0 max-w-[calc(100vw-2rem)]" align="end">
        <div className="density-padding">
          <DayPicker
            mode="single"
            selected={selectedDate}
            onSelect={(date) => {
              if (date) {
                onDateChange(date);
                setIsOpen(false);
              }
            }}
            disabled={disabled}
            month={month}
            onMonthChange={setMonth}
            showOutsideDays={true}
            className="p-0"
            classNames={{
              months: "flex flex-col sm:flex-row space-y-4 sm:space-x-4 sm:space-y-0",
              month: "space-y-4",
              caption: "flex items-center justify-between pt-1 mb-2 px-1 relative min-h-[2rem]",
              caption_label: "text-sm font-semibold text-foreground absolute left-1/2 -translate-x-1/2 whitespace-nowrap",
              nav: "flex items-center w-full justify-between",
              nav_button: cn(
                buttonVariants({ variant: "ghost" }),
                "h-7 w-7 min-w-[1.75rem] min-h-[1.75rem] p-0 hover:bg-muted shrink-0"
              ),
              nav_button_previous: "",
              nav_button_next: "",
              table: "w-full border-collapse space-y-1",
              head_row: "flex",
              head_cell: "text-muted-foreground rounded-md min-w-[2.25rem] w-9 font-normal text-xs flex items-center justify-center",
              row: "flex w-full mt-2",
              cell: "min-w-[2.25rem] min-h-[2.25rem] h-9 w-9 text-center text-sm p-0 relative [&:has([aria-selected].day-range-end)]:rounded-r-md [&:has([aria-selected].day-outside)]:bg-accent/50 first:[&:has([aria-selected])]:rounded-l-md last:[&:has([aria-selected])]:rounded-r-md focus-within:relative focus-within:z-20",
              day: cn(
                buttonVariants({ variant: "ghost" }),
                "h-9 w-9 min-w-[2.25rem] min-h-[2.25rem] p-0 font-normal aria-selected:opacity-100 relative"
              ),
              day_range_end: "day-range-end",
              day_selected:
                "bg-primary/20 text-primary rounded-md hover:bg-primary/25 focus:bg-primary/25",
              day_today: cn(
                "font-semibold relative",
                // Bullet indicator for today (when not selected) - filled purple dot
                "after:content-[''] after:absolute after:bottom-0.5 after:left-1/2 after:-translate-x-1/2",
                "after:w-1 after:h-1 after:rounded-full",
                "after:bg-primary after:z-[1]",
                // When today is also selected, use white filled dot
                "[&.day_selected]:after:bg-white",
                "[&.day_selected]:after:w-1.5 [&.day_selected]:after:h-1.5"
              ),
              day_outside:
                "day-outside text-muted-foreground opacity-50 aria-selected:bg-accent/50 aria-selected:text-muted-foreground aria-selected:opacity-30",
              day_disabled: "text-muted-foreground opacity-50",
              day_range_middle: "aria-selected:bg-accent aria-selected:text-accent-foreground",
              day_hidden: "invisible",
            }}
            components={{
              IconLeft: ({ ..._props }) => <ChevronLeft className="h-4 w-4" />,
              IconRight: ({ ..._props }) => <ChevronRight className="h-4 w-4" />,
            }}
          />
          {/* Today Button inside calendar */}
          <div className="density-padding-x density-padding-y border-t border-border mt-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleToday}
              disabled={isSelectedToday || (disabled && disabled(today))}
              className={cn(
                "w-full text-xs min-h-[2rem]",
                isSelectedToday && "opacity-50 cursor-not-allowed"
              )}
            >
              Today
            </Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}

