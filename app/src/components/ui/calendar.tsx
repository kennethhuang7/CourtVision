import * as React from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { DayPicker } from "react-day-picker";

import { cn } from "@/lib/utils";
import { buttonVariants } from "@/components/ui/button";

export type CalendarProps = React.ComponentProps<typeof DayPicker>;

function Calendar({ className, classNames, showOutsideDays = true, ...props }: CalendarProps) {
  return (
    <DayPicker
      showOutsideDays={showOutsideDays}
      className={cn("p-3", className)}
      classNames={{
        months: "flex flex-col sm:flex-row space-y-4 sm:space-x-4 sm:space-y-0",
        month: "space-y-4",
        caption: "flex items-center justify-between pt-1 mb-2 px-1 relative min-h-[2rem]",
        caption_label: "text-sm font-semibold text-foreground absolute left-1/2 -translate-x-1/2 whitespace-nowrap",
        nav: "flex items-center w-full justify-between",
        nav_button: cn(
          buttonVariants({ variant: "ghost" }),
          "h-7 w-7 min-w-[1.75rem] min-h-[1.75rem] p-0 hover:bg-muted shrink-0",
        ),
        nav_button_previous: "",
        nav_button_next: "",
        table: "w-full border-collapse space-y-1",
        head_row: "flex",
        head_cell: "text-muted-foreground rounded-md min-w-[2.25rem] w-9 font-normal text-xs flex items-center justify-center",
        row: "flex w-full mt-2",
        cell: "min-w-[2.25rem] min-h-[2.25rem] h-9 w-9 text-center text-sm p-0 relative [&:has([aria-selected].day-range-end)]:rounded-r-md [&:has([aria-selected].day-outside)]:bg-accent/50 first:[&:has([aria-selected])]:rounded-l-md last:[&:has([aria-selected])]:rounded-r-md focus-within:relative focus-within:z-20",
        day: cn(buttonVariants({ variant: "ghost" }), "h-9 w-9 min-w-[2.25rem] min-h-[2.25rem] p-0 font-normal aria-selected:opacity-100 relative"),
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
        ...classNames,
      }}
      components={{
        IconLeft: ({ ..._props }) => <ChevronLeft className="h-4 w-4" />,
        IconRight: ({ ..._props }) => <ChevronRight className="h-4 w-4" />,
      }}
      {...props}
    />
  );
}
Calendar.displayName = "Calendar";

export { Calendar };
