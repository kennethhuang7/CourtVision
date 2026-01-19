import { Info } from 'lucide-react';
import { useState } from 'react';

interface InfoTooltipProps {
  content: string;
}

export function InfoTooltip({ content }: InfoTooltipProps) {
  const [isVisible, setIsVisible] = useState(false);

  return (
    <div className="relative inline-block">
      <button
        type="button"
        onMouseEnter={() => setIsVisible(true)}
        onMouseLeave={() => setIsVisible(false)}
        onClick={() => setIsVisible(!isVisible)}
        className="text-muted-foreground hover:text-foreground transition-colors ml-1.5"
      >
        <Info className="w-3.5 h-3.5" />
      </button>
      {isVisible && (
        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 z-50 animate-in fade-in zoom-in-95 duration-150">
          <div className="bg-popover/95 backdrop-blur-md text-popover-foreground text-xs rounded-lg px-3 py-2 shadow-xl shadow-black/20 border border-border/50">
            {content}
            <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-px">
              <div className="border-4 border-transparent border-t-popover" />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
