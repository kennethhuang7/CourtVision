import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}


export function getInitials(name: string): string {
  if (!name || name.trim().length === 0) {
    return 'U';
  }
  
  const trimmed = name.trim();
  const words = trimmed.split(/\s+/).filter(w => w.length > 0);
  
  if (words.length === 1) {
    
    return trimmed.slice(0, 2).toUpperCase();
  } else {
    
    return words
      .slice(0, 2)
      .map(w => w[0])
      .join('')
      .toUpperCase();
  }
}
