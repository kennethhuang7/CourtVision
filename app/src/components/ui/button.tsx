import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "relative inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-medium ring-offset-background transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 overflow-hidden [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 active:scale-[0.98] active:transition-transform active:duration-75",
  {
    variants: {
      variant: {
        default:
          "bg-primary text-primary-foreground shadow-lg shadow-primary/20 " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/20 before:via-white/5 before:to-transparent before:opacity-60 before:transition-opacity " +
          "hover:shadow-primary/40 hover:before:opacity-80 active:before:opacity-40",
        destructive:
          "bg-destructive text-destructive-foreground shadow-lg shadow-destructive/20 " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/15 before:to-transparent before:opacity-50 before:transition-opacity " +
          "hover:bg-destructive/90 hover:shadow-destructive/35 hover:before:opacity-70",
        outline:
          "border border-border/60 bg-card/40 backdrop-blur-sm text-foreground " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/8 before:to-transparent before:opacity-0 before:transition-opacity " +
          "hover:bg-card/60 hover:border-primary/40 hover:before:opacity-100",
        secondary:
          "bg-secondary text-secondary-foreground " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/8 before:to-transparent before:opacity-40 before:transition-opacity " +
          "hover:bg-secondary/80 hover:before:opacity-60",
        ghost:
          "hover:bg-secondary/70 hover:text-secondary-foreground " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/5 before:to-transparent before:opacity-0 before:transition-opacity " +
          "hover:before:opacity-100",
        link: "text-primary underline-offset-4 hover:underline hover:text-primary/80",
        hero:
          "bg-primary text-primary-foreground font-semibold shadow-lg shadow-primary/30 " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/25 before:via-white/10 before:to-transparent before:opacity-70 before:transition-opacity " +
          "hover:shadow-primary/50 hover:scale-[1.02] hover:before:opacity-90 active:scale-[0.98] active:before:opacity-50",
        glass:
          "bg-card/50 backdrop-blur-md border border-border/40 text-foreground " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/10 before:to-transparent before:opacity-50 before:transition-opacity " +
          "hover:bg-card/70 hover:border-primary/30 hover:before:opacity-70",
        success:
          "bg-success text-success-foreground shadow-lg shadow-success/20 " +
          "before:absolute before:inset-0 before:bg-gradient-to-b before:from-white/15 before:to-transparent before:opacity-50 before:transition-opacity " +
          "hover:bg-success/90 hover:shadow-success/35 hover:before:opacity-70",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-9 rounded-md px-3",
        lg: "h-12 rounded-lg px-8 text-base",
        xl: "h-14 rounded-xl px-10 text-lg",
        icon: "h-10 w-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";
    return <Comp className={cn(buttonVariants({ variant, size, className }))} ref={ref} {...props} />;
  },
);
Button.displayName = "Button";

export { Button, buttonVariants };
