"use client";

import * as React from "react";

import { cn } from "@/lib/utils";

const Progress = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & {
    value?: number;
  }
>(({ className, value = 0, ...props }, ref) => {
  const clampedValue = Math.max(0, Math.min(100, value));

  return (
    <div
      ref={ref}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={clampedValue}
      className={cn("relative h-2 w-full overflow-hidden rounded-full bg-secondary", className)}
      role="progressbar"
      {...props}
    >
      <div
        className="h-full w-full flex-1 bg-foreground transition-all"
        style={{ transform: `translateX(-${100 - clampedValue}%)` }}
      />
    </div>
  );
});
Progress.displayName = "Progress";

export { Progress };
