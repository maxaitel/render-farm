import * as React from "react";

import { cn } from "@/lib/utils";

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost";
}

const variants: Record<NonNullable<ButtonProps["variant"]>, string> = {
  primary: "bg-ember text-white hover:bg-[#b31825]",
  secondary:
    "border border-line bg-white text-ink hover:border-ember/35 hover:bg-mist",
  ghost: "text-ink/72 hover:bg-black/5",
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "primary", ...props }, ref) => (
    <button
      className={cn(
        "inline-flex h-11 items-center justify-center rounded-full px-5 text-sm font-semibold tracking-[0.02em] transition duration-200 disabled:cursor-not-allowed disabled:opacity-50",
        variants[variant],
        className,
      )}
      ref={ref}
      {...props}
    />
  ),
);

Button.displayName = "Button";
