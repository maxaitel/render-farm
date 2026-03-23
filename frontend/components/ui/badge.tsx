import { cn } from "@/lib/utils";

const styles = {
  queued: "bg-black/5 text-black/68",
  running: "bg-[#f7dfe1] text-[#971b28]",
  completed: "bg-[#e2ebe1] text-[#315837]",
  failed: "bg-[#f7dfdf] text-[#8a3535]",
} as const;

export function Badge({
  phase,
  className,
}: {
  phase: keyof typeof styles;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.1em]",
        styles[phase],
        className,
      )}
    >
      {phase}
    </span>
  );
}
