import type { ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]) {
  // Minimal cn implementation — joins truthy class names
  return inputs
    .flat()
    .filter((v) => typeof v === "string" && v.length > 0)
    .join(" ");
}
