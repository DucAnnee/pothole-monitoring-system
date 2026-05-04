export const SEVERITY = {
  critical: { color: "#DC2626", bg: "#FEF2F2", label: "Critical" },
  high: { color: "#EA580C", bg: "#FFF7ED", label: "High" },
  moderate: { color: "#D97706", bg: "#FFFBEB", label: "Moderate" },
  minor: { color: "#16A34A", bg: "#F0FDF4", label: "Minor" },
} as const;

export type SeverityLevel = keyof typeof SEVERITY;

export function normalizeSeverity(raw: string): SeverityLevel {
  const lower = raw?.toLowerCase() ?? "";
  if (lower in SEVERITY) return lower as SeverityLevel;
  return "minor";
}
