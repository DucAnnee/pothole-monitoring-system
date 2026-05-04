import Chip from "@mui/material/Chip";
import { SEVERITY, normalizeSeverity } from "~/constants/severity";

interface Props {
  level: string;
}

export function SeverityBadge({ level }: Props) {
  const key = normalizeSeverity(level);
  const { color, bg, label } = SEVERITY[key];
  return (
    <Chip
      label={label}
      size="small"
      sx={{ color, bgcolor: bg, fontWeight: 600, fontSize: 11 }}
    />
  );
}
