import Box from "@mui/material/Box";

interface Props {
  color: string;
  size?: number;
  title?: string;
}

export function StatusDot({ color, size = 8, title }: Props) {
  return (
    <Box
      component="span"
      title={title}
      sx={{
        display: "inline-block",
        width: size,
        height: size,
        borderRadius: "50%",
        bgcolor: color,
        flexShrink: 0,
      }}
    />
  );
}
