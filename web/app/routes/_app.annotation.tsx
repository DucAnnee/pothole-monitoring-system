import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import Slider from "@mui/material/Slider";
import TextField from "@mui/material/TextField";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import {
  ArrowLeft,
  MousePointer,
  Pencil,
  Pentagon,
  Redo,
  RotateCcw,
  Spline,
  Undo,
  ZoomIn,
  ZoomOut,
} from "lucide-react";
import { useState } from "react";
import { useNavigate } from "react-router";

export const handle = { title: "Annotation Editor" };

const INITIAL_POLYGON = [
  { x: 200, y: 120 },
  { x: 350, y: 100 },
  { x: 380, y: 230 },
  { x: 220, y: 260 },
];

export default function AnnotationPage() {
  const navigate = useNavigate();
  const [tool, setTool] = useState("select");
  const [opacity, setOpacity] = useState(80);
  const [zoom, setZoom] = useState(100);
  const [vertices, setVertices] = useState(INITIAL_POLYGON);
  const [dragging, setDragging] = useState<number | null>(null);
  const [notes, setNotes] = useState("");
  const [autoSegRunning, setAutoSegRunning] = useState(false);

  function handleVertexMouseDown(i: number) {
    setDragging(i);
  }

  function handleSvgMouseMove(e: React.MouseEvent<SVGSVGElement>) {
    if (dragging === null) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    setVertices((prev) => prev.map((v, idx) => (idx === dragging ? { x, y } : v)));
  }

  function handleSvgMouseUp() {
    setDragging(null);
  }

  function runAutoSeg() {
    setAutoSegRunning(true);
    setTimeout(() => setAutoSegRunning(false), 2000);
  }

  const polygonPoints = vertices.map((v) => `${v.x},${v.y}`).join(" ");

  return (
    <Box
      sx={{
        position: "absolute",
        inset: 0,
        top: 56,
        display: "flex",
        flexDirection: "column",
        bgcolor: "#1A2332",
      }}
    >
      {/* Top toolbar */}
      <Box
        sx={{
          height: 48,
          bgcolor: "#0F1824",
          display: "flex",
          alignItems: "center",
          px: 2,
          gap: 2,
          borderBottom: "1px solid #2A3444",
          flexShrink: 0,
        }}
      >
        <IconButton size="small" sx={{ color: "#8895A7" }} onClick={() => navigate(-1)}>
          <ArrowLeft size={16} />
        </IconButton>
        <Typography variant="caption" color="#8895A7">frame_0042.jpg</Typography>
        <Box sx={{ flex: 1 }} />
        <Typography variant="caption" color="#5A6B7F">Opacity</Typography>
        <Slider
          value={opacity}
          onChange={(_, v) => setOpacity(v as number)}
          min={0}
          max={100}
          size="small"
          sx={{ width: 100, color: "#1488DB" }}
        />
        <Typography variant="caption" color="#5A6B7F">{opacity}%</Typography>
        <IconButton size="small" sx={{ color: "#8895A7" }} onClick={() => setZoom((z) => Math.min(z + 10, 200))}>
          <ZoomIn size={14} />
        </IconButton>
        <Typography variant="caption" color="#8895A7">{zoom}%</Typography>
        <IconButton size="small" sx={{ color: "#8895A7" }} onClick={() => setZoom((z) => Math.max(z - 10, 25))}>
          <ZoomOut size={14} />
        </IconButton>
      </Box>

      {/* Body */}
      <Box sx={{ flex: 1, display: "flex", overflow: "hidden" }}>
        {/* Left panel */}
        <Box
          sx={{
            width: 240,
            flexShrink: 0,
            bgcolor: "#0F1824",
            borderRight: "1px solid #2A3444",
            p: 2,
            display: "flex",
            flexDirection: "column",
            gap: 2,
            overflowY: "auto",
          }}
        >
          <Box>
            <Typography variant="caption" color="#5A6B7F">DETECTION</Typography>
            <Typography variant="caption" color="#8895A7" display="block" mt={0.5}>ID: det-0042</Typography>
            <Typography variant="caption" color="#8895A7" display="block">Severity: CRITICAL</Typography>
            <Typography variant="caption" color="#8895A7" display="block">Depth: 8.4 cm</Typography>
            <Typography variant="caption" color="#8895A7" display="block">Area: 312 cm²</Typography>
          </Box>

          <Box>
            <Typography variant="caption" color="#5A6B7F">CONFIDENCE</Typography>
            <Box sx={{ display: "flex", justifyContent: "space-between", mt: 0.5 }}>
              <Typography variant="caption" color="#8895A7">42%</Typography>
            </Box>
            <LinearProgress
              variant="determinate"
              value={42}
              sx={{
                height: 6,
                borderRadius: 3,
                bgcolor: "#2A3444",
                "& .MuiLinearProgress-bar": { bgcolor: "#D97706" },
              }}
            />
          </Box>

          <Box>
            <Typography variant="caption" color="#5A6B7F">OPERATOR NOTES</Typography>
            <TextField
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              multiline
              rows={4}
              fullWidth
              size="small"
              placeholder="Add notes…"
              sx={{
                mt: 0.5,
                "& .MuiOutlinedInput-root": {
                  bgcolor: "#1A2332",
                  color: "#8895A7",
                  fontSize: 12,
                  "& fieldset": { borderColor: "#2A3444" },
                },
              }}
            />
          </Box>
        </Box>

        {/* Canvas */}
        <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", overflow: "hidden" }}>
          <svg
            width="600"
            height="400"
            style={{ background: "#2A3444", borderRadius: 8, cursor: dragging !== null ? "grabbing" : "default" }}
            onMouseMove={handleSvgMouseMove}
            onMouseUp={handleSvgMouseUp}
            onMouseLeave={handleSvgMouseUp}
          >
            {/* Simulated road */}
            <rect x={0} y={0} width={600} height={400} fill="#2A3444" />
            <line x1={0} y1={200} x2={600} y2={200} stroke="#3A4A5A" strokeWidth={60} />
            {[50, 150, 250, 350, 450, 550].map((x) => (
              <rect key={x} x={x} y={194} width={40} height={12} fill="#4A5A6A" rx={2} />
            ))}

            {/* Polygon */}
            <polygon
              points={polygonPoints}
              fill="rgba(20,136,219,0.25)"
              stroke="#1488DB"
              strokeWidth={2}
            />

            {/* Vertices */}
            {vertices.map((v, i) => (
              <circle
                key={i}
                cx={v.x}
                cy={v.y}
                r={6}
                fill="#1488DB"
                stroke="#fff"
                strokeWidth={1.5}
                style={{ cursor: "grab" }}
                onMouseDown={() => handleVertexMouseDown(i)}
              />
            ))}
          </svg>
        </Box>

        {/* Right toolbar */}
        <Box
          sx={{
            width: 56,
            flexShrink: 0,
            bgcolor: "#0F1824",
            borderLeft: "1px solid #2A3444",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            py: 1,
            gap: 0.5,
          }}
        >
          <ToggleButtonGroup
            value={tool}
            exclusive
            onChange={(_, v) => v && setTool(v)}
            orientation="vertical"
            size="small"
            sx={{ "& .MuiToggleButton-root": { color: "#5A6B7F", border: "none", "&.Mui-selected": { color: "#1488DB", bgcolor: "#1488DB22" } } }}
          >
            <Tooltip title="Select" placement="left">
              <ToggleButton value="select"><MousePointer size={16} /></ToggleButton>
            </Tooltip>
            <Tooltip title="Pan" placement="left">
              <ToggleButton value="pan"><Spline size={16} /></ToggleButton>
            </Tooltip>
            <Tooltip title="Polygon" placement="left">
              <ToggleButton value="polygon"><Pentagon size={16} /></ToggleButton>
            </Tooltip>
            <Tooltip title="Edit Vertex" placement="left">
              <ToggleButton value="vertex"><Pencil size={16} /></ToggleButton>
            </Tooltip>
          </ToggleButtonGroup>

          <Box sx={{ height: 1, bgcolor: "#2A3444", width: "80%", my: 0.5 }} />

          {[
            { icon: <Undo size={14} />, title: "Undo" },
            { icon: <Redo size={14} />, title: "Redo" },
            { icon: <RotateCcw size={14} />, title: "Reset" },
          ].map(({ icon, title }) => (
            <Tooltip key={title} title={title} placement="left">
              <IconButton size="small" sx={{ color: "#5A6B7F" }}>{icon}</IconButton>
            </Tooltip>
          ))}

          <Box sx={{ flex: 1 }} />

          <Tooltip title={autoSegRunning ? "Running…" : "Auto Segment (SAM3)"} placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button
                size="small"
                variant="contained"
                fullWidth
                onClick={runAutoSeg}
                disabled={autoSegRunning}
                sx={{ fontSize: 9, px: 0.5, minWidth: 0, bgcolor: autoSegRunning ? "#2A3444" : undefined }}
              >
                {autoSegRunning ? "…" : "SAM"}
              </Button>
            </Box>
          </Tooltip>

          <Tooltip title="Save" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button size="small" variant="outlined" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0, color: "#8895A7", borderColor: "#2A3444" }}>
                Save
              </Button>
            </Box>
          </Tooltip>

          <Tooltip title="Submit" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button size="small" variant="contained" color="success" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0 }}>
                Submit
              </Button>
            </Box>
          </Tooltip>
        </Box>
      </Box>
    </Box>
  );
}
