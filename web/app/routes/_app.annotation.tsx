import type { Route } from "./+types/_app.annotation";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import TextField from "@mui/material/TextField";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { ArrowLeft, MousePointer, Pentagon, ZoomIn, ZoomOut } from "lucide-react";
import { useCallback, useEffect, useId, useRef, useState } from "react";
import { Form, useActionData, useLoaderData, useNavigate, useNavigation } from "react-router";
import {
  buildAnnotationLabel,
  parseAnnotationActionPayload,
  type Polygon,
} from "~/lib/annotation-contract";
import {
  insertAnnotation,
  insertAuditLog,
  queryReviewTaskDetail,
  updateReviewTaskStatus,
} from "~/lib/postgis.server";

export const handle = { title: "Annotation Editor" };

export function ErrorBoundary() {
  return (
    <Box sx={{ p: 4 }}>
      <Alert severity="warning" sx={{ mb: 2 }}>
        No review task selected. Open a task from the Low Confidence queue.
      </Alert>
      <Button component="a" href="/lowconf" variant="outlined" size="small">
        Go to Low Confidence Queue
      </Button>
    </Box>
  );
}

type AnnotationActionData =
  | { ok: false; error: string }
  | { ok: true; annotationId: string; status: "draft" | "final" };

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const task = url.searchParams.get("task");
  if (!task) throw new Response("Missing review task", { status: 400 });

  const detail = await queryReviewTaskDetail(task).catch((error) => {
    console.warn("[PostGIS] review task unavailable:", error instanceof Error ? error.message : error);
    return null;
  });
  if (!detail) throw new Response("Review task not found", { status: 404 });

  return { detail };
}

export async function action({ request }: Route.ActionArgs) {
  const formData = await request.formData();
  const parsed = parseAnnotationActionPayload(formData);
  if (!parsed.ok) return Response.json({ ok: false, error: parsed.error }, { status: 400 });

  const label = buildAnnotationLabel(parsed.payload);
  const annotationId = await insertAnnotation({
    defectId: parsed.payload.defectId,
    evidenceId: parsed.payload.evidenceId,
    label,
    annotatorId: "operator-demo",
  });

  if (parsed.payload.intent === "submit") {
    await updateReviewTaskStatus(parsed.payload.reviewTaskId, parsed.payload.defectId, "completed");
    await insertAuditLog({
      actorId: "operator-demo",
      action: "annotation.submit",
      entityType: "review_task",
      entityId: parsed.payload.reviewTaskId,
      payload: { annotation_id: annotationId, defect_id: parsed.payload.defectId },
    });
  }

  return Response.json({ ok: true, annotationId, status: label.status });
}

// ─── canvas constants ─────────────────────────────────────────────────────────
const CANVAS_W = 600;
const CANVAS_H = 400;
const CLOSE_RADIUS = 12; // px — click within this of first vertex to close polygon
const HIT_VERTEX = 8;    // px — vertex grab radius
const HIT_EDGE = 7;      // px — edge insertion radius

// ─── geometry helpers ─────────────────────────────────────────────────────────
type Pt = { x: number; y: number };

function dist(a: Pt, b: Pt) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function distToSegment(p: Pt, a: Pt, b: Pt): { d: number; t: number } {
  const dx = b.x - a.x, dy = b.y - a.y;
  const len2 = dx * dx + dy * dy;
  if (len2 === 0) return { d: dist(p, a), t: 0 };
  const t = Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / len2));
  return { d: Math.hypot(p.x - (a.x + t * dx), p.y - (a.y + t * dy)), t };
}

function ptOnSegment(a: Pt, b: Pt, t: number): Pt {
  return { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t };
}

function isInsidePolygon(pt: Pt, verts: Pt[]): boolean {
  let inside = false;
  const n = verts.length;
  for (let i = 0, j = n - 1; i < n; j = i++) {
    const xi = verts[i].x, yi = verts[i].y, xj = verts[j].x, yj = verts[j].y;
    if ((yi > pt.y) !== (yj > pt.y) && pt.x < ((xj - xi) * (pt.y - yi)) / (yj - yi) + xi)
      inside = !inside;
  }
  return inside;
}

// ─── types ────────────────────────────────────────────────────────────────────
interface PolygonShape {
  id: string;
  vertices: Pt[];
  source: string;
}

// ─── component ────────────────────────────────────────────────────────────────
export default function AnnotationPage() {
  const navigate = useNavigate();
  const { detail } = useLoaderData<typeof loader>();
  const actionData = useActionData() as AnnotationActionData | undefined;
  const navigation = useNavigation();
  const isSubmitting = navigation.state !== "idle";
  const uid = useId();

  const initialVerts = (detail.latestAnnotation?.polygon ?? detail.originalPolygon).map(
    ([x, y]) => ({ x, y }),
  );
  const initialSource = detail.latestAnnotation?.source ?? "manual";
  const initialId = `${uid}-0`;

  const [polygons, setPolygons] = useState<PolygonShape[]>([
    { id: initialId, vertices: initialVerts, source: initialSource },
  ]);
  const [selectedId, setSelectedId] = useState<string>(initialId);
  const [drawingVerts, setDrawingVerts] = useState<Pt[]>([]);
  const [mousePos, setMousePos] = useState<Pt | null>(null);
  const [tool, setTool] = useState<"select" | "polygon">("select");
  const [zoom, setZoom] = useState(100);
  const [panOffset, setPanOffset] = useState<Pt>({ x: 0, y: 0 });
  const [notes, setNotes] = useState(detail.latestAnnotation?.notes ?? "");
  const [autoSegRunning, setAutoSegRunning] = useState(false);
  const [annotationSource, setAnnotationSource] = useState(initialSource);

  // drag state refs (avoid re-render mid-drag)
  const dragVertexRef = useRef<{ pid: string; vi: number } | null>(null);
  const dragPolygonRef = useRef<{ pid: string; start: Pt; origVerts: Pt[] } | null>(null);
  const panRef = useRef<{ mx: number; my: number; ox: number; oy: number } | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  function svgPoint(e: React.MouseEvent): Pt {
    const rect = svgRef.current!.getBoundingClientRect();
    const scale = zoom / 100;
    return {
      x: (e.clientX - rect.left) / scale - panOffset.x,
      y: (e.clientY - rect.top) / scale - panOffset.y,
    };
  }

  // Delete selected polygon on Delete/Backspace key
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== "Delete" && e.key !== "Backspace") return;
      if ((e.target as HTMLElement).tagName === "TEXTAREA") return;
      setPolygons((prev) => {
        if (prev.length <= 1) return prev; // keep at least one
        const next = prev.filter((p) => p.id !== selectedId);
        setSelectedId(next[next.length - 1]?.id ?? "");
        return next;
      });
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selectedId]);

  // ── event handlers ───────────────────────────────────────────────────────────

  function handleSvgMouseDown(e: React.MouseEvent<SVGSVGElement>) {
    if (e.button !== 0) return;
    const pt = svgPoint(e);

    if (tool === "select") {
      // Check vertex hit on selected polygon
      const sel = polygons.find((p) => p.id === selectedId);
      if (sel) {
        const vi = sel.vertices.findIndex((v) => dist(v, pt) <= HIT_VERTEX);
        if (vi !== -1) {
          dragVertexRef.current = { pid: sel.id, vi };
          return;
        }
        // Check polygon body hit
        if (isInsidePolygon(pt, sel.vertices)) {
          dragPolygonRef.current = { pid: sel.id, start: pt, origVerts: [...sel.vertices] };
          return;
        }
      }
      // Check other polygons for selection
      for (const poly of [...polygons].reverse()) {
        if (poly.id === selectedId) continue;
        if (isInsidePolygon(pt, poly.vertices)) {
          setSelectedId(poly.id);
          return;
        }
      }
      // Empty space — start pan
      panRef.current = { mx: e.clientX, my: e.clientY, ox: panOffset.x, oy: panOffset.y };
    }

    if (tool === "polygon") {
      // Check edge insertion on any polygon
      for (const poly of polygons) {
        const verts = poly.vertices;
        for (let i = 0; i < verts.length; i++) {
          const a = verts[i], b = verts[(i + 1) % verts.length];
          const { d, t } = distToSegment(pt, a, b);
          if (d <= HIT_EDGE) {
            const inserted = ptOnSegment(a, b, t);
            setPolygons((prev) =>
              prev.map((p) =>
                p.id !== poly.id
                  ? p
                  : { ...p, vertices: [...verts.slice(0, i + 1), inserted, ...verts.slice(i + 1)] },
              ),
            );
            return;
          }
        }
      }

      // Close polygon if clicking near first vertex
      if (drawingVerts.length >= 3 && dist(pt, drawingVerts[0]) <= CLOSE_RADIUS) {
        const newId = `${uid}-${Date.now()}`;
        setPolygons((prev) => [
          ...prev,
          { id: newId, vertices: drawingVerts, source: "manual" },
        ]);
        setSelectedId(newId);
        setAnnotationSource("manual");
        setDrawingVerts([]);
        return;
      }

      // Add vertex to in-progress polygon
      setDrawingVerts((prev) => [...prev, pt]);
    }
  }

  function handleSvgMouseMove(e: React.MouseEvent<SVGSVGElement>) {
    const pt = svgPoint(e);
    setMousePos(pt);

    if (dragVertexRef.current) {
      const { pid, vi } = dragVertexRef.current;
      setPolygons((prev) =>
        prev.map((p) =>
          p.id !== pid
            ? p
            : { ...p, vertices: p.vertices.map((v, idx) => (idx === vi ? pt : v)) },
        ),
      );
      setAnnotationSource((s) => (s === "sam3_assist" ? "manual_refined_sam3" : s));
      return;
    }

    if (dragPolygonRef.current) {
      const { pid, start, origVerts } = dragPolygonRef.current;
      const dx = pt.x - start.x, dy = pt.y - start.y;
      setPolygons((prev) =>
        prev.map((p) =>
          p.id !== pid
            ? p
            : { ...p, vertices: origVerts.map((v) => ({ x: v.x + dx, y: v.y + dy })) },
        ),
      );
      return;
    }

    if (panRef.current) {
      const { mx, my, ox, oy } = panRef.current;
      const scale = zoom / 100;
      setPanOffset({ x: ox + (e.clientX - mx) / scale, y: oy + (e.clientY - my) / scale });
    }
  }

  function handleSvgMouseUp() {
    dragVertexRef.current = null;
    dragPolygonRef.current = null;
    panRef.current = null;
  }

  function handleSvgMouseLeave() {
    handleSvgMouseUp();
    setMousePos(null);
  }

  // ── auto-seg ─────────────────────────────────────────────────────────────────
  const runAutoSeg = useCallback(async () => {
    setAutoSegRunning(true);
    try {
      const sel = polygons.find((p) => p.id === selectedId);
      const currentPoly = sel?.vertices.map(({ x, y }) => [x, y]) ?? [];
      const response = await fetch("/api/annotation/assist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          review_task_id: detail.reviewTaskId,
          evidence_id: detail.evidenceId,
          image_object_key: detail.rawImageObjectKey,
          current_polygon: currentPoly,
          prompt: "pothole",
        }),
      });
      if (!response.ok) throw new Error("SAM3 assist failed");
      const result = (await response.json()) as { polygon: Polygon };
      const newVerts = result.polygon.map(([x, y]) => ({ x, y }));
      const newId = `${uid}-sam-${Date.now()}`;
      setPolygons((prev) => [...prev, { id: newId, vertices: newVerts, source: "sam3_assist" }]);
      setSelectedId(newId);
      setAnnotationSource("sam3_assist");
    } catch (error) {
      console.warn("[SAM3 assist]", error instanceof Error ? error.message : error);
    } finally {
      setAutoSegRunning(false);
    }
  }, [polygons, selectedId, detail, uid]);

  // ── derived save payload ─────────────────────────────────────────────────────
  const activePoly =
    polygons.find((p) => p.id === selectedId) ?? polygons[polygons.length - 1];
  const polygonJson = JSON.stringify(activePoly?.vertices.map(({ x, y }) => [x, y]) ?? []);
  const saveSource = activePoly?.source ?? annotationSource;
  const confidenceValue = detail.confidencePercent;

  // ── drawing ghost ────────────────────────────────────────────────────────────
  const ghostLine =
    tool === "polygon" && mousePos && drawingVerts.length > 0
      ? { x1: drawingVerts[drawingVerts.length - 1].x, y1: drawingVerts[drawingVerts.length - 1].y, x2: mousePos.x, y2: mousePos.y }
      : null;
  const nearClose =
    tool === "polygon" && mousePos && drawingVerts.length >= 3 && dist(mousePos, drawingVerts[0]) <= CLOSE_RADIUS;

  const svgCursor =
    tool === "polygon"
      ? "crosshair"
      : dragPolygonRef.current
        ? "grabbing"
        : "default";

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
      {/* Top bar */}
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
        <Typography variant="caption" color="#8895A7">{detail.defectId}</Typography>
        <Box sx={{ flex: 1 }} />
        <IconButton size="small" sx={{ color: "#8895A7" }} onClick={() => setZoom((z) => Math.min(z + 10, 200))}>
          <ZoomIn size={14} />
        </IconButton>
        <Typography variant="caption" color="#8895A7">{zoom}%</Typography>
        <IconButton size="small" sx={{ color: "#8895A7" }} onClick={() => setZoom((z) => Math.max(z - 10, 25))}>
          <ZoomOut size={14} />
        </IconButton>
      </Box>

      <Box sx={{ flex: 1, display: "flex", overflow: "hidden" }}>
        {/* Left info panel */}
        <Box
          sx={{
            width: 220,
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
            <Typography variant="caption" color="#8895A7" display="block" mt={0.5}>ID: {detail.defectId}</Typography>
            <Typography variant="caption" color="#8895A7" display="block">Severity: {detail.severity.toUpperCase()}</Typography>
            <Typography variant="caption" color="#8895A7" display="block">Task: {detail.taskStatus.replace("_", " ")}</Typography>
          </Box>

          <Box>
            <Typography variant="caption" color="#5A6B7F">CONFIDENCE</Typography>
            <Box sx={{ display: "flex", justifyContent: "space-between", mt: 0.5 }}>
              <Typography variant="caption" color="#8895A7">{confidenceValue}%</Typography>
            </Box>
            <LinearProgress
              variant="determinate"
              value={confidenceValue}
              sx={{
                height: 6,
                borderRadius: 3,
                bgcolor: "#2A3444",
                "& .MuiLinearProgress-bar": { bgcolor: "#D97706" },
              }}
            />
          </Box>

          <Box>
            <Typography variant="caption" color="#5A6B7F">POLYGONS ({polygons.length})</Typography>
            {polygons.map((p, i) => (
              <Box
                key={p.id}
                onClick={() => setSelectedId(p.id)}
                sx={{
                  mt: 0.5,
                  px: 1,
                  py: 0.5,
                  borderRadius: 1,
                  cursor: "pointer",
                  border: `1px solid ${p.id === selectedId ? "#1488DB" : "#2A3444"}`,
                  bgcolor: p.id === selectedId ? "#1488DB22" : "transparent",
                }}
              >
                <Typography variant="caption" color={p.id === selectedId ? "#1488DB" : "#8895A7"}>
                  #{i + 1} · {p.vertices.length}pt · {p.source === "sam3_assist" ? "SAM3" : "manual"}
                </Typography>
              </Box>
            ))}
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
              placeholder="Add notes..."
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

          {actionData && !actionData.ok && (
            <Typography variant="caption" color="error" display="block">{actionData.error}</Typography>
          )}
          {actionData && actionData.ok && (
            <Typography variant="caption" color="#86EFAC" display="block">
              Annotation {actionData.status === "final" ? "submitted" : "saved"}.
            </Typography>
          )}
        </Box>

        {/* Canvas */}
        <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", overflow: "hidden" }}>
          <svg
            ref={svgRef}
            width={CANVAS_W * (zoom / 100)}
            height={CANVAS_H * (zoom / 100)}
            style={{ background: "#2A3444", borderRadius: 8, cursor: svgCursor, userSelect: "none" }}
            onMouseDown={handleSvgMouseDown}
            onMouseMove={handleSvgMouseMove}
            onMouseUp={handleSvgMouseUp}
            onMouseLeave={handleSvgMouseLeave}
          >
            <g transform={`scale(${zoom / 100})`}>
              <g transform={`translate(${panOffset.x},${panOffset.y})`}>
                {/* Background image or placeholder road */}
                {detail.imageProxyUrl ? (
                  <image href={detail.imageProxyUrl} x={0} y={0} width={CANVAS_W} height={CANVAS_H} preserveAspectRatio="xMidYMid slice" />
                ) : (
                  <>
                    <rect x={0} y={0} width={CANVAS_W} height={CANVAS_H} fill="#2A3444" />
                    <line x1={0} y1={200} x2={CANVAS_W} y2={200} stroke="#3A4A5A" strokeWidth={60} />
                    {[50, 150, 250, 350, 450, 550].map((x) => (
                      <rect key={x} x={x} y={194} width={40} height={12} fill="#4A5A6A" rx={2} />
                    ))}
                  </>
                )}

                {/* Completed polygons */}
                {polygons.map((poly) => {
                  const isSelected = poly.id === selectedId;
                  const pts = poly.vertices.map(({ x, y }) => `${x},${y}`).join(" ");
                  return (
                    <g key={poly.id}>
                      <polygon
                        points={pts}
                        fill={isSelected ? "rgba(20,136,219,0.25)" : "rgba(20,136,219,0.10)"}
                        stroke={isSelected ? "#1488DB" : "#5A8DB5"}
                        strokeWidth={isSelected ? 2 : 1.5}
                      />
                      {isSelected &&
                        poly.vertices.map((v, i) => (
                          <circle
                            key={i}
                            cx={v.x}
                            cy={v.y}
                            r={5}
                            fill="#1488DB"
                            stroke="#fff"
                            strokeWidth={1.5}
                            style={{ cursor: "grab" }}
                          />
                        ))}
                    </g>
                  );
                })}

                {/* In-progress drawing polygon */}
                {drawingVerts.length > 0 && (
                  <g>
                    {drawingVerts.length > 1 && (
                      <polyline
                        points={drawingVerts.map(({ x, y }) => `${x},${y}`).join(" ")}
                        fill="none"
                        stroke="#F59E0B"
                        strokeWidth={1.5}
                        strokeDasharray="4 2"
                      />
                    )}
                    {/* Ghost line to mouse */}
                    {ghostLine && (
                      <line
                        x1={ghostLine.x1} y1={ghostLine.y1}
                        x2={ghostLine.x2} y2={ghostLine.y2}
                        stroke="#F59E0B"
                        strokeWidth={1.5}
                        strokeDasharray="4 2"
                        opacity={0.7}
                      />
                    )}
                    {/* Existing vertices */}
                    {drawingVerts.map((v, i) => (
                      <circle
                        key={i}
                        cx={v.x}
                        cy={v.y}
                        r={i === 0 ? (nearClose ? 8 : 5) : 4}
                        fill={i === 0 ? (nearClose ? "#10B981" : "#F59E0B") : "#F59E0B"}
                        stroke="#fff"
                        strokeWidth={1.5}
                      />
                    ))}
                    {/* Ghost dot at mouse */}
                    {mousePos && !nearClose && (
                      <circle cx={mousePos.x} cy={mousePos.y} r={4} fill="#F59E0B" opacity={0.6} />
                    )}
                  </g>
                )}

                {/* Ghost dot in polygon mode with no drawing started */}
                {tool === "polygon" && mousePos && drawingVerts.length === 0 && (
                  <circle cx={mousePos.x} cy={mousePos.y} r={4} fill="#F59E0B" opacity={0.5} />
                )}
              </g>
            </g>
          </svg>
        </Box>

        {/* Right toolbar */}
        <Form
          method="post"
          style={{
            width: 56,
            flexShrink: 0,
            backgroundColor: "#0F1824",
            borderLeft: "1px solid #2A3444",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            paddingTop: 8,
            paddingBottom: 8,
            gap: 4,
          }}
        >
          <input type="hidden" name="review_task_id" value={detail.reviewTaskId} />
          <input type="hidden" name="defect_id" value={detail.defectId} />
          <input type="hidden" name="evidence_id" value={detail.evidenceId ?? ""} />
          <input type="hidden" name="polygon" value={polygonJson} />
          <input type="hidden" name="source" value={saveSource} />
          <input type="hidden" name="notes" value={notes} />

          <ToggleButtonGroup
            value={tool}
            exclusive
            onChange={(_, v) => {
              if (!v) return;
              setTool(v as "select" | "polygon");
              if (v === "select") setDrawingVerts([]);
            }}
            orientation="vertical"
            size="small"
            sx={{
              "& .MuiToggleButton-root": {
                color: "#5A6B7F",
                border: "none",
                "&.Mui-selected": { color: "#1488DB", bgcolor: "#1488DB22" },
              },
            }}
          >
            <Tooltip title="Select / Pan" placement="left">
              <ToggleButton value="select"><MousePointer size={16} /></ToggleButton>
            </Tooltip>
            <Tooltip title="Draw Polygon" placement="left">
              <ToggleButton value="polygon"><Pentagon size={16} /></ToggleButton>
            </Tooltip>
          </ToggleButtonGroup>

          <Box sx={{ height: 1, bgcolor: "#2A3444", width: "80%", my: 0.5 }} />

          <Tooltip title={autoSegRunning ? "Running…" : "SAM3 Auto-Segment"} placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button
                type="button"
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

          <Box sx={{ flex: 1 }} />

          <Tooltip title="Save as draft" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button
                name="intent"
                value="save"
                type="submit"
                disabled={isSubmitting}
                size="small"
                variant="outlined"
                fullWidth
                sx={{ fontSize: 8, px: 0.25, minWidth: 0, color: "#8895A7", borderColor: "#2A3444", lineHeight: 1.2 }}
              >
                Save Draft
              </Button>
            </Box>
          </Tooltip>

          <Tooltip title="Submit as final annotation" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button
                name="intent"
                value="submit"
                type="submit"
                disabled={isSubmitting}
                size="small"
                variant="contained"
                color="success"
                fullWidth
                sx={{ fontSize: 9, px: 0.5, minWidth: 0 }}
              >
                Submit
              </Button>
            </Box>
          </Tooltip>
        </Form>
      </Box>
    </Box>
  );
}
