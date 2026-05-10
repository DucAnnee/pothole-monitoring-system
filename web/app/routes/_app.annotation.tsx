import type { Route } from "./+types/_app.annotation";
import Alert from "@mui/material/Alert";
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
  if (!task) {
    throw new Response("Missing review task", { status: 400 });
  }

  const detail = await queryReviewTaskDetail(task).catch((error) => {
    console.warn(
      "[PostGIS] review task unavailable:",
      error instanceof Error ? error.message : error,
    );
    return null;
  });
  if (!detail) {
    throw new Response("Review task not found", { status: 404 });
  }

  return { detail };
}

export async function action({ request }: Route.ActionArgs) {
  const formData = await request.formData();
  const parsed = parseAnnotationActionPayload(formData);
  if (!parsed.ok) {
    return Response.json({ ok: false, error: parsed.error }, { status: 400 });
  }

  const label = buildAnnotationLabel(parsed.payload);
  const annotationId = await insertAnnotation({
    defectId: parsed.payload.defectId,
    evidenceId: parsed.payload.evidenceId,
    label,
    annotatorId: "operator-demo",
  });

  if (parsed.payload.intent === "submit") {
    await updateReviewTaskStatus(
      parsed.payload.reviewTaskId,
      parsed.payload.defectId,
      "completed",
    );
    await insertAuditLog({
      actorId: "operator-demo",
      action: "annotation.submit",
      entityType: "review_task",
      entityId: parsed.payload.reviewTaskId,
      payload: {
        annotation_id: annotationId,
        defect_id: parsed.payload.defectId,
      },
    });
  }

  return Response.json({ ok: true, annotationId, status: label.status });
}

export default function AnnotationPage() {
  const navigate = useNavigate();
  const { detail } = useLoaderData<typeof loader>();
  const actionData = useActionData() as AnnotationActionData | undefined;
  const navigation = useNavigation();
  const initialPolygon = detail.latestAnnotation?.polygon ?? detail.originalPolygon;
  const [tool, setTool] = useState("select");
  const [opacity, setOpacity] = useState(80);
  const [zoom, setZoom] = useState(100);
  const [vertices, setVertices] = useState(
    initialPolygon.map(([x, y]) => ({ x, y })),
  );
  const [dragging, setDragging] = useState<number | null>(null);
  const [notes, setNotes] = useState(detail.latestAnnotation?.notes ?? "");
  const [annotationSource, setAnnotationSource] = useState(
    detail.latestAnnotation?.source ?? "manual",
  );
  const [autoSegRunning, setAutoSegRunning] = useState(false);
  const isSubmitting = navigation.state !== "idle";

  function handleVertexMouseDown(i: number) {
    setDragging(i);
  }

  function handleSvgMouseMove(e: React.MouseEvent<SVGSVGElement>) {
    if (dragging === null) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    setVertices((prev) => prev.map((v, idx) => (idx === dragging ? { x, y } : v)));
    setAnnotationSource((prev) =>
      prev === "sam3_assist" ? "manual_refined_sam3" : prev,
    );
  }

  function handleSvgMouseUp() {
    setDragging(null);
  }

  function resetPolygon() {
    setVertices(detail.originalPolygon.map(([x, y]) => ({ x, y })));
    setAnnotationSource("manual");
  }

  async function runAutoSeg() {
    setAutoSegRunning(true);
    try {
      const response = await fetch("/api/annotation/assist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          review_task_id: detail.reviewTaskId,
          evidence_id: detail.evidenceId,
          image_object_key: detail.rawImageObjectKey,
          current_polygon: vertices.map((v) => [v.x, v.y]),
          prompt: "pothole",
        }),
      });
      if (!response.ok) throw new Error("SAM3 assist failed");
      const result = (await response.json()) as { polygon: Polygon };
      setVertices(result.polygon.map(([x, y]) => ({ x, y })));
      setAnnotationSource("sam3_assist");
    } catch (error) {
      console.warn("[SAM3 assist]", error instanceof Error ? error.message : error);
    } finally {
      setAutoSegRunning(false);
    }
  }

  const polygonPoints = vertices.map((v) => `${v.x},${v.y}`).join(" ");
  const polygonJson = JSON.stringify(vertices.map((v) => [v.x, v.y]));
  const confidenceValue = detail.confidencePercent;

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
        <Typography variant="caption" color="#8895A7">
          {detail.defectId}
        </Typography>
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

      <Box sx={{ flex: 1, display: "flex", overflow: "hidden" }}>
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
            <Typography variant="caption" color="#8895A7" display="block" mt={0.5}>
              ID: {detail.defectId}
            </Typography>
            <Typography variant="caption" color="#8895A7" display="block">
              Severity: {detail.severity.toUpperCase()}
            </Typography>
            <Typography variant="caption" color="#8895A7" display="block">
              Task: {detail.taskStatus.replace("_", " ")}
            </Typography>
            <Typography variant="caption" color="#8895A7" display="block">
              Evidence: {detail.evidenceId ?? "none"}
            </Typography>
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
            <Typography variant="caption" color="error" display="block">
              {actionData.error}
            </Typography>
          )}
          {actionData && actionData.ok && (
            <Typography variant="caption" color="#86EFAC" display="block">
              Annotation {actionData.status === "final" ? "submitted" : "saved"}.
            </Typography>
          )}
        </Box>

        <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", overflow: "hidden" }}>
          <svg
            width="600"
            height="400"
            style={{
              background: "#2A3444",
              borderRadius: 8,
              cursor: dragging !== null ? "grabbing" : "default",
              transform: `scale(${zoom / 100})`,
              transformOrigin: "center",
            }}
            onMouseMove={handleSvgMouseMove}
            onMouseUp={handleSvgMouseUp}
            onMouseLeave={handleSvgMouseUp}
          >
            {detail.imageProxyUrl ? (
              <image
                href={detail.imageProxyUrl}
                x={0}
                y={0}
                width={600}
                height={400}
                preserveAspectRatio="xMidYMid slice"
              />
            ) : (
              <>
                <rect x={0} y={0} width={600} height={400} fill="#2A3444" />
                <line x1={0} y1={200} x2={600} y2={200} stroke="#3A4A5A" strokeWidth={60} />
                {[50, 150, 250, 350, 450, 550].map((x) => (
                  <rect key={x} x={x} y={194} width={40} height={12} fill="#4A5A6A" rx={2} />
                ))}
              </>
            )}

            <polygon
              points={polygonPoints}
              fill={`rgba(20,136,219,${opacity / 300})`}
              stroke="#1488DB"
              strokeWidth={2}
            />

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
          <input type="hidden" name="source" value={annotationSource} />
          <input type="hidden" name="notes" value={notes} />

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

          <Tooltip title="Undo" placement="left">
            <span>
              <IconButton type="button" size="small" sx={{ color: "#5A6B7F" }} disabled>
                <Undo size={14} />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Redo" placement="left">
            <span>
              <IconButton type="button" size="small" sx={{ color: "#5A6B7F" }} disabled>
                <Redo size={14} />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Reset" placement="left">
            <IconButton type="button" size="small" sx={{ color: "#5A6B7F" }} onClick={resetPolygon}>
              <RotateCcw size={14} />
            </IconButton>
          </Tooltip>

          <Box sx={{ flex: 1 }} />

          <Tooltip title={autoSegRunning ? "Running..." : "Assist Segment (SAM3)"} placement="left">
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
                {autoSegRunning ? "..." : "SAM"}
              </Button>
            </Box>
          </Tooltip>

          <Tooltip title="Save" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button name="intent" value="save" type="submit" disabled={isSubmitting} size="small" variant="outlined" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0, color: "#8895A7", borderColor: "#2A3444" }}>
                Save
              </Button>
            </Box>
          </Tooltip>

          <Tooltip title="Submit" placement="left">
            <Box sx={{ px: 0.5, width: "100%" }}>
              <Button name="intent" value="submit" type="submit" disabled={isSubmitting} size="small" variant="contained" color="success" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0 }}>
                Submit
              </Button>
            </Box>
          </Tooltip>
        </Form>
      </Box>
    </Box>
  );
}
