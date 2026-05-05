from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from data_models import BoundingBox, SegmentedPothole


@dataclass
class _TrackedDetection:
    """Recent detection used to suppress duplicate uploads."""

    track_id: int
    bbox: BoundingBox
    confidence: float
    last_seen_frame: int


class DetectionDeduplicator:
    """Suppress repeated detections by matching recent boxes with IoU."""

    def __init__(self, iou_threshold: float = 0.5, max_age_frames: int = 15) -> None:
        if iou_threshold < 0 or iou_threshold > 1:
            raise ValueError("iou_threshold must be between 0 and 1")
        if max_age_frames <= 0:
            raise ValueError("max_age_frames must be greater than 0")

        self.iou_threshold = iou_threshold
        self.max_age_frames = max_age_frames
        self._tracks: list[_TrackedDetection] = []
        self._next_track_id = 1

    def deduplicate(
        self,
        frame_index: int,
        detections: Iterable[SegmentedPothole],
    ) -> list[SegmentedPothole]:
        """Return detections that do not match a recent tracked box."""
        self._prune_expired(frame_index)
        accepted: list[SegmentedPothole] = []

        for detection in detections:
            match = self._find_match(detection.bbox)
            if match is None:
                self._tracks.append(
                    _TrackedDetection(
                        track_id=self._next_track_id,
                        bbox=normalize_bbox(detection.bbox),
                        confidence=detection.confidence,
                        last_seen_frame=frame_index,
                    )
                )
                self._next_track_id += 1
                accepted.append(detection)
                continue

            match.bbox = normalize_bbox(detection.bbox)
            match.confidence = detection.confidence
            match.last_seen_frame = frame_index

        return accepted

    @property
    def track_count(self) -> int:
        """Return the number of active recent detections."""
        return len(self._tracks)

    def _find_match(self, bbox: BoundingBox) -> _TrackedDetection | None:
        best_track: _TrackedDetection | None = None
        best_iou = self.iou_threshold

        for track in self._tracks:
            iou = box_iou(track.bbox, bbox)
            if iou >= best_iou:
                best_track = track
                best_iou = iou

        return best_track

    def _prune_expired(self, frame_index: int) -> None:
        self._tracks = [
            track
            for track in self._tracks
            if frame_index - track.last_seen_frame <= self.max_age_frames
        ]


def normalize_bbox(bbox: BoundingBox) -> BoundingBox:
    """Return a box ordered as `(x1, y1, x2, y2)`."""
    x1, y1, x2, y2 = bbox
    return min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)


def box_iou(box_a: BoundingBox, box_b: BoundingBox) -> float:
    """Compute intersection-over-union for two `(x1, y1, x2, y2)` boxes."""
    ax1, ay1, ax2, ay2 = normalize_bbox(box_a)
    bx1, by1, bx2, by2 = normalize_bbox(box_b)

    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)

    inter_width = max(0.0, inter_x2 - inter_x1)
    inter_height = max(0.0, inter_y2 - inter_y1)
    intersection = inter_width * inter_height

    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - intersection
    if union <= 0:
        return 0.0
    return intersection / union
