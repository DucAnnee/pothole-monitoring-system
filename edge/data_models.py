"""
Shared data models for the edge device pipeline.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Literal
import numpy as np

ModelType = Literal["yolo", "rfdetr"]


@dataclass(frozen=True)
class RuntimeModel:
    """Resolved model settings used by the live inference pipeline.

    This is the shared contract between config fallback, registry resolution,
    startup model updates, and segmenter initialization.

    Attributes:
        model_id: Stable id from the registry, or a config-derived fallback id.
        model_type: Segmenter implementation to instantiate.
        model_path: Local artifact path passed to the segmenter.
        confidence_threshold: Detection threshold paired with this model.
        source: Human-readable source for logs and startup diagnostics.
    """

    model_id: str
    model_type: ModelType
    model_path: str
    confidence_threshold: float
    source: str


@dataclass
class DetectionMask:
    """Single pothole mask detection."""

    conf: float
    coordinates: List[List[float]]  # [[x1, y1], [x2, y2], ...]


@dataclass
class DetectionData:
    """Data passed from inference to processing."""

    frame_id: str
    timestamp: datetime
    frame: np.ndarray  # RGB image
    masks: List[DetectionMask]

    def to_dict(self):
        """Convert to dictionary (excluding frame for serialization)."""
        return {
            "frame_id": self.frame_id,
            "timestamp": self.timestamp.isoformat(),
            "masks": [
                {"conf": m.conf, "coordinates": m.coordinates} for m in self.masks
            ],
        }


@dataclass
class BundledData:
    """Bundled data ready for upload."""

    event_id: str
    frame_id: str
    timestamp: datetime
    frame: np.ndarray  # original RGB image
    conf: float
    coordinates: List[List[float]]
