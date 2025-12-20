"""
Shared data models for the edge device pipeline.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional
import numpy as np


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
class EnrichedData:
    """Enriched data ready for upload."""

    event_id: str
    frame_id: str
    timestamp: datetime
    frame: np.ndarray  # Original RGB image
    bev_frame: Optional[np.ndarray]  # Bird's eye view image
    conf: float
    coordinates: List[List[float]]
    bev_mask: Optional[List[List[float]]]
    area_cm2: float
