# Edge Pipeline

The main orchestrator for the edge device pipeline that coordinates pothole detection, segmentation, and cloud upload with offline fallback capabilities.

## Overview

The edge pipeline implements a multi-threaded architecture that:
1. Processes video frames or camera feed to detect potholes using YOLO or RF-DETR segmentation models
2. Queues detections in memory for asynchronous processing
3. Bundles detection data with metadata (GPS, timestamps, confidence scores)
4. Uploads to cloud services (Kafka + MinIO S3)
5. Handles offline scenarios with persistent local storage
6. Provides real-time visualization with OpenCV

## Architecture

```
       ┌─────────────────────┐
       │  Video Source       │
       │  (File or Camera)   │
       └──────────┬──────────┘
                  │
                  ▼
┌──────────────────────────────────┐
│  Inference Worker                │
│  - Frame sampling                │
│  - Trapezoid ROI masking         │
│  - Model inference (YOLO/RFDETR) │
│  - Pothole filtering             │
│  - Detection queueing            │
│  - Real-time visualization       │
└─────────────────┬────────────────┘
                  │
                  │ (In-memory Queue, max 100)
                  ▼
┌──────────────────────────────────┐
│  Uploading Worker                │
│  - Detection bundling            │
│  - GPS coordinate generation     │
│  - Cloud upload (Kafka + MinIO)  │
│  - Offline storage fallback      │
│  - Connection recovery           │
│  - Local storage processing      │
└─────────────────┬────────────────┘
                  │                   ┌──────────────────┐
                  ├─ (Online) ──────► │ MinIO + Kafka    │
                  │                   │ (Cloud Storage)  │
                  │                   └──────────────────┘
                  │                   
                  │                   ┌──────────────────┐
                  └─ (Offline) ─────► │ ./local_storage/ │
                                      │ (Disk Storage)   │
                                      └──────────────────┘
```

## Key Components

### EdgePipeline Class 

Main orchestrator that manages the complete pipeline lifecycle. Located in [`main.py`](./main.py)

**Initialization:**
```python
pipeline = EdgePipeline(config_path="config.yaml", video_path="path/to/video.mp4")
```

**Parameters:**
- `config_path` (str): Path to YAML configuration file (default: `"config.yaml"`)
- `video_path` (str, optional): Path to video file. If not provided, uses camera device 0

**Attributes:**
- `vehicle_id` (str): Unique vehicle identifier (auto-generated with UUID prefix)
- `detection_queue` (Queue): Thread-safe in-memory queue (maxsize=100)
- `segmenter` (PotholeSegmenter): Segmentation model instance (YOLO or RF-DETR)
- `uploader` (Uploader): Upload and storage handler
- `running` (bool): Pipeline state flag for coordinated shutdown

### Segmentation Module

Located in [`segmentation/pothole_segmenter.py`](./segmentation/pothole_segmenter.py), provides abstract base class with two implementations:

**Factory Pattern:**
```python
segmenter = PotholeSegmenter.create(
    model_type="yolo",  # or "rfdetr"
    model_path="models/yolo11s.pt",
    trapezoid_coords=np.array([[...], [...], [...], [...]]),
    confidence_threshold=0.25
)
```

**YOLOSegmenter:**
- Uses Ultralytics YOLO segmentation models
- Returns masks as contour coordinates (N, 2)
- Dependency: `ultralytics`

**RFDETRSegmenter:**
- Uses RF-DETR segmentation model
- Converts binary masks to contours
- Dependency: `rfdetr`

**Key Methods:**
- `segment(frame_rgb)`: Returns list of (mask_coords, confidence) tuples
- `create_masked_image(frame_rgb)`: Applies trapezoid ROI masking
- `pothole_in_trapezoid(mask, frame_shape)`: Filters detections outside ROI

### Uploader Module

Located in [`uploader.py`](./uploader.py), handles all cloud interactions and offline storage.

**Features:**
- Kafka producer with Avro serialization
- MinIO S3 image storage
- Persistent local storage for offline mode
- Automatic connection recovery
- GPS coordinate simulation

**Key Methods:**
- `process_detection(detection)`: Bundles masks into individual events
- `upload_to_cloud(bundled)`: Uploads to Kafka + MinIO
- `store_to_disk(bundled)`: Persists to local storage
- `process_local_storage()`: Uploads stored data when back online

### Worker Threads

#### 1. Inference Worker

**Thread Target:** `inference_worker()`

**Flow:**
1. Opens video source (file or camera device 0)
2. Samples frames based on `frame_interval` config
3. Converts frame to RGB format
4. Runs segmentation model
5. Filters potholes within trapezoid ROI
6. Bundles valid detections as `DetectionData`
7. Queues detections (with timeout protection)
8. Displays visualization (if enabled)

**Visualization:**
- **Green polyline**: Trapezoid detection region
- **Blue filled polygons**: Detected potholes
- **Window controls**: Press `Q` to quit

**Error Handling:**
- Video open failure → Stops pipeline
- Frame read failure → Graceful shutdown
- Queue full → Drops frame with warning

#### 2. Uploading Worker

**Thread Target:** `uploading_worker()`

**Flow:**
1. Processes local storage on startup (if online)
2. Dequeues detections from memory queue
3. Bundles each mask into separate `BundledData`
4. Attempts cloud upload (Kafka + MinIO)
5. Falls back to local storage on failure
6. Periodically checks connection status (every 10 stored items)
7. Processes stored data when reconnected

**Offline Mode:**
- Automatically triggered on upload failure
- Stores images to `local/images/`
- Stores metadata to `local/metadata/`
- Periodic connection health checks
- Automatic recovery and backlog processing

## Data Models

Located in [`data_models.py`](./data_models.py):

**DetectionMask:**
```python
@dataclass
class DetectionMask:
    conf: float
    coordinates: List[List[float]]  # [[x1, y1], [x2, y2], ...]
```

**DetectionData:**
```python
@dataclass
class DetectionData:
    frame_id: str
    timestamp: datetime
    frame: np.ndarray  # RGB image
    masks: List[DetectionMask]
```

**BundledData:**
```python
@dataclass
class BundledData:
    event_id: str
    frame_id: str
    timestamp: datetime
    frame: np.ndarray
    conf: float
    coordinates: List[List[float]]
```

## Configuration

The pipeline reads settings from [`config.yaml`](./config.yaml):

```yaml
# Model selection
model_type: "yolo"  # or "rfdetr"

# Model configurations
models:
  yolo:
    weights_path: "models/yolo11s.pt"
    confidence_threshold: 0.1
  rfdetr:
    weights_path: "models/rfdetr.pth"
    confidence_threshold: 0.25

# Processing parameters
processing:
  frame_interval: 3  # Process every Nth frame

# Monitoring
enable_monitoring: true

# Detection region (normalized coordinates 0-1)
detection_region:
  trapezoid_coords:
    - [0.4034, 0.5731]  # Top-Left
    - [0.5081, 0.5796]  # Top-Right
    - [0.6456, 0.7972]  # Bottom-Right
    - [0.3268, 0.7843]  # Bottom-Left

# Kafka configuration
kafka:
  topic: "pothole.raw.events.v1"
  bootstrap_servers: "localhost:19092,localhost:29092,localhost:39092"
  schema_registry_url: "http://localhost:8082"

# MinIO configuration
minio:
  endpoint: "localhost:9000"
  access_key: "${MINIO_ACCESS_KEY:minioadmin}"
  secret_key: "${MINIO_SECRET_KEY:minioadmin}"
  bucket: "warehouse"
  prefix: "raw_images"
  secure: false

# GPS simulation (HCM City bounds)
gps:
  lat_min: 10.7
  lat_max: 10.9
  lon_min: 106.6
  lon_max: 106.8
```

## Usage

### Command Line

```bash
# Use default config and test video
python main.py

# Custom config
python main.py --config custom_config.yaml

# Different video
python main.py --video path/to/video.mp4

# Both custom
python main.py --config custom.yaml --video test.mp4
```

### Programmatic

```python
from main import EdgePipeline

# Initialize
pipeline = EdgePipeline(
    config_path="config.yaml",
    video_path="assets/test.mp4"
)

# Start processing
pipeline.start()

# Stop gracefully (or Ctrl+C)
pipeline.stop()
```

## Pipeline Lifecycle

### Startup Sequence

1. Load and validate configuration (`config_loader.py`)
2. Generate unique vehicle ID
3. Initialize detection queue (maxsize=100)
4. Initialize segmentation model (YOLO or RF-DETR)
5. Initialize uploader (connect to Kafka + MinIO)
6. Check online/offline status
7. Start inference worker thread (daemon)
8. Start uploading worker thread (daemon)
9. Enter monitoring loop with 30s stats reporting

### Shutdown Sequence

**Triggers:**
- Press `Ctrl+C` (SIGINT)
- Press `Q` in visualization window
- Send SIGTERM signal
- Call `pipeline.stop()` method

**Cleanup Steps:**
1. Set `running = False` flag
2. Join worker threads (5s timeout each)
3. Flush Kafka producer buffer
4. Print final statistics
5. Clean up model resources (`segmenter.cleanup()`)

## Statistics & Monitoring

The pipeline prints statistics every 30 seconds during operation:

```
======================================================================
PROCESSING & UPLOAD STATISTICS
======================================================================
Processed:  145
Uploaded:   142
Stored:     3
Failed:     0
======================================================================
Queue depth: 2
```

**Metrics:**
- **Processed**: Total masks processed from detections
- **Uploaded**: Successfully uploaded to cloud (Kafka + MinIO)
- **Stored**: Persisted to local storage (offline mode)
- **Failed**: Storage/upload failures
- **Queue depth**: Current in-memory queue size

## Error Handling

### Inference Errors
- **Video not found**: Stops pipeline immediately
- **Camera unavailable**: Stops pipeline with error message
- **Frame read failure**: Graceful shutdown after logging
- **Segmentation error**: Logs error, continues processing

### Upload Errors
- **Kafka connection lost**: Switches to offline mode, stores locally
- **MinIO upload failure**: Stores locally, logs error
- **Schema serialization error**: Logs error, increments failed counter

### Connection Recovery
- **Periodic checks**: Every 10 stored items when offline
- **Automatic reconnection**: Re-initializes Kafka + MinIO clients
- **Backlog processing**: Uploads all stored data when back online
- **Seamless transition**: No data loss between online/offline modes

## Queue Management

**Configuration:**
- Maximum size: 100 items
- Memory footprint: ~200MB (assuming 1080p RGB frames)
- Timeout: 1 second for put/get operations

**Behaviors:**
- **Queue full**: Drops new detections with warning message
- **Queue empty**: Upload worker blocks with 1s timeout
- **Graceful handling**: No crashes on queue operations

## Thread Safety

- **Queue**: Thread-safe `queue.Queue` for detection passing
- **Flags**: Atomic `running` flag for coordinated shutdown
- **Resource isolation**: Each thread manages its own resources
- **No shared state**: Workers communicate exclusively via queue
- **Signal handlers**: Graceful shutdown on SIGINT/SIGTERM

## Dependencies
**Installation with [`requirements.txt`](./requirements.txt):** 
```bash
pip install -r requirements.txt
```

**Core Libraries:**
- `numpy`: Array operations and coordinate transformations
- `opencv-python (cv2)`: Video I/O and image processing
- `pyyaml`: Configuration file parsing
- `confluent-kafka`: Kafka producer with Avro support
- `minio`: S3-compatible object storage client

**Segmentation Models:**
- `ultralytics`: YOLO segmentation (YOLOSegmenter)
- `rfdetr`: RF-DETR segmentation (RFDETRSegmenter)

**Project Modules:**
- `config_loader`: Configuration management with env var substitution
- `segmentation`: Model abstraction and inference
- `uploader`: Cloud upload and offline storage
- `data_models`: Data structures (DetectionData, BundledData, DetectionMask)

## Performance Considerations

### Memory Management
- Queue limited to 100 items (~200MB for 1080p)
- Frames released after bundling
- Persistent storage uses disk, not RAM
- Model cleanup on shutdown

### CPU Usage
- Frame interval controls inference frequency (default: every 3rd frame)
- Separate threads prevent blocking
- NumPy vectorized operations for masks
- Efficient contour extraction with OpenCV

### I/O Optimization
- Asynchronous Kafka producer with batching
- Compressed JPEG storage (quality optimized)
- MinIO streaming uploads
- Local storage with separate image/metadata dirs

### Network Optimization
- Avro schema evolution support
- Compressed image format (JPEG)
- Connection pooling via Kafka producer
- Automatic retry with exponential backoff (planned)

## Troubleshooting

### Pipeline Won't Start

**Issue**: Configuration errors
```bash
# Solution: Validate config file
python -c "from config_loader import load_config; load_config().print_config_summary()"
```

**Issue**: Model file not found
```bash
# Solution: Check weights path
ls -la models/yolo11s.pt  # or models/rfdetr.pth
```

**Issue**: Cloud services unavailable
- Check: Kafka brokers at `localhost:19092,29092,39092`
- Check: MinIO at `localhost:9000`
- Solution: Pipeline will run in offline mode automatically

### High Queue Depth

**Symptoms**: Queue depth consistently near 100

**Solutions:**
1. Increase `frame_interval` in config (e.g., 3 → 5)
2. Check network bandwidth to Kafka/MinIO
3. Verify cloud service performance
4. Consider reducing video resolution
5. Check if upload worker is blocked

### Visualization Issues

**Issue**: Window not displaying

**Solutions:**
- Verify X11/Wayland display server is running
- Check `DISPLAY` environment variable
- Set `enable_monitoring: false` in config for headless mode
- Install full OpenCV: `pip install opencv-python` (not `opencv-python-headless`)

### Memory Leaks

**Symptoms**: Memory usage grows over time

**Solutions:**
1. Check queue is being consumed (queue depth should fluctuate)
2. Verify frames are released after processing
3. Monitor `local/` directory size (offline storage)
4. Restart pipeline periodically if needed

## File Structure

```
edge/
├── main.py                      # Main pipeline orchestrator
├── config.yaml                  # Configuration file
├── config_loader.py             # Config parser with env var support
├── uploader.py                  # Cloud upload and offline storage
├── data_models.py               # Data structures
├── segmentation/
│   ├── __init__.py              # Module exports
│   └── pothole_segmenter.py     # Segmentation abstraction
├── models/                      # Model weights (gitignored)
│   ├── yolo11s.pt
│   └── rfdetr.pth
├── local/                       # Offline storage (auto-created)
│   ├── images/                  # Stored frames
│   └── metadata/                # Detection metadata JSON
└── README.md                    # This file
```

## Future Enhancements

- [x] Web-based monitoring dashboard
- [x] Support for RF-DETR model
- [x] Offline storage with automatic recovery
- [x] Configurable queue size
- [ ] Multi-camera support with camera selection
- [ ] Hardware acceleration (CUDA/TensorRT)
- [ ] Exponential backoff for connection retry
- [ ] Configurable GPS source (not just simulation)
- [ ] Real-time metrics export (Prometheus/Grafana)
- [ ] Dynamic frame interval based on detection density
