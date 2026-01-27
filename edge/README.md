# Edge Pipeline

The main orchestrator for the edge device pipeline that coordinates pothole detection, processing, and data uploading.

## Overview

The `edge_pipeline.py` module implements a multi-threaded pipeline that:
1. Processes video frames to detect potholes using YOLO segmentation
2. Queues detections in memory for processing
3. Enriches detection data and uploads to cloud services (Kafka/S3)
4. Handles offline scenarios with persistent storage
5. Provides real-time visualization with OpenCV

## Architecture

```
      ┌─────────────────┐
      │  Video Source   │
      └────────┬────────┘
               │
               ▼
┌─────────────────────────────┐
│  Inference Worker           │
│  - Frame sampling           │
│  - YOLO segmentation        │
│  - Trapezoid filtering      │
│  - Detection queueing       │
└──────────────┬──────────────┘
               │
               │ (In-memory Queue)
               ▼
┌─────────────────────────────┐
│  Processing Worker          │
│  - Surface area estimation  │
│  - Cloud upload             │
│  - Offline storage          │
│  - Connection recovery      │
└─────────────────────────────┘
```

## Key Components

### EdgePipeline Class

Main orchestrator that manages the pipeline lifecycle.

#### Initialization
```python
pipeline = EdgePipeline(config_path="config.yaml")
```

**Parameters:**
- `config_path` (str): Path to YAML configuration file

**Attributes:**
- `vehicle_id` (str): Unique vehicle identifier (auto-generated)
- `detection_queue` (Queue): In-memory queue (max 100 items)
- `segmenter` (PotholeSegmentationYOLO): Segmentation model
- `processor` (ProcessingUploader): Upload and storage handler
- `running` (bool): Pipeline state flag

### Worker Threads

#### 1. Inference Worker (`inference_worker`)

Processes video frames and detects potholes.

**Responsibilities:**
- Read video frames from source
- Apply frame interval sampling
- Create masked images with trapezoid ROI
- Run YOLO inference
- Filter detections within trapezoid
- Queue valid detections
- Display real-time visualization

**Visualization:**
- Green trapezoid: Detection region of interest
- Blue filled areas: Detected potholes
- Press 'q' to quit

#### 2. Processing Worker (`processing_worker`)

Processes queued detections and handles uploads.

**Responsibilities:**
- Dequeue detections from in-memory queue
- Enrich detection data (GPS, metadata)
- Upload to cloud (Kafka + S3)
- Handle offline storage
- Monitor connection status
- Process persistent storage when back online

**Error Handling:**
- Automatic offline mode on upload failure
- Periodic connection checks (every 10 stored items)
- Persistent storage recovery on reconnection

## Pipeline Lifecycle

### Starting the Pipeline

```python
pipeline = EdgePipeline(config_path="config.yaml")
pipeline.start()
```

**Startup Sequence:**
1. Load configuration
2. Initialize segmentation model
3. Initialize processing/upload handlers
4. Check online/offline status
5. Start inference thread
6. Start processing thread
7. Monitor and report stats

### Stopping the Pipeline

**Methods:**
- Press `Ctrl+C` (SIGINT)
- Press `q` in visualization window
- Send SIGTERM signal
- Call `pipeline.stop()` programmatically

**Shutdown Sequence:**
1. Set `running` flag to False
2. Wait for threads to finish (5s timeout)
3. Flush Kafka producer
4. Print final statistics

## Configuration

The pipeline reads settings from `config.yaml`:

```yaml
model:
  type: yolo
  path: models/pothole_model.pt
  confidence_threshold: 0.5

video:
  path: data/test_video.mp4
  frame_interval: 5

detection:
  trapezoid_coords:
    - [0.2, 0.6]
    - [0.8, 0.6]
    - [0.95, 0.95]
    - [0.05, 0.95]

kafka:
  bootstrap_servers: localhost:9092
  topic: pothole-detections

aws:
  bucket: pothole-images
  region: us-east-1
```

## Usage

### Command Line

```bash
# Use default config.yaml
python edge_pipeline.py

# Specify custom config
python edge_pipeline.py --config custom_config.yaml
```

### Programmatic

```python
from edge_pipeline import EdgePipeline

# Initialize pipeline
pipeline = EdgePipeline(config_path="config.yaml")

# Start processing
pipeline.start()

# Stop when done
pipeline.stop()
```

## Queue Management

The detection queue has a maximum size of 100 items to prevent out-of-memory issues.

**Queue Behavior:**
- **Full Queue:** Drops new frames with warning message
- **Empty Queue:** Processing worker blocks with 1s timeout
- **Queue Size:** Reported in stats every 30 seconds

## Statistics & Monitoring

The pipeline prints statistics every 30 seconds:

```
[STATS] Processed: 45 | Uploaded: 42 | Stored: 3 | Queue: 2
```

**Metrics:**
- `Processed`: Total detections processed
- `Uploaded`: Successfully uploaded to cloud
- `Stored`: Saved to persistent storage (offline)
- `Queue`: Current in-memory queue depth

## Error Handling

### Inference Errors
- Video file not found → Stop pipeline
- Frame read failure → Stop pipeline gracefully
- Model inference error → Log and continue

### Processing Errors
- Upload failure → Switch to offline mode
- Storage write error → Log and continue
- Queue timeout → Check running flag and retry

### Connection Recovery
- Periodic health checks in offline mode
- Automatic persistent storage processing on reconnection
- Seamless transition between online/offline modes

## Dependencies

**Core:**
- `numpy`: Array operations
- `opencv-python (cv2)`: Video processing and display
- `queue`: Thread-safe queueing
- `threading`: Multi-threading support

**Project Modules:**
- `config_loader`: Configuration management
- `segmentation`: YOLO model wrapper
- `processing_uploader`: Data enrichment and upload
- `data_models`: Data structures (DetectionData, DetectionMask)

## Thread Safety

- **Queue:** Thread-safe `queue.Queue` for detection sharing
- **Flags:** `running` flag for coordinated shutdown
- **Resources:** Each thread manages its own resources
- **No Shared State:** Workers communicate only via queue

## Performance Considerations

### Memory Management
- Queue size limited to 100 items (~200MB max for 1080p frames)
- Frames released after processing
- Persistent storage for offline scenarios

### CPU Usage
- Frame interval controls inference frequency
- Separate threads prevent blocking
- Efficient mask operations with NumPy

### I/O Optimization
- Batch uploads when online
- Async Kafka producer
- Compressed image storage

## Troubleshooting

### Pipeline Won't Start
- Check video path in config
- Verify model file exists
- Check Kafka/S3 connectivity (offline mode OK)

### High Queue Depth
- Increase `frame_interval` to reduce load
- Check network bandwidth
- Verify cloud service performance

### Display Issues
- Ensure X11/display server available
- Check OpenCV installation
- Verify window manager compatibility

## Future Enhancements

- [ ] Support for RT-DETR model
- [ ] Multi-camera support
- [ ] Hardware acceleration (CUDA/TensorRT)
- [ ] Configurable queue size
- [ ] Web-based monitoring dashboard
- [ ] Graceful reconnection with exponential backoff
