```
┌─────────────┐
│ Edge Device │ (detect + segment)
└──────┬──────┘
       │ 1. Upload image
       ↓
┌─────────────┐
│    MinIO    │ warehouse/raw_images/{event_id}.jpg
└──────┬──────┘
       │ 2. Send event
       ↓
┌──────────────────────────┐
│ pothole.raw.events.v1    │ (partition by vehicle_id)
└────┬────────────┬────────┘
     │            │
     ↓            ↓
┌─────────┐  ┌──────────────┐
│ Depth   │  │ Surface Area │
│ Service │  │ Service      │
└────┬────┘  └──────┬───────┘
     │              │
     ↓              ↓
┌─────────────┐  ┌─────────────────┐
│pothole.     │  │pothole.surface. │
│depth.v1     │  │area.v1          │
└────┬────────┘  └────┬────────────┘
     │                │
     └────────┬───────┘
              ↓
     ┌────────────────┐
     │  Aggregation   │ (join by event_id)
     │    Service     │
     └────────┬───────┘
              │ 3. Calculate severity
              ↓
     ┌─────────────────────┐
     │pothole.severity.    │
     │score.v1             │
     └──────────┬──────────┘
                │
                ↓
     ┌──────────────────────┐
     │ Final Enrichment     │
     │ Service              │
     │ - Read raw_events    │
     │ - Join severity      │
     │ - Call OSM API       │
     │ - Dedupe potholes    │
     └────┬─────────────┬───┘
          │             │
          ↓             ↓
     ┌─────────┐  ┌──────────────┐
     │potholes │  │pothole_      │
     │(upsert) │  │history(ins.) │
     └─────────┘  └──────────────┘
```