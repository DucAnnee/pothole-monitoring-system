"""
Historical Data Seeder for Pothole Monitoring System

Seeds the Iceberg tables with realistic historical data for demonstration.
Generates 5-15 new potholes per day for the last 30 days, with 2-5 potholes
resolved every 3 days.

Run this AFTER data platform containers are healthy, but BEFORE cloud/edge services.
"""

import random
import time
import uuid
import json
import argparse
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import trino.dbapi
import h3

# ============================================================================
# CONFIGURATION
# ============================================================================

# Default Trino connection settings (can be overridden via CLI args)
DEFAULT_TRINO_HOST = "localhost"
DEFAULT_TRINO_PORT = 8081
DEFAULT_TRINO_USER = "trino"
DEFAULT_TRINO_CATALOG = "iceberg"
DEFAULT_TRINO_SCHEMA = "city"

# Seed data parameters
DAYS_OF_HISTORY = 30
MIN_POTHOLES_PER_DAY = 5
MAX_POTHOLES_PER_DAY = 15
MIN_RESOLVED_PER_3_DAYS = 2
MAX_RESOLVED_PER_3_DAYS = 5

# Geographic bounds for Ho Chi Minh City (can be expanded for other cities)
GEO_BOUNDS = {
    "hcmc": {
        "lat_min": 10.75,
        "lat_max": 10.85,
        "lon_min": 106.60,
        "lon_max": 106.75,
        "city": "Ho Chi Minh City",
        "districts": [
            {"name": "District 1", "ward_prefix": "Ben Nghe"},
            {"name": "District 3", "ward_prefix": "Ward"},
            {"name": "District 5", "ward_prefix": "Ward"},
            {"name": "District 7", "ward_prefix": "Tan Phong"},
            {"name": "District 10", "ward_prefix": "Ward"},
            {"name": "Binh Thanh", "ward_prefix": "Ward"},
            {"name": "Phu Nhuan", "ward_prefix": "Ward"},
            {"name": "Go Vap", "ward_prefix": "Ward"},
            {"name": "Thu Duc", "ward_prefix": "Linh Trung"},
            {"name": "Tan Binh", "ward_prefix": "Ward"},
        ],
        "streets": [
            "Nguyen Hue", "Le Loi", "Dong Khoi", "Hai Ba Trung", 
            "Nguyen Thi Minh Khai", "Dien Bien Phu", "Le Van Sy",
            "Nguyen Van Troi", "Cach Mang Thang 8", "Vo Van Tan",
            "Truong Chinh", "Pham Van Dong", "Nguyen Van Linh",
            "Le Duc Tho", "Quang Trung", "Hoang Van Thu",
            "Nguyen Xi", "Xo Viet Nghe Tinh", "Phan Xich Long",
            "Bach Dang", "Nguyen Dinh Chieu", "Le Thanh Ton",
        ],
    },
    "hanoi": {
        "lat_min": 20.98,
        "lat_max": 21.08,
        "lon_min": 105.78,
        "lon_max": 105.88,
        "city": "Hanoi",
        "districts": [
            {"name": "Hoan Kiem", "ward_prefix": "Hang"},
            {"name": "Ba Dinh", "ward_prefix": "Phuc Xa"},
            {"name": "Dong Da", "ward_prefix": "Cat Linh"},
            {"name": "Hai Ba Trung", "ward_prefix": "Bach Khoa"},
            {"name": "Cau Giay", "ward_prefix": "Dich Vong"},
        ],
        "streets": [
            "Trang Tien", "Hang Bai", "Pho Hue", "Nguyen Thai Hoc",
            "Kim Ma", "Giang Vo", "Lang Ha", "Thai Ha",
            "Ton Duc Thang", "Nguyen Chi Thanh", "Xuan Thuy",
        ],
    }
}

# Severity levels and their characteristics
# Boundaries define when severity_score maps to each level:
#   MINOR: 1-3, MODERATE: 4-5, HIGH: 6-7, CRITICAL: 8-10
SEVERITY_CONFIG = {
    "MINOR": {"score_range": (1, 3), "depth_range": (1.0, 3.0), "area_range": (50, 200)},
    "MODERATE": {"score_range": (4, 5), "depth_range": (3.0, 6.0), "area_range": (200, 500)},
    "HIGH": {"score_range": (6, 7), "depth_range": (6.0, 10.0), "area_range": (500, 1000)},
    "CRITICAL": {"score_range": (8, 10), "depth_range": (10.0, 20.0), "area_range": (1000, 3000)},
}

# Normal distribution parameters for severity score
# Mean=5.5 centers distribution around moderate-high severity
# Std=1.5 gives good spread: ~68% in [4,7], ~27% in [2,3] or [8,9], ~5% extreme
SEVERITY_MEAN = 5.5
SEVERITY_STD = 1.5


# ============================================================================
# H3 HELPERS
# ============================================================================

H3_DEDUP_RESOLUTION = 12  # ~0.5m hexagons


def get_h3_index(lat: float, lon: float, resolution: int = H3_DEDUP_RESOLUTION) -> int:
    """Convert lat/lon to H3 index as integer."""
    h3_hex = h3.latlng_to_cell(lat, lon, resolution)
    return h3.str_to_int(h3_hex)


# ============================================================================
# DATA GENERATION HELPERS
# ============================================================================

def random_location(bounds: Dict) -> Tuple[float, float]:
    """Generate random GPS coordinates within bounds."""
    lat = random.uniform(bounds["lat_min"], bounds["lat_max"])
    lon = random.uniform(bounds["lon_min"], bounds["lon_max"])
    return round(lat, 7), round(lon, 7)


def random_address(bounds: Dict) -> Dict[str, str]:
    """Generate random address data."""
    district = random.choice(bounds["districts"])
    street = random.choice(bounds["streets"])
    ward_num = random.randint(1, 20)
    
    return {
        "city": bounds["city"],
        "district": district["name"],
        "ward": f"{district['ward_prefix']} {ward_num}",
        "street_name": f"{random.randint(1, 500)} {street} Street",
        "road_id": f"osm_way_{random.randint(100000, 999999)}",
    }


def score_to_severity_level(score: int) -> str:
    """
    Map severity score (1-10) to severity level category.
    """
    if score <= 3:
        return "MINOR"
    elif score <= 5:
        return "MODERATE"
    elif score <= 7:
        return "HIGH"
    else:
        return "CRITICAL"


def random_severity() -> Tuple[str, float, float, float]:
    """
    Generate random severity using NORMAL DISTRIBUTION.
    
    Distribution: N(μ=5.5, σ=1.5) clipped to [1, 10]
    Expected outcome:
      - ~68% of potholes have severity 4-7 (moderate to high)
      - ~27% have severity 2-3 or 8-9 (minor or critical)
      - ~5% have severity 1 or 10 (extreme values)
    
    Returns: (severity_level, depth_cm, surface_area_cm2, severity_score)
    """
    # Generate severity score from normal distribution
    raw_score = np.random.normal(loc=SEVERITY_MEAN, scale=SEVERITY_STD)
    
    # Clip to valid range [1, 10] and round to integer
    severity_score = int(np.clip(np.round(raw_score), 1, 10))
    
    # Determine severity level from score
    severity_level = score_to_severity_level(severity_score)
    
    # Get the config for this severity level to generate consistent depth/area
    config = SEVERITY_CONFIG[severity_level]
    
    # Generate depth and area within the appropriate range for this severity level
    # Add some noise to make it more realistic
    depth_cm = round(random.uniform(*config["depth_range"]), 2)
    surface_area_cm2 = round(random.uniform(*config["area_range"]), 2)
    
    # Return as float for database compatibility
    return severity_level, depth_cm, surface_area_cm2, float(severity_score)


def generate_polygon(lat: float, lon: float, area_cm2: float) -> str:
    """Generate a simple polygon around the center point."""
    # Approximate radius in degrees (very rough)
    # 1 degree ≈ 111km, so 1m ≈ 0.000009 degrees
    radius_m = (area_cm2 / 10000) ** 0.5  # rough sqrt of area in m2
    radius_deg = radius_m * 0.000009 * 2  # scale factor
    
    # Generate hexagonal polygon
    points = []
    for i in range(6):
        angle = i * 60 * 3.14159 / 180
        px = lon + radius_deg * 1.5 * (1 + random.uniform(-0.1, 0.1)) * (angle / 3.14159)
        py = lat + radius_deg * (1 + random.uniform(-0.1, 0.1)) * ((6 - i) / 6)
        points.append([round(px, 8), round(py, 8)])
    points.append(points[0])  # Close the polygon
    
    return json.dumps({
        "type": "Polygon",
        "coordinates": [points]
    })


def random_vehicle_id() -> str:
    """Generate a random vehicle ID."""
    prefixes = ["HCM-HN", "HCM-BT", "HCM-TD", "HCM-GV", "HCM-Q1", "HCM-Q3", "HCM-Q7"]
    return f"{random.choice(prefixes)}-{random.randint(1, 99):02d}"


def format_timestamp(dt: datetime) -> str:
    """Format datetime for Trino TIMESTAMP."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def escape_sql(value: str) -> str:
    """Escape single quotes for SQL."""
    if value is None:
        return None
    return value.replace("'", "''")


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

class TrinoSeeder:
    """Handles database operations for seeding historical data."""
    
    def __init__(self, host: str, port: int, user: str, catalog: str, schema: str):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.conn = None
        
    def connect(self, max_retries: int = 30, retry_interval: int = 5) -> bool:
        """Connect to Trino with retries."""
        print(f"[CONNECT] Connecting to Trino at {self.host}:{self.port}...")
        
        for attempt in range(max_retries):
            try:
                self.conn = trino.dbapi.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                )
                # Test connection
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                print(f"[CONNECT] Successfully connected to Trino")
                return True
            except Exception as e:
                print(f"[CONNECT] Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"[CONNECT] Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
        
        print("[CONNECT] Failed to connect to Trino after all retries")
        return False
    
    def ensure_tables_exist(self):
        """Create tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Create schema
        print("[SETUP] Creating schema if not exists...")
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
            cursor.fetchall()
        except Exception as e:
            print(f"[WARN] Schema creation: {e}")
        
        # Potholes table DDL
        potholes_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.potholes (
            pothole_id VARCHAR NOT NULL COMMENT 'UUID for this pothole',
            first_event_id VARCHAR NOT NULL COMMENT 'Event that first detected this pothole',
            reported_at TIMESTAMP(3) NOT NULL COMMENT 'First detection timestamp',
            gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
            gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
            geom_h3 BIGINT NOT NULL COMMENT 'H3 index at resolution 12',
            city VARCHAR COMMENT 'City name',
            ward VARCHAR COMMENT 'Ward',
            district VARCHAR COMMENT 'District',
            street_name VARCHAR COMMENT 'Street name',
            road_id VARCHAR COMMENT 'OSM way_id or road identifier',
            depth_cm DOUBLE NOT NULL COMMENT 'Latest depth in centimeters',
            surface_area_cm2 DOUBLE NOT NULL COMMENT 'Latest surface area',
            severity_score DOUBLE NOT NULL COMMENT 'Latest severity',
            severity_level VARCHAR NOT NULL COMMENT 'MINOR/MODERATE/HIGH/CRITICAL',
            pothole_polygon VARCHAR NOT NULL COMMENT 'Latest GeoJSON polygon',
            raw_image_path VARCHAR COMMENT 'S3 path to raw image',
            bev_image_path VARCHAR COMMENT 'S3 path to BEV image',
            status VARCHAR NOT NULL COMMENT 'reported | in_progress | fixed',
            in_progress_at TIMESTAMP(3) COMMENT 'When repair started',
            fixed_at TIMESTAMP(3) COMMENT 'When repair completed',
            last_updated_at TIMESTAMP(3) NOT NULL COMMENT 'Last UPSERT timestamp',
            observation_count INTEGER NOT NULL COMMENT 'How many times seen'
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['month(reported_at)']
        )
        """
        
        # Pothole history table DDL
        history_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.pothole_history (
            observation_id VARCHAR NOT NULL COMMENT 'UUID for this observation',
            pothole_id VARCHAR NOT NULL COMMENT 'Links to potholes table',
            event_id VARCHAR NOT NULL COMMENT 'Links to raw_events',
            recorded_at TIMESTAMP(3) NOT NULL COMMENT 'Observation timestamp',
            depth_cm DOUBLE NOT NULL COMMENT 'Depth at observation',
            surface_area_cm2 DOUBLE NOT NULL COMMENT 'Surface area at observation',
            severity_score DOUBLE NOT NULL COMMENT 'Severity at observation',
            severity_level VARCHAR NOT NULL COMMENT 'Severity level at observation',
            gps_lat DOUBLE NOT NULL COMMENT 'Lat at observation',
            gps_lon DOUBLE NOT NULL COMMENT 'Lon at observation',
            pothole_polygon VARCHAR NOT NULL COMMENT 'Polygon at observation',
            status VARCHAR NOT NULL COMMENT 'Status at this observation'
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['day(recorded_at)']
        )
        """
        
        print("[SETUP] Creating potholes table if not exists...")
        try:
            cursor.execute(potholes_ddl)
            cursor.fetchall()
            print("[SETUP] Potholes table ready")
        except Exception as e:
            print(f"[ERROR] Creating potholes table: {e}")
            raise
        
        print("[SETUP] Creating pothole_history table if not exists...")
        try:
            cursor.execute(history_ddl)
            cursor.fetchall()
            print("[SETUP] Pothole_history table ready")
        except Exception as e:
            print(f"[ERROR] Creating pothole_history table: {e}")
            raise
        
        cursor.close()
    
    def clear_existing_data(self):
        """Clear existing seeded data (optional)."""
        cursor = self.conn.cursor()
        
        print("[CLEANUP] Clearing existing data...")
        try:
            cursor.execute(f"DELETE FROM {self.catalog}.{self.schema}.pothole_history WHERE 1=1")
            cursor.fetchall()
            print("[CLEANUP] Cleared pothole_history")
        except Exception as e:
            print(f"[WARN] Could not clear pothole_history: {e}")
        
        try:
            cursor.execute(f"DELETE FROM {self.catalog}.{self.schema}.potholes WHERE 1=1")
            cursor.fetchall()
            print("[CLEANUP] Cleared potholes")
        except Exception as e:
            print(f"[WARN] Could not clear potholes: {e}")
        
        cursor.close()
    
    def insert_pothole(self, pothole: Dict[str, Any]) -> bool:
        """Insert a pothole record."""
        cursor = self.conn.cursor()
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.potholes (
            pothole_id, first_event_id, reported_at, gps_lat, gps_lon, geom_h3,
            city, ward, district, street_name, road_id,
            depth_cm, surface_area_cm2, severity_score, severity_level,
            pothole_polygon, raw_image_path, bev_image_path,
            status, in_progress_at, fixed_at, last_updated_at, observation_count
        ) VALUES (
            '{pothole["pothole_id"]}',
            '{pothole["first_event_id"]}',
            TIMESTAMP '{pothole["reported_at"]}',
            {pothole["gps_lat"]},
            {pothole["gps_lon"]},
            {pothole["geom_h3"]},
            {f"'{escape_sql(pothole['city'])}'" if pothole.get('city') else 'NULL'},
            {f"'{escape_sql(pothole['ward'])}'" if pothole.get('ward') else 'NULL'},
            {f"'{escape_sql(pothole['district'])}'" if pothole.get('district') else 'NULL'},
            {f"'{escape_sql(pothole['street_name'])}'" if pothole.get('street_name') else 'NULL'},
            {f"'{escape_sql(pothole['road_id'])}'" if pothole.get('road_id') else 'NULL'},
            {pothole["depth_cm"]},
            {pothole["surface_area_cm2"]},
            {pothole["severity_score"]},
            '{pothole["severity_level"]}',
            '{escape_sql(pothole["pothole_polygon"])}',
            {f"'{escape_sql(pothole['raw_image_path'])}'" if pothole.get('raw_image_path') else 'NULL'},
            {f"'{escape_sql(pothole['bev_image_path'])}'" if pothole.get('bev_image_path') else 'NULL'},
            '{pothole["status"]}',
            {f"TIMESTAMP '{pothole['in_progress_at']}'" if pothole.get('in_progress_at') else 'NULL'},
            {f"TIMESTAMP '{pothole['fixed_at']}'" if pothole.get('fixed_at') else 'NULL'},
            TIMESTAMP '{pothole["last_updated_at"]}',
            {pothole["observation_count"]}
        )
        """
        
        try:
            cursor.execute(insert_sql)
            cursor.fetchall()
            cursor.close()
            return True
        except Exception as e:
            print(f"[ERROR] Insert pothole failed: {e}")
            cursor.close()
            return False
    
    def insert_history(self, history: Dict[str, Any]) -> bool:
        """Insert a pothole history record."""
        cursor = self.conn.cursor()
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.pothole_history (
            observation_id, pothole_id, event_id, recorded_at,
            depth_cm, surface_area_cm2, severity_score, severity_level,
            gps_lat, gps_lon, pothole_polygon, status
        ) VALUES (
            '{history["observation_id"]}',
            '{history["pothole_id"]}',
            '{history["event_id"]}',
            TIMESTAMP '{history["recorded_at"]}',
            {history["depth_cm"]},
            {history["surface_area_cm2"]},
            {history["severity_score"]},
            '{history["severity_level"]}',
            {history["gps_lat"]},
            {history["gps_lon"]},
            '{escape_sql(history["pothole_polygon"])}',
            '{history["status"]}'
        )
        """
        
        try:
            cursor.execute(insert_sql)
            cursor.fetchall()
            cursor.close()
            return True
        except Exception as e:
            print(f"[ERROR] Insert history failed: {e}")
            cursor.close()
            return False
    
    def update_pothole_status(self, pothole_id: str, status: str, 
                              in_progress_at: Optional[str] = None,
                              fixed_at: Optional[str] = None,
                              last_updated_at: str = None) -> bool:
        """Update pothole status."""
        cursor = self.conn.cursor()
        
        updates = [f"status = '{status}'"]
        if in_progress_at:
            updates.append(f"in_progress_at = TIMESTAMP '{in_progress_at}'")
        if fixed_at:
            updates.append(f"fixed_at = TIMESTAMP '{fixed_at}'")
        if last_updated_at:
            updates.append(f"last_updated_at = TIMESTAMP '{last_updated_at}'")
        
        update_sql = f"""
        UPDATE {self.catalog}.{self.schema}.potholes
        SET {', '.join(updates)}
        WHERE pothole_id = '{pothole_id}'
        """
        
        try:
            cursor.execute(update_sql)
            cursor.fetchall()
            cursor.close()
            return True
        except Exception as e:
            print(f"[ERROR] Update pothole status failed: {e}")
            cursor.close()
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# ============================================================================
# MAIN SEEDING LOGIC
# ============================================================================

def generate_historical_data(
    seeder: TrinoSeeder,
    region: str = "hcmc",
    days: int = DAYS_OF_HISTORY,
    clear_existing: bool = False
) -> Dict[str, int]:
    """
    Generate and insert historical pothole data.
    
    Returns statistics about generated data.
    """
    bounds = GEO_BOUNDS.get(region, GEO_BOUNDS["hcmc"])
    
    print("=" * 70)
    print("HISTORICAL DATA SEEDER")
    print("=" * 70)
    print(f"Region: {bounds['city']}")
    print(f"Days of history: {days}")
    print(f"Potholes per day: {MIN_POTHOLES_PER_DAY}-{MAX_POTHOLES_PER_DAY}")
    print(f"Resolved per 3 days: {MIN_RESOLVED_PER_3_DAYS}-{MAX_RESOLVED_PER_3_DAYS}")
    print("=" * 70)
    
    # Setup
    seeder.ensure_tables_exist()
    
    if clear_existing:
        seeder.clear_existing_data()
    
    # Statistics
    stats = {
        "total_potholes": 0,
        "total_history_records": 0,
        "reported": 0,
        "in_progress": 0,
        "fixed": 0,
    }
    
    # Track all potholes for status updates
    all_potholes: List[Dict] = []
    now = datetime.now(timezone.utc)
    
    print("\n[PHASE 1] Generating new potholes for each day...")
    
    # Generate potholes for each day
    for day_offset in range(days, 0, -1):
        day_date = now - timedelta(days=day_offset)
        num_potholes = random.randint(MIN_POTHOLES_PER_DAY, MAX_POTHOLES_PER_DAY)
        
        print(f"  Day -{day_offset} ({day_date.strftime('%Y-%m-%d')}): {num_potholes} potholes")
        
        for i in range(num_potholes):
            # Random time within the day
            hour = random.randint(6, 22)  # 6 AM to 10 PM
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            reported_at = day_date.replace(hour=hour, minute=minute, second=second, microsecond=random.randint(0, 999999))
            
            # Generate pothole data
            lat, lon = random_location(bounds)
            address = random_address(bounds)
            severity_level, depth_cm, surface_area_cm2, severity_score = random_severity()
            
            pothole_id = str(uuid.uuid4())
            event_id = str(uuid.uuid4())
            vehicle_id = random_vehicle_id()
            
            pothole = {
                "pothole_id": pothole_id,
                "first_event_id": event_id,
                "reported_at": format_timestamp(reported_at),
                "gps_lat": lat,
                "gps_lon": lon,
                "geom_h3": get_h3_index(lat, lon),
                "city": address["city"],
                "ward": address["ward"],
                "district": address["district"],
                "street_name": address["street_name"],
                "road_id": address["road_id"],
                "depth_cm": depth_cm,
                "surface_area_cm2": surface_area_cm2,
                "severity_score": severity_score,
                "severity_level": severity_level,
                "pothole_polygon": generate_polygon(lat, lon, surface_area_cm2),
                "raw_image_path": f"s3://warehouse/raw_images/{event_id}.jpg",
                "bev_image_path": f"s3://warehouse/bev_images/{event_id}.jpg",
                "status": "reported",
                "in_progress_at": None,
                "fixed_at": None,
                "last_updated_at": format_timestamp(reported_at),
                "observation_count": 1,
                "_reported_at_dt": reported_at,  # Keep datetime for later processing
            }
            
            if seeder.insert_pothole(pothole):
                stats["total_potholes"] += 1
                stats["reported"] += 1
                all_potholes.append(pothole)
                
                # Also insert initial history record
                history = {
                    "observation_id": str(uuid.uuid4()),
                    "pothole_id": pothole_id,
                    "event_id": event_id,
                    "recorded_at": format_timestamp(reported_at),
                    "depth_cm": depth_cm,
                    "surface_area_cm2": surface_area_cm2,
                    "severity_score": severity_score,
                    "severity_level": severity_level,
                    "gps_lat": lat,
                    "gps_lon": lon,
                    "pothole_polygon": pothole["pothole_polygon"],
                    "status": "reported",
                }
                
                if seeder.insert_history(history):
                    stats["total_history_records"] += 1
    
    print(f"\n[PHASE 1 COMPLETE] Created {stats['total_potholes']} potholes")
    
    # Phase 2: Resolve some potholes every 3 days
    print("\n[PHASE 2] Resolving potholes every 3 days...")
    
    # Sort by reported date
    all_potholes.sort(key=lambda x: x["_reported_at_dt"])
    
    # Track which potholes are available to resolve
    reportable_potholes = all_potholes.copy()
    
    for day_offset in range(days - 3, 0, -3):  # Every 3 days
        resolve_date = now - timedelta(days=day_offset)
        num_to_resolve = random.randint(MIN_RESOLVED_PER_3_DAYS, MAX_RESOLVED_PER_3_DAYS)
        
        # Filter potholes that were reported before this date and still reported
        eligible = [p for p in reportable_potholes 
                   if p["_reported_at_dt"] < resolve_date - timedelta(days=2) 
                   and p["status"] == "reported"]
        
        if not eligible:
            print(f"  Day -{day_offset}: No eligible potholes to resolve")
            continue
        
        # Select random potholes to resolve
        to_resolve = random.sample(eligible, min(num_to_resolve, len(eligible)))
        
        print(f"  Day -{day_offset} ({resolve_date.strftime('%Y-%m-%d')}): Resolving {len(to_resolve)} potholes")
        
        for pothole in to_resolve:
            # Set in_progress 1-3 days before fixed
            in_progress_offset = random.randint(1, 3)
            in_progress_at = resolve_date - timedelta(days=in_progress_offset)
            fixed_at = resolve_date.replace(
                hour=random.randint(8, 17),
                minute=random.randint(0, 59)
            )
            
            # Update status
            if seeder.update_pothole_status(
                pothole_id=pothole["pothole_id"],
                status="fixed",
                in_progress_at=format_timestamp(in_progress_at),
                fixed_at=format_timestamp(fixed_at),
                last_updated_at=format_timestamp(fixed_at)
            ):
                pothole["status"] = "fixed"
                stats["reported"] -= 1
                stats["fixed"] += 1
                
                # Add history record for in_progress
                history_in_progress = {
                    "observation_id": str(uuid.uuid4()),
                    "pothole_id": pothole["pothole_id"],
                    "event_id": pothole["first_event_id"],
                    "recorded_at": format_timestamp(in_progress_at),
                    "depth_cm": pothole["depth_cm"],
                    "surface_area_cm2": pothole["surface_area_cm2"],
                    "severity_score": pothole["severity_score"],
                    "severity_level": pothole["severity_level"],
                    "gps_lat": pothole["gps_lat"],
                    "gps_lon": pothole["gps_lon"],
                    "pothole_polygon": pothole["pothole_polygon"],
                    "status": "in_progress",
                }
                if seeder.insert_history(history_in_progress):
                    stats["total_history_records"] += 1
                
                # Add history record for fixed
                history_fixed = {
                    "observation_id": str(uuid.uuid4()),
                    "pothole_id": pothole["pothole_id"],
                    "event_id": pothole["first_event_id"],
                    "recorded_at": format_timestamp(fixed_at),
                    "depth_cm": pothole["depth_cm"],
                    "surface_area_cm2": pothole["surface_area_cm2"],
                    "severity_score": pothole["severity_score"],
                    "severity_level": pothole["severity_level"],
                    "gps_lat": pothole["gps_lat"],
                    "gps_lon": pothole["gps_lon"],
                    "pothole_polygon": pothole["pothole_polygon"],
                    "status": "fixed",
                }
                if seeder.insert_history(history_fixed):
                    stats["total_history_records"] += 1
    
    # Phase 3: Set some recent potholes to in_progress
    print("\n[PHASE 3] Setting some recent potholes to in_progress...")
    
    recent_reported = [p for p in all_potholes 
                      if p["status"] == "reported" 
                      and p["_reported_at_dt"] > now - timedelta(days=10)]
    
    num_in_progress = min(random.randint(3, 8), len(recent_reported))
    if num_in_progress > 0:
        to_in_progress = random.sample(recent_reported, num_in_progress)
        
        for pothole in to_in_progress:
            in_progress_at = pothole["_reported_at_dt"] + timedelta(
                days=random.randint(1, 3),
                hours=random.randint(0, 12)
            )
            
            if seeder.update_pothole_status(
                pothole_id=pothole["pothole_id"],
                status="in_progress",
                in_progress_at=format_timestamp(in_progress_at),
                last_updated_at=format_timestamp(in_progress_at)
            ):
                pothole["status"] = "in_progress"
                stats["reported"] -= 1
                stats["in_progress"] += 1
                
                # Add history record
                history = {
                    "observation_id": str(uuid.uuid4()),
                    "pothole_id": pothole["pothole_id"],
                    "event_id": pothole["first_event_id"],
                    "recorded_at": format_timestamp(in_progress_at),
                    "depth_cm": pothole["depth_cm"],
                    "surface_area_cm2": pothole["surface_area_cm2"],
                    "severity_score": pothole["severity_score"],
                    "severity_level": pothole["severity_level"],
                    "gps_lat": pothole["gps_lat"],
                    "gps_lon": pothole["gps_lon"],
                    "pothole_polygon": pothole["pothole_polygon"],
                    "status": "in_progress",
                }
                if seeder.insert_history(history):
                    stats["total_history_records"] += 1
        
        print(f"  Set {num_in_progress} potholes to in_progress")
    
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Seed historical pothole data into Iceberg tables"
    )
    parser.add_argument(
        "--host", 
        default=DEFAULT_TRINO_HOST,
        help=f"Trino host (default: {DEFAULT_TRINO_HOST})"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=DEFAULT_TRINO_PORT,
        help=f"Trino port (default: {DEFAULT_TRINO_PORT})"
    )
    parser.add_argument(
        "--user", 
        default=DEFAULT_TRINO_USER,
        help=f"Trino user (default: {DEFAULT_TRINO_USER})"
    )
    parser.add_argument(
        "--catalog", 
        default=DEFAULT_TRINO_CATALOG,
        help=f"Trino catalog (default: {DEFAULT_TRINO_CATALOG})"
    )
    parser.add_argument(
        "--schema", 
        default=DEFAULT_TRINO_SCHEMA,
        help=f"Trino schema (default: {DEFAULT_TRINO_SCHEMA})"
    )
    parser.add_argument(
        "--region",
        choices=["hcmc", "hanoi"],
        default="hcmc",
        help="Geographic region for data generation (default: hcmc)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DAYS_OF_HISTORY,
        help=f"Days of historical data to generate (default: {DAYS_OF_HISTORY})"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing data before seeding"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible data generation"
    )
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
        print(f"[CONFIG] Using random seed: {args.seed}")
    
    # Create seeder and connect
    seeder = TrinoSeeder(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema
    )
    
    if not seeder.connect():
        print("[FATAL] Could not connect to Trino. Exiting.")
        return 1
    
    try:
        stats = generate_historical_data(
            seeder=seeder,
            region=args.region,
            days=args.days,
            clear_existing=args.clear
        )
        
        print("\n" + "=" * 70)
        print("SEEDING COMPLETE")
        print("=" * 70)
        print(f"Total potholes created: {stats['total_potholes']}")
        print(f"  - Reported: {stats['reported']}")
        print(f"  - In Progress: {stats['in_progress']}")
        print(f"  - Fixed: {stats['fixed']}")
        print(f"Total history records: {stats['total_history_records']}")
        print("=" * 70)
        
        return 0
        
    except Exception as e:
        print(f"[FATAL] Seeding failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        seeder.close()


if __name__ == "__main__":
    exit(main())
