-- Demo seed data for report screenshots.
-- Inserts directly into serving.current_road_defects (bypasses inbox trigger).
-- Safe to re-run: ON CONFLICT DO NOTHING.

INSERT INTO serving.current_road_defects (
  defect_id, defect_type, status,
  severity_score, severity_level, confidence, quality_flags,
  geometry, district, ward,
  first_seen_at, last_seen_at, observation_count,
  latest_raw_image_object_key, updated_at
) VALUES

-- ── Quận 1 ────────────────────────────────────────────────────────────────────
('defect-d1-001','POTHOLE','reported',      8.7,'CRITICAL',0.88,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7021,10.7749),4326),'Quận 1','Phường Bến Nghé',    NOW()-'27 days'::interval, NOW()-'1 day'::interval,  14,'raw_images/d1_001.jpg',NOW()),
('defect-d1-002','POTHOLE','in_progress',   7.2,'HIGH',    0.76,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7003,10.7765),4326),'Quận 1','Phường Bến Thành',   NOW()-'20 days'::interval, NOW()-'3 days'::interval,  8, 'raw_images/d1_002.jpg',NOW()),
('defect-d1-003','POTHOLE','reported',      5.4,'MODERATE',0.82,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7039,10.7741),4326),'Quận 1','Phường Đa Kao',      NOW()-'15 days'::interval, NOW()-'2 days'::interval,  5, 'raw_images/d1_003.jpg',NOW()),
('defect-d1-004','POTHOLE','reported',      3.1,'MINOR',   0.91,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6998,10.7780),4326),'Quận 1','Phường Nguyễn Thái Bình',NOW()-'10 days'::interval,NOW()-'1 day'::interval,3, 'raw_images/d1_004.jpg',NOW()),
('defect-d1-005','POTHOLE','reported',      9.1,'CRITICAL',0.43,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7015,10.7758),4326),'Quận 1','Phường Phạm Ngũ Lão',NOW()-'5 days'::interval, NOW()-'6 hours'::interval, 2, 'raw_images/d1_005.jpg',NOW()),
('defect-d1-006','POTHOLE','fixed',         6.3,'HIGH',    0.79,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7033,10.7770),4326),'Quận 1','Phường Bến Nghé',    NOW()-'30 days'::interval, NOW()-'8 days'::interval,  17,'raw_images/d1_006.jpg',NOW()),
('defect-d1-007','POTHOLE','reported',      4.8,'MODERATE',0.55,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7008,10.7745),4326),'Quận 1','Phường Cầu Kho',     NOW()-'3 days'::interval,  NOW()-'4 hours'::interval, 1, 'raw_images/d1_007.jpg',NOW()),

-- ── Quận 3 ────────────────────────────────────────────────────────────────────
('defect-d3-001','POTHOLE','reported',      7.8,'HIGH',    0.84,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6910,10.7862),4326),'Quận 3','Phường Võ Thị Sáu',  NOW()-'18 days'::interval, NOW()-'2 days'::interval,  9, 'raw_images/d3_001.jpg',NOW()),
('defect-d3-002','POTHOLE','in_progress',   9.4,'CRITICAL',0.92,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6893,10.7877),4326),'Quận 3','Phường 1',           NOW()-'25 days'::interval, NOW()-'1 day'::interval,  22,'raw_images/d3_002.jpg',NOW()),
('defect-d3-003','POTHOLE','reported',      5.1,'MODERATE',0.67,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6925,10.7848),4326),'Quận 3','Phường 2',           NOW()-'12 days'::interval, NOW()-'3 days'::interval,  4, 'raw_images/d3_003.jpg',NOW()),
('defect-d3-004','POTHOLE','reported',      3.8,'MINOR',   0.48,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6902,10.7840),4326),'Quận 3','Phường Nguyễn Cư Trinh',NOW()-'7 days'::interval,NOW()-'1 day'::interval, 2, 'raw_images/d3_004.jpg',NOW()),
('defect-d3-005','POTHOLE','reported',      6.9,'HIGH',    0.73,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6918,10.7871),4326),'Quận 3','Phường Võ Thị Sáu',  NOW()-'9 days'::interval,  NOW()-'12 hours'::interval,3, 'raw_images/d3_005.jpg',NOW()),
('defect-d3-006','POTHOLE','fixed',         4.2,'MODERATE',0.81,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6887,10.7855),4326),'Quận 3','Phường 1',           NOW()-'28 days'::interval, NOW()-'10 days'::interval, 11,'raw_images/d3_006.jpg',NOW()),

-- ── Quận 5 ────────────────────────────────────────────────────────────────────
('defect-d5-001','POTHOLE','reported',      8.2,'CRITICAL',0.87,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6681,10.7558),4326),'Quận 5','Phường 1',           NOW()-'22 days'::interval, NOW()-'2 days'::interval,  13,'raw_images/d5_001.jpg',NOW()),
('defect-d5-002','POTHOLE','in_progress',   5.7,'MODERATE',0.71,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6663,10.7570),4326),'Quận 5','Phường 4',           NOW()-'11 days'::interval, NOW()-'1 day'::interval,  6, 'raw_images/d5_002.jpg',NOW()),
('defect-d5-003','POTHOLE','reported',      3.3,'MINOR',   0.94,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6695,10.7545),4326),'Quận 5','Phường 11',          NOW()-'6 days'::interval,  NOW()-'8 hours'::interval, 2, 'raw_images/d5_003.jpg',NOW()),
('defect-d5-004','POTHOLE','reported',      7.1,'HIGH',    0.52,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6670,10.7562),4326),'Quận 5','Phường 1',           NOW()-'4 days'::interval,  NOW()-'5 hours'::interval, 1, 'raw_images/d5_004.jpg',NOW()),
('defect-d5-005','POTHOLE','reported',      4.5,'MODERATE',0.78,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6688,10.7577),4326),'Quận 5','Phường 7',           NOW()-'14 days'::interval, NOW()-'3 days'::interval,  4, 'raw_images/d5_005.jpg',NOW()),

-- ── Quận 7 ────────────────────────────────────────────────────────────────────
('defect-d7-001','POTHOLE','reported',      6.6,'HIGH',    0.83,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7225,10.7328),4326),'Quận 7','Phường Tân Phú',     NOW()-'16 days'::interval, NOW()-'1 day'::interval,  7, 'raw_images/d7_001.jpg',NOW()),
('defect-d7-002','POTHOLE','reported',      9.0,'CRITICAL',0.37,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7208,10.7340),4326),'Quận 7','Phường Tân Thuận Tây',NOW()-'8 days'::interval, NOW()-'10 hours'::interval,3, 'raw_images/d7_002.jpg',NOW()),
('defect-d7-003','POTHOLE','in_progress',   5.3,'MODERATE',0.88,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7241,10.7315),4326),'Quận 7','Phường Phú Mỹ',     NOW()-'19 days'::interval, NOW()-'2 days'::interval,  10,'raw_images/d7_003.jpg',NOW()),
('defect-d7-004','POTHOLE','reported',      3.7,'MINOR',   0.66,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7233,10.7307),4326),'Quận 7','Phường Tân Kiểng',   NOW()-'5 days'::interval,  NOW()-'1 day'::interval,  2, 'raw_images/d7_004.jpg',NOW()),
('defect-d7-005','POTHOLE','fixed',         7.4,'HIGH',    0.90,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7215,10.7355),4326),'Quận 7','Phường Tân Phú',     NOW()-'29 days'::interval, NOW()-'12 days'::interval, 18,'raw_images/d7_005.jpg',NOW()),

-- ── Quận Bình Thạnh ───────────────────────────────────────────────────────────
('defect-bt-001','POTHOLE','reported',      8.5,'CRITICAL',0.85,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7130,10.8060),4326),'Quận Bình Thạnh','Phường 1',   NOW()-'21 days'::interval, NOW()-'1 day'::interval,  15,'raw_images/bt_001.jpg',NOW()),
('defect-bt-002','POTHOLE','in_progress',   6.1,'HIGH',    0.74,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7112,10.8073),4326),'Quận Bình Thạnh','Phường 7',   NOW()-'14 days'::interval, NOW()-'2 days'::interval,  8, 'raw_images/bt_002.jpg',NOW()),
('defect-bt-003','POTHOLE','reported',      4.4,'MODERATE',0.61,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7148,10.8047),4326),'Quận Bình Thạnh','Phường 12',  NOW()-'9 days'::interval,  NOW()-'1 day'::interval,  3, 'raw_images/bt_003.jpg',NOW()),
('defect-bt-004','POTHOLE','reported',      2.9,'MINOR',   0.89,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7120,10.8085),4326),'Quận Bình Thạnh','Phường Bình Quới',NOW()-'4 days'::interval,NOW()-'6 hours'::interval,1,'raw_images/bt_004.jpg',NOW()),
('defect-bt-005','POTHOLE','reported',      7.3,'HIGH',    0.44,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7138,10.8038),4326),'Quận Bình Thạnh','Phường 1',   NOW()-'2 days'::interval,  NOW()-'3 hours'::interval, 1, 'raw_images/bt_005.jpg',NOW()),
('defect-bt-006','POTHOLE','fixed',         5.8,'MODERATE',0.78,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7106,10.8062),4326),'Quận Bình Thạnh','Phường 7',   NOW()-'26 days'::interval, NOW()-'9 days'::interval,  12,'raw_images/bt_006.jpg',NOW()),
('defect-bt-007','POTHOLE','reported',      9.3,'CRITICAL',0.91,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7155,10.8051),4326),'Quận Bình Thạnh','Phường 12',  NOW()-'3 days'::interval,  NOW()-'4 hours'::interval, 2, 'raw_images/bt_007.jpg',NOW()),

-- ── Quận Gò Vấp ───────────────────────────────────────────────────────────────
('defect-gv-001','POTHOLE','reported',      7.0,'HIGH',    0.80,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6662,10.8388),4326),'Quận Gò Vấp','Phường 1',      NOW()-'17 days'::interval, NOW()-'2 days'::interval,  6, 'raw_images/gv_001.jpg',NOW()),
('defect-gv-002','POTHOLE','reported',      4.1,'MODERATE',0.58,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6645,10.8401),4326),'Quận Gò Vấp','Phường 3',      NOW()-'10 days'::interval, NOW()-'1 day'::interval,  3, 'raw_images/gv_002.jpg',NOW()),
('defect-gv-003','POTHOLE','in_progress',   8.8,'CRITICAL',0.86,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6678,10.8375),4326),'Quận Gò Vấp','Phường 12',     NOW()-'23 days'::interval, NOW()-'1 day'::interval,  19,'raw_images/gv_003.jpg',NOW()),
('defect-gv-004','POTHOLE','reported',      3.5,'MINOR',   0.93,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6655,10.8415),4326),'Quận Gò Vấp','Phường 16',     NOW()-'6 days'::interval,  NOW()-'10 hours'::interval,2, 'raw_images/gv_004.jpg',NOW()),
('defect-gv-005','POTHOLE','reported',      6.2,'HIGH',    0.41,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6668,10.8362),4326),'Quận Gò Vấp','Phường 1',      NOW()-'2 days'::interval,  NOW()-'5 hours'::interval, 1, 'raw_images/gv_005.jpg',NOW()),
('defect-gv-006','POTHOLE','fixed',         5.0,'MODERATE',0.72,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6638,10.8379),4326),'Quận Gò Vấp','Phường 3',      NOW()-'28 days'::interval, NOW()-'11 days'::interval, 9, 'raw_images/gv_006.jpg',NOW()),

-- ── Quận Tân Bình ─────────────────────────────────────────────────────────────
('defect-tb-001','POTHOLE','reported',      7.6,'HIGH',    0.77,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6530,10.8022),4326),'Quận Tân Bình','Phường 1',     NOW()-'13 days'::interval, NOW()-'1 day'::interval,  5, 'raw_images/tb_001.jpg',NOW()),
('defect-tb-002','POTHOLE','reported',      5.5,'MODERATE',0.85,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6513,10.8035),4326),'Quận Tân Bình','Phường 4',     NOW()-'8 days'::interval,  NOW()-'2 days'::interval,  3, 'raw_images/tb_002.jpg',NOW()),
('defect-tb-003','POTHOLE','in_progress',   8.9,'CRITICAL',0.53,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6547,10.8009),4326),'Quận Tân Bình','Phường Sơn Kỳ',NOW()-'19 days'::interval, NOW()-'1 day'::interval,  11,'raw_images/tb_003.jpg',NOW()),
('defect-tb-004','POTHOLE','reported',      3.2,'MINOR',   0.88,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6521,10.8048),4326),'Quận Tân Bình','Phường 7',     NOW()-'5 days'::interval,  NOW()-'8 hours'::interval, 1, 'raw_images/tb_004.jpg',NOW()),
('defect-tb-005','POTHOLE','reported',      6.8,'HIGH',    0.69,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6538,10.8016),4326),'Quận Tân Bình','Phường 1',     NOW()-'11 days'::interval, NOW()-'3 days'::interval,  4, 'raw_images/tb_005.jpg',NOW()),
('defect-tb-006','POTHOLE','fixed',         4.7,'MODERATE',0.83,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6505,10.8027),4326),'Quận Tân Bình','Phường 4',     NOW()-'25 days'::interval, NOW()-'7 days'::interval,  8, 'raw_images/tb_006.jpg',NOW()),

-- ── Quận Phú Nhuận ────────────────────────────────────────────────────────────
('defect-pn-001','POTHOLE','reported',      7.5,'HIGH',    0.79,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6836,10.7998),4326),'Quận Phú Nhuận','Phường 2',    NOW()-'15 days'::interval, NOW()-'2 days'::interval,  7, 'raw_images/pn_001.jpg',NOW()),
('defect-pn-002','POTHOLE','in_progress',   5.2,'MODERATE',0.91,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6819,10.8011),4326),'Quận Phú Nhuận','Phường 10',   NOW()-'20 days'::interval, NOW()-'3 days'::interval,  9, 'raw_images/pn_002.jpg',NOW()),
('defect-pn-003','POTHOLE','reported',      8.6,'CRITICAL',0.46,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6852,10.7985),4326),'Quận Phú Nhuận','Phường 2',    NOW()-'4 days'::interval,  NOW()-'6 hours'::interval, 2, 'raw_images/pn_003.jpg',NOW()),
('defect-pn-004','POTHOLE','reported',      3.6,'MINOR',   0.87,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6827,10.7999),4326),'Quận Phú Nhuận','Phường 15',   NOW()-'7 days'::interval,  NOW()-'1 day'::interval,  2, 'raw_images/pn_004.jpg',NOW()),
('defect-pn-005','POTHOLE','reported',      6.4,'HIGH',    0.75,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.6844,10.8004),4326),'Quận Phú Nhuận','Phường 10',   NOW()-'12 days'::interval, NOW()-'2 days'::interval,  5, 'raw_images/pn_005.jpg',NOW()),

-- ── Thủ Đức ───────────────────────────────────────────────────────────────────
('defect-td-001','POTHOLE','reported',      6.7,'HIGH',    0.82,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7720,10.8505),4326),'Thủ Đức','Phường Linh Trung', NOW()-'16 days'::interval, NOW()-'2 days'::interval,  6, 'raw_images/td_001.jpg',NOW()),
('defect-td-002','POTHOLE','reported',      4.3,'MODERATE',0.70,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7703,10.8518),4326),'Thủ Đức','Phường Bình Thọ',   NOW()-'10 days'::interval, NOW()-'1 day'::interval,  3, 'raw_images/td_002.jpg',NOW()),
('defect-td-003','POTHOLE','in_progress',   9.2,'CRITICAL',0.89,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7738,10.8492),4326),'Thủ Đức','Phường Trường Thọ', NOW()-'24 days'::interval, NOW()-'1 day'::interval,  20,'raw_images/td_003.jpg',NOW()),
('defect-td-004','POTHOLE','reported',      3.0,'MINOR',   0.57,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7711,10.8530),4326),'Thủ Đức','Phường Linh Chiểu', NOW()-'3 days'::interval,  NOW()-'4 hours'::interval, 1, 'raw_images/td_004.jpg',NOW()),
('defect-td-005','POTHOLE','fixed',         7.8,'HIGH',    0.93,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7727,10.8510),4326),'Thủ Đức','Phường Linh Trung', NOW()-'27 days'::interval, NOW()-'8 days'::interval,  14,'raw_images/td_005.jpg',NOW()),
('defect-td-006','POTHOLE','reported',      5.6,'MODERATE',0.39,'[]'::jsonb, ST_SetSRID(ST_MakePoint(106.7745,10.8499),4326),'Thủ Đức','Phường Trường Thọ', NOW()-'1 day'::interval,   NOW()-'2 hours'::interval, 1, 'raw_images/td_006.jpg',NOW())

ON CONFLICT (defect_id) DO NOTHING;
