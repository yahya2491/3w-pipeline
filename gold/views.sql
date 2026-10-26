-- Gold views for operations engineers. They are computed on demand from the published
-- recordings and event_phases tables, so they always agree with them.
-- Metric definitions are in docs/gold/GOLD_CONTRACT.md. Statements are separated by semicolons.

-- One row per recording: when its event started and how it developed.
CREATE OR REPLACE TEMP VIEW recording_event_timing AS
SELECT
    r.recording_id,
    r.well_id,
    r.source_type,
    r.event_id,
    r.event_name,
    r.inferred_label_share,
    -- normal lead time: recording start to the first transient or steady row
    MIN(CASE WHEN p.phase IN ('transient', 'steady') THEN p.start_offset_s END) AS normal_lead_s,
    -- time to steady: first transient row to first steady row
    MIN(CASE WHEN p.phase = 'steady' THEN p.start_offset_s END)
        - MIN(CASE WHEN p.phase = 'transient' THEN p.start_offset_s END) AS time_to_steady_s,
    -- event duration: first to last transient or steady row
    MAX(CASE WHEN p.phase IN ('transient', 'steady') THEN p.end_offset_s END)
        - MIN(CASE WHEN p.phase IN ('transient', 'steady') THEN p.start_offset_s END) AS event_duration_s
FROM recordings r
JOIN event_phases p ON p.recording_id = r.recording_id
GROUP BY r.recording_id, r.well_id, r.source_type, r.event_id, r.event_name, r.inferred_label_share;

-- Real wells only: which events each well had and how they developed.
CREATE OR REPLACE TEMP VIEW well_event_summary AS
SELECT
    well_id,
    event_id,
    event_name,
    COUNT(*) AS recordings,
    ROUND(AVG(normal_lead_s) / 60, 1) AS avg_normal_lead_min,
    ROUND(AVG(time_to_steady_s) / 60, 1) AS avg_time_to_steady_min,
    ROUND(SUM(event_duration_s) / 3600, 1) AS total_event_hours,
    ROUND(AVG(inferred_label_share), 3) AS avg_inferred_label_share
FROM recording_event_timing
WHERE source_type = 'REAL'
GROUP BY well_id, event_id, event_name
ORDER BY well_id, event_id;

-- Every event type, with real, simulated and drawn recordings kept apart.
CREATE OR REPLACE TEMP VIEW event_type_summary AS
SELECT
    event_id,
    event_name,
    source_type,
    COUNT(*) AS recordings,
    ROUND(AVG(normal_lead_s) / 60, 1) AS avg_normal_lead_min,
    ROUND(AVG(time_to_steady_s) / 60, 1) AS avg_time_to_steady_min,
    ROUND(AVG(event_duration_s) / 60, 1) AS avg_event_duration_min,
    ROUND(AVG(inferred_label_share), 3) AS avg_inferred_label_share
FROM recording_event_timing
GROUP BY event_id, event_name, source_type
ORDER BY event_id, source_type;
