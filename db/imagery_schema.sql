CREATE SCHEMA IF NOT EXISTS imagery;

CREATE TABLE IF NOT EXISTS imagery.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    run_started_at TIMESTAMPTZ,
    mode VARCHAR,
    status VARCHAR,
    year INTEGER,
    zoom INTEGER,
    building_count INTEGER,
    distinct_tile_count INTEGER,
    raw_tile_bytes_estimate BIGINT,
    cropped_output_bytes_estimate BIGINT,
    peak_working_bytes_estimate BIGINT,
    disk_cap_bytes BIGINT
);

CREATE TABLE IF NOT EXISTS imagery.tile_manifest (
    run_id VARCHAR,
    tile_key VARCHAR,
    year INTEGER,
    zoom INTEGER,
    x INTEGER,
    y INTEGER,
    planned_url VARCHAR
);

CREATE TABLE IF NOT EXISTS imagery.bbl_crops (
    run_id VARCHAR,
    bbl VARCHAR,
    output_path VARCHAR,
    center_tile_key VARCHAR,
    tile_count INTEGER,
    crop_left INTEGER,
    crop_top INTEGER,
    crop_right INTEGER,
    crop_bottom INTEGER,
    crop_width INTEGER,
    crop_height INTEGER
);
