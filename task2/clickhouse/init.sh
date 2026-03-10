#!/bin/bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS reports;"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS reports.user_reports
(
    username          String,
    prosthesis_serial String,
    report_date       Date,
    avg_response_ms   Float64,
    movement_count    UInt32,
    processed_at      DateTime
) ENGINE = MergeTree()
ORDER BY (username, report_date);
"
