CREATE TABLE IF NOT EXISTS telemetry (
    id               SERIAL PRIMARY KEY,
    prosthesis_serial TEXT NOT NULL,
    recorded_date    DATE NOT NULL,
    avg_response_ms  FLOAT NOT NULL,
    movement_count   INT NOT NULL
);

INSERT INTO telemetry (prosthesis_serial, recorded_date, avg_response_ms, movement_count) VALUES
    ('SN-001', CURRENT_DATE - 1, 82.5,  120),
    ('SN-001', CURRENT_DATE,     79.3,  135),
    ('SN-002', CURRENT_DATE - 1, 91.0,  98),
    ('SN-002', CURRENT_DATE,     88.7,  110),
    ('SN-003', CURRENT_DATE - 1, 75.2,  150),
    ('SN-003', CURRENT_DATE,     77.8,  142);
