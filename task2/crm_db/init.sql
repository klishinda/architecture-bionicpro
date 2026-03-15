CREATE TABLE IF NOT EXISTS patients (
    id               SERIAL PRIMARY KEY,
    username         TEXT NOT NULL UNIQUE,
    prosthesis_serial TEXT NOT NULL
);

INSERT INTO patients (username, prosthesis_serial) VALUES
    ('prothetic1', 'SN-001'),
    ('prothetic2', 'SN-002'),
    ('prothetic3', 'SN-003');
