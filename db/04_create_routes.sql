CREATE TABLE routes (
    id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(id) ON DELETE SET NULL,
    origin TEXT,
    destination TEXT,
    distance_km NUMERIC,
    status TEXT CHECK (status IN ('planned', 'active', 'completed')) DEFAULT 'planned',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
