CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    vehicle_id INT REFERENCES vehicles(id) ON DELETE SET NULL,
    driver_id INT REFERENCES users(id) ON DELETE SET NULL,
    status VARCHAR(50) DEFAULT 'planned' CHECK (status IN ('planned','in_progress','completed')),
    route_path GEOGRAPHY(LINESTRING, 4326), -- PostGIS field for full route geometry
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for status lookups
CREATE INDEX IF NOT EXISTS idx_routes_status ON routes(status);
