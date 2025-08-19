CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    location GEOGRAPHY(POINT, 4326),  -- PostGIS field for lat/long
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for spatial queries
CREATE INDEX IF NOT EXISTS idx_customers_location ON customers USING GIST(location);
