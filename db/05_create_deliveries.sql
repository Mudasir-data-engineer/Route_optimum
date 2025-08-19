CREATE TABLE IF NOT EXISTS deliveries (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    route_id INT REFERENCES routes(id) ON DELETE SET NULL,
    scheduled_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending','in_transit','delivered','failed')),
    priority VARCHAR(50) DEFAULT 'normal' CHECK (priority IN ('low','normal','high')),
    delivery_location GEOGRAPHY(POINT, 4326), -- PostGIS field for delivery location
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
CREATE INDEX IF NOT EXISTS idx_deliveries_priority ON deliveries(priority);
