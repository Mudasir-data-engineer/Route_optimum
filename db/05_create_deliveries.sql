CREATE TABLE deliveries (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    route_id INTEGER REFERENCES routes(id) ON DELETE SET NULL,
    delivery_address TEXT NOT NULL,
    scheduled_date DATE NOT NULL,
    status TEXT CHECK (status IN ('pending', 'in_transit', 'delivered')) DEFAULT 'pending',
    package_weight FLOAT NOT NULL,
    delivery_urgency TEXT CHECK (delivery_urgency IN ('low', 'medium', 'high')) DEFAULT 'medium',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
