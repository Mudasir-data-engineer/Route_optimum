CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    plate_number VARCHAR(50) UNIQUE NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    driver_id INT UNIQUE REFERENCES users(id) ON DELETE SET NULL,
    status VARCHAR(50) DEFAULT 'available' CHECK (status IN ('available','on_route','maintenance'))
);
