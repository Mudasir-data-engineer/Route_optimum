CREATE TABLE vehicles (
    id SERIAL PRIMARY KEY,
    plate_number TEXT UNIQUE NOT NULL,
    type TEXT,
    capacity INTEGER NOT NULL,
    status TEXT CHECK (status IN ('available', 'active', 'inactive')) DEFAULT 'available',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
);
