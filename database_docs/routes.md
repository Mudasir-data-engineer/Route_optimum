# Routes Table - Data Dictionary

| Field         | Type           | Description                                | Example                   |
|---------------|----------------|--------------------------------------------|---------------------------|
| id            | SERIAL (PK)    | Unique route identifier                     | 1                         |
| vehicle_id    | INTEGER (FK)   | Vehicle assigned to the route                | 2                         |
| origin        | TEXT           | Starting point of the route                   | "Warehouse A"             |
| destination   | TEXT           | Ending point of the route                     | "Customer B"              |
| distance_km   | FLOAT          | Total distance covered (km)                   | 23.5                      |
| status        | VARCHAR(50)    | Route status (planned, active, completed)     | "planned"                 |
| created_at    | TIMESTAMP      | Timestamp when route record was created       | "2025-06-04 16:00:00"     |
