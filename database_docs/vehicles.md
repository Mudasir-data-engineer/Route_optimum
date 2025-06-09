# Vehicles Table - Data Dictionary

| Field         | Type           | Description                                | Example                   |
|---------------|----------------|--------------------------------------------|---------------------------|
| id            | SERIAL (PK)    | Unique vehicle identifier                   | 1                         |
| plate_number  | VARCHAR(20)    | Unique license plate number                  | "XYZ-1234"                |
| type          | VARCHAR(50)    | Vehicle type (van, bike, truck, etc.)       | "Van"                     |
| capacity      | INTEGER        | Maximum load capacity (weight or volume)    | 1000                      |
| status        | VARCHAR(50)    | Current vehicle status                        | "available"               |
| created_at    | TIMESTAMP      | Timestamp when vehicle record was created   | "2025-06-04 15:30:00"     |
