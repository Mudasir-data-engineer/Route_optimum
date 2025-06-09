# Deliveries Table - Data Dictionary

| Field            | Type           | Description                                | Example                   |
|------------------|----------------|--------------------------------------------|---------------------------|
| id               | SERIAL (PK)    | Unique delivery identifier                   | 1                         |
| customer_id      | INTEGER (FK)   | Customer who requested the delivery          | 3                         |
| route_id         | INTEGER (FK)   | Route the delivery is assigned to (nullable) | 1                         |
| delivery_address | TEXT           | Address where delivery should be made         | "456 Elm St, Springfield" |
| scheduled_date   | DATE           | Date the delivery is scheduled for            | "2025-06-07"              |
| status           | VARCHAR(50)    | Current delivery status (pending, delivered, failed) | "pending"         |
| package_weight   | FLOAT          | Weight of the package in kg                    | 5.4                       |
| created_at       | TIMESTAMP      | Timestamp when delivery record was created    | "2025-06-04 16:15:00"     |
