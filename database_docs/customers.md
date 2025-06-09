# Customers Table - Data Dictionary

| Field          | Type            | Description                               | Example                         |
|----------------|-----------------|-------------------------------------------|--------------------------------|
| id             | SERIAL (PK)     | Unique ID for each customer               | 1                              |
| user_id        | INTEGER (FK)    | Linked user ID (optional)                 | 3                              |
| company_name   | VARCHAR(255)    | Customer’s company name                    | "Acme Corp"                    |
| contact_name   | VARCHAR(255)    | Contact person’s full name                 | "John Smith"                   |
| email          | VARCHAR(255)    | Contact email address                      | "john@acme.com"                |
| phone          | VARCHAR(20)     | Phone number                              | "+1-202-555-0198"              |
| address        | TEXT            | Delivery address                          | "123 Main St, Springfield"     |
| created_at     | TIMESTAMP       | Timestamp when record was created          | "2025-06-04 15:12:00"          |
