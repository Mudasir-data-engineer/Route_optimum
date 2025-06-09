# Users Table - Data Dictionary

| Field          | Type           | Description                                | Example                   |
|----------------|----------------|--------------------------------------------|---------------------------|
| id             | SERIAL (PK)    | Unique user identifier                      | 1                         |
| username       | VARCHAR(150)   | Unique username for login                    | "jsmith"                  |
| email          | VARCHAR(255)   | User’s email address                         | "jsmith@example.com"      |
| password      | VARCHAR(128)   | Hashed password                             | (hashed string)           |
| role           | VARCHAR(50)    | Role of the user (admin, driver, customer)  | "customer"                |
| contact_info   | VARCHAR(255)   | Additional contact information               | "+1-202-555-0123"         |
| is_active      | BOOLEAN        | User active status                           | true                      |
| date_joined    | TIMESTAMP      | When the user account was created            | "2025-06-01 12:00:00"     |
