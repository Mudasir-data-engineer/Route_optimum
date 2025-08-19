-- Sample data for deliveries table
INSERT INTO deliveries (customer_id, route_id, scheduled_date, status, priority, delivery_location) VALUES
(1, 1, '2025-08-20', 'pending', 'high', ST_GeogFromText('POINT(-73.935242 40.730610)')),
(2, 2, '2025-08-21', 'in_transit', 'normal', ST_GeogFromText('POINT(-74.005941 40.712784)')),
(3, 3, '2025-08-22', 'delivered', 'low', ST_GeogFromText('POINT(-73.985428 40.748817)'));
