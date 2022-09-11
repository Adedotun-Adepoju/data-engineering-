-- Joins:
-- method 1
SELECT 
	tpep_pickup_datetime, 
	tpep_dropoff_datetime,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zpu."Zone") AS dropoff_loc
FROM public.yellow_taxi_data t ,
	 public.zones zpu,
	 public.zones zdo
WHERE 
	t."PULocationID" = zpu."LocationID" AND 
	t."DOLocationID" = zdo."LocationID"
LIMIT 100;

--method 2
SELECT 
	tpep_pickup_datetime, 
	tpep_dropoff_datetime,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zpu."Zone") AS dropoff_loc
FROM public.yellow_taxi_data t 
	JOIN public.zones zpu ON t."PULocationID" = zpu."LocationID"
	JOIN public.zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;
