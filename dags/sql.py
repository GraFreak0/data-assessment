# sql.py
query_trip_data = """
SELECT 
    DATE_FORMAT(pickup_date, '%Y-%m') AS month,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN number_of_trips ELSE NULL END), 1) AS sat_mean_trip_count,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN fare_amount ELSE NULL END), 1) AS sat_mean_fare_per_trip,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN duration ELSE NULL END), 1) AS sat_mean_duration_per_trip,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN number_of_trips ELSE NULL END), 1) AS sun_mean_trip_count,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN fare_amount ELSE NULL END), 1) AS sun_mean_fare_per_trip,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN duration ELSE NULL END), 1) AS sun_mean_duration_per_trip
FROM (
    SELECT 
        pickup_date,
        COUNT(*) AS number_of_trips,
        AVG(fare_amount) AS fare_amount,
        AVG(TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime)) AS duration
    FROM tripdata
    WHERE pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
        AND (DAYOFWEEK(pickup_date) = 7 OR DAYOFWEEK(pickup_date) = 1)
    GROUP BY pickup_date
) AS Trips
GROUP BY month
ORDER BY month;
"""