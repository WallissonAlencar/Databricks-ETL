CREATE OR REFRESH STREAMING TABLE silver_orders_enriched 
AS
SELECT
  
  CAST(order_id AS STRING) AS order_id,
  CAST(order_date AS TIMESTAMP) AS order_date,
  CAST(total_amount AS DECIMAL(10,2)) AS total_amount,

  CAST(user_key AS STRING) AS user_key,
  CAST(driver_key AS STRING) AS driver_key,
  CAST(restaurant_key AS STRING) AS restaurant_key,
  CAST(payment_key AS STRING) AS payment_key,
  CAST(rating_key AS STRING) AS rating_key,

  YEAR(order_date) AS order_year,
  MONTH(order_date) AS order_month,
  DAY(order_date) AS order_day,
  HOUR(order_date) AS order_hour,
  MINUTE(order_date) AS order_minute,
  DAYOFWEEK(order_date) AS day_of_week,

    CASE DAYOFWEEK(order_date)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    ELSE 'Saturday'
  END AS day_name,

    CASE
    WHEN DAYOFWEEK(order_date) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend,

    CASE
    WHEN total_amount < 20 THEN 'Low'
    WHEN total_amount < 50 THEN 'Medium'
    ELSE 'High'
  END AS amount_category,

    CASE
    WHEN HOUR(order_date) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN HOUR(order_date) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN HOUR(order_date) BETWEEN 18 AND 22 THEN 'Evening'
    ELSE 'Night'
  END AS time_of_day,

    CASE
    WHEN HOUR(order_date) BETWEEN 11 AND 14 OR HOUR(order_date) BETWEEN 18 AND 21 THEN TRUE
    ELSE FALSE
  END AS is_peak_hour,
  
  QUARTER(order_date) AS order_quarter,

    source_file,
  ingestion_time,

    CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version

FROM STREAM bronze_orders;
