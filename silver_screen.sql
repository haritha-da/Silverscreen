—models/
Movie_catalogue_clean.sql


SELECT
   movie_id,
   movie_title,
   COALESCE(genre, 'Unknown') AS genre,
   studio
FROM {{ source('silver_screen', 'movie_catalogue') }}


-–models/
Stg_Nj_001.sql


SELECT
   movie_id,
   DATE_TRUNC('month', timestamp) AS month,
   'NJ_001' AS location,
   SUM(ticket_amount) AS tickets_sold,
   SUM(transaction_total) AS revenue
FROM {{ source('silver_screen', 'nj_001') }}
GROUP BY movie_id, month, location


-–models/
Stg_Nj002.sql


SELECT
   movie_id,
   DATE_TRUNC('month', date) AS month,
   'NJ_002' AS location,
   SUM(ticket_amount) AS tickets_sold,
   SUM(total_earned) AS revenue
FROM {{ source('silver_screen', 'nj_002') }}
GROUP BY movie_id, month, location


-–models/
Stg_Nj003.sql
SELECT
   details AS movie_id,
   DATE_TRUNC('month', timestamp) AS month,
   'NJ_003' AS location,
   SUM(amount) AS tickets_sold,
   SUM(total_value) AS revenue
FROM {{ source('silver_screen', 'nj_003') }}
WHERE product_type = 'ticket'
GROUP BY movie_id, month, location






–models/
invoices.sql


SELECT
   movie_id,
   location_id AS location,
   DATE_TRUNC('month', month) AS month,
   SUM(total_invoice_sum) AS rental_cost
FROM {{ source('silver_screen', 'invoices') }}
GROUP BY movie_id, location, month


–models/
Unify_locations.sql


{{ config(materialized='table')}}
WITH nj_001 AS (
   SELECT * FROM {{ ref('nj001') }}
),
nj_002 AS (
   SELECT * FROM {{ ref('nj002') }}
),
nj_003 AS (
   SELECT * FROM {{ ref('nj003') }}
)


SELECT * FROM nj_001
UNION ALL
SELECT * FROM nj_002
UNION ALL
SELECT * FROM nj_003


-–models/
Final_movie_performance.sql


{{ config(materialized='table')}}
WITH sales AS (
   SELECT * FROM {{ ref('unify_location') }}
),
movies AS (
   SELECT * FROM {{ ref('movie_catalogue_clean') }}
),
invoices AS (
   SELECT * FROM {{ ref('invoices') }}
)


SELECT
   sales.movie_id,
   movies.movie_title,
   movies.genre,
   movies.studio,
   sales.month,
   sales.location,
   COALESCE(invoices.rental_cost, 0) AS rental_cost,
   sales.tickets_sold,
   sales.revenue
FROM sales
LEFT JOIN movies ON sales.movie_id = movies.movie_id
LEFT JOIN invoices
   ON sales.movie_id = invoices.movie_id
   AND sales.month = invoices.month
   AND sales.location = invoices.location


–tests/
Test_non_negative_values.sql
-- Check if rental_cost and revenue are non-negative
SELECT *
FROM {{ ref('final_movie_performance') }}
WHERE revenue < 0 OR rental_cost < 0
