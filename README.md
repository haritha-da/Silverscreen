# 🎥 Silver Screen Movie Theater Performance Analysis

📅 Project Overview

Silver Screen operates three movie theaters in New Jersey and aims to understand the relationship between movie rental costs and revenue generated from ticket sales.

This project builds a dbt Cloud pipeline to analyze monthly movie performance at each location, ensuring standardized, cleaned, and aggregated data across inconsistent formats from three locations: NJ_001, NJ_002, and NJ_003.

📅 Objective

The goal of this project is to create a summary table that includes:

Movie details

Location information

Monthly rental costs for each movie

Total ticket sales per movie per location

Total revenue from ticket sales per movie per location

🔍 Key Challenge

Each theater location records data differently, requiring cleaning, standardization, and aggregation before constructing the final model.

⚙️ dbt Cloud Structure and Model Execution 

💻 dbt Cloud Directory & Execution Flow

Our dbt Cloud models are structured in layers:

1️⃣ Staging Models

stg_nj_001, stg_nj_002, stg_nj_003

Standardizes and cleans raw data

Extracts month from timestamps

Renames columns for consistency

2️⃣ Intermediate Models

int_movie_sales

Merges all locations using UNION ALL

Filters out non-ticket sales

Extracts movie_id from NJ_003’s details column

3️⃣ Final Model

final_silver_screen_table

Aggregates all ticket sales by movie and month

Ensures a consistent structure across locations

📈 Execution Flow in dbt Cloud

Models execute sequentially, following dependencies set in the dbt lineage.

📊 Lineage Diagram and Key Transformations (4:00 - 6:00)

👩‍🎓 dbt Cloud Lineage Diagram

Shows how data moves through transformations:

💡 Data Flow:

Raw Sources: source('silver_screen', 'nj_001'), etc.

Staging Models: stg_nj_001, etc.

Intermediate Models: int_movie_sales

Final Model: final_silver_screen_table

🌟 Key Transformations

✅ Column Standardization – Unifying column names (e.g., ticket_amount, transaction_total).
✅ Data Aggregation – Converting daily data (NJ_002) to monthly level.
✅ Filtering & Cleaning – Retaining only movie ticket sales in NJ_003.

💡 Result: A clean, structured dataset ready for business analysis.

⚙️ Testing and Automation in dbt (6:00 - 8:00)

Ensuring data quality with built-in and custom tests in dbt Cloud.

📊 Built-in dbt Tests

✔️ not_null – Ensures movie_id, movie_title, and month are always populated.
✔️ unique – Ensures movie_id + month + location is unique.

🌟 Custom Tests (tests/custom_tests.sql)

✔️ Validate Data Integrity: Ensures rental_cost and revenue are non-negative.

👩‍🎓 Automated Testing in dbt Cloud

Runs tests after every model execution

Maintains data integrity before updates are published

🌟 Conclusion 

📊 Final Insights

Built a robust dbt model to unify and analyze movie performance across multiple locations.

Ensured data consistency through structured transformations and dbt Cloud dependencies.

Automated data quality checks to maintain reliable insights.

🛠️ Impact for Silver Screen:

✅ Supports data-driven decision-making
✅ Provides reliable, standardized revenue analysis
✅ Enables executives to optimize movie selection and pricing strategies
