# Data Warehouse Incremental Load SQL Query

## Overview
This project involves constructing an SQL query for an ELT (Extract, Load, Transform) pipeline that incrementally loads data from PostgreSQL databases into a Redshift data warehouse. The data is sourced from three relational tables: `orders`, `transactions`, and `verification`, and is integrated into a data mart named `purchases`. The query ensures data integrity and completeness, handling new inserts and updates while avoiding unnecessary processing of unchanged records.

## Task Requirements
- **Source Tables**: `orders`, `transactions`, `verification`
- **Target Table**: `purchases` (in Redshift)
- **Data Characteristics**:
  - Data is incrementally loaded from PostgreSQL to Redshift.
  - Each table includes `created_at`, `updated_at`, and `uploaded_at` timestamps.
  - Data changes include ~70% new rows and ~30% updated rows per load.
  - Relationships between tables are 1:1.
  - The `purchases` table includes an `uploaded_at` timestamp indicating the last record insertion or update.
- **Query Requirements**:
  - The query should select only records that need to be inserted or updated.
  - Ensure data completeness by avoiding partial records (no `NULL` values for joined tables).
  - The query should be optimized for performance in a large-scale data environment.

## Thought Process
To determine which records should be inserted, updated, or left unchanged in the `purchases` table, I followed these principles:

### 1. Identifying New Records (Insert)
A record is considered new if any of the `orders`, `transactions`, or `verification` entries were created after the last processed timestamp in the `purchases` table. This is determined by comparing the `created_at` and `updated_at` timestamps:

- If `created_at = updated_at` and `uploaded_at > last_processed_timestamp`, it indicates a new record that should be inserted.

### 2. Identifying Updated Records (Update)
A record is identified as needing an update if any of the related entries were modified after their initial creation and after the last processed timestamp:

- If `created_at < updated_at` and `uploaded_at > last_processed_timestamp`, it indicates an updated record that should be processed.

### 3. Ignoring Unchanged Records
Records that have not changed since the last processing should be excluded from the query to optimize performance:

- If `uploaded_at <= last_processed_timestamp`, no action is needed as the data is already up-to-date in the `purchases` table.

### 4. Ensuring Data Completeness
To maintain data integrity, I used `INNER JOIN` for all table relationships. This ensures that only complete records with corresponding entries in `orders`, `transactions`, and `verification` are selected. The task explicitly requires avoiding partial records with `NULL` values.
