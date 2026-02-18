Note : https://www.markdownguide.org/cheat-sheet/

# SQL for Data Engineers


### Foundational Skills
1. **Querying & Filtering**: Mastering SELECT statements for exploration and WHERE clauses to isolate relevant records from massive datasets.
2. **Joins & Set Ops**: Using INNER, LEFT, and FULL OUTER JOINs to integrate disparate data sources, alongside UNION/UNION ALL to combine results.
3. **Aggregations**: Summarizing data using GROUP BY paired with functions like SUM, AVG, and COUNT to derive KPIs.
4. **Data Control & Transactions**: Using GRANT/REVOKE for security and COMMIT/ROLLBACK to ensure atomicity in complex pipeline updates. 

### Advanced Techniques for Pipelines
1. **Common Table Expressions (CTEs)**: Modularizing logic into readable blocks with WITH clauses, including recursive CTEs for hierarchical data.
2. **Window Functions**: Performing complex analytical tasks like running totals or moving averages using ROW_NUMBER, RANK, and LAG/LEAD without collapsing rows.
3. **Query Optimization**: Leveraging EXPLAIN plans to identify bottlenecks and applying indexing strategies (clustered vs. non-clustered) to speed up retrieval.
4. **Stored Procedures & Triggers**: Encapsulating frequent tasks and automating integrity checks directly within the database engine. 

### Core Engineering Tasks
1. **ETL/ELT Design**: Writing SQL-based transformations (e.g., using tools like dbt) to move data from raw landing zones to analytical schemas.
2. **Data Modeling**: Designing fact and dimension tables and managing Slowly Changing Dimensions (SCDs) for historical tracking.
3. **Data Quality & Auditing**: Creating automated validation queries to catch NULL values, duplicates, or out-of-range anomalies before they reach production
