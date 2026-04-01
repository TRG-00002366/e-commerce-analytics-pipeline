# Data Governance for Amazon E-Commerce Analytics Pipeline

## 1. Data Classification

This project uses a tiered classification model to protect data in transit and at rest.

- Public
  - Aggregated reports (e.g., total revenue by region, volumes)
  - Non-sensitive metrics that can be shared outside the org
- Internal
  - Raw pipeline metadata (job status, error logs, lineage IDs)
  - Non-PII customer segmentation data (REGULAR / PRIME / VIP)
- Confidential
  - Order-level data containing buyer product selection, unit prices, discounts, event timestamps
  - Customer IDs and region codes
- Restricted
  - Personally identifiable information (PII) and controls around PII storage/access
  - Credentials and secrets (Snowflake connection strings, Kafka access keys)

## 2. PII Inventory and Masking Policies

### 2.1 PII data elements
- `customer_id` (pseudo-ID in generated flows)
- `order_id` (unique transaction reference)
- `product_id` (product catalog key)
- `region` (geography identifier, may be sensitive at fine granularity)

### 2.2 Masking policies
- Apply masking in Silver and Gold models for safe outputs:
  - `customer_id` -> `left(customer_id, 6) || '****'` (or `hash(customer_id)`)
  - `order_id` -> `left(order_id, 8) || '****'`
- Use Snowflake Dynamic Data Masking for sensitive queries:
  - `CREATE MASKING POLICY customer_id_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('ANALYTICS_ROLE', 'ACCOUNTADMIN') THEN val ELSE 'ANON_CUSTOMER' END;`
  - Apply via `ALTER TABLE GOLD.FACT_ORDERS ALTER COLUMN customer_id SET MASKING POLICY customer_id_mask;`
- Avoid storing any raw customer financial credentials.

### 2.3 PII classification in tables
- Bronze: `orders.raw_data` is VARIANT; treat as confidential unless explicitly filtered.
- Silver: structured columns including `customer_id`, `order_id`, `region` carry robustness rules and masking
- Gold: dimension and fact tables follow least-privilege and masked-view controls

## 3. RBAC Matrix

Use Snowflake roles and grants to enforce least privilege.

### 3.1 Recommended roles
- `ACCOUNTADMIN` (setup only)
- `SYSADMIN` (infrastructure deployment)
- `ANALYTICS_DEV` (dbt authoring, transformations)
- `ANALYTICS_READ` (reporting/dashboards)
- `STAGING_OPERATOR` (pipeline jobs ingest/copy)

### 3.2 Schema/table access matrix

- `ACCOUNTADMIN`
  - full access to `AMAZON_DB.*`
- `STAGING_OPERATOR`
  - `USE WAREHOUSE COMPUTE_WH`, `USE DATABASE AMAZON_DB`
  - `SELECT`, `INSERT`, `UPDATE`, `TRUNCATE` on `BRONZE.*`, `SILVER.*`
- `ANALYTICS_DEV`
  - `USAGE` on `DATABASE AMAZON_DB`, `SCHEMA GOLD`
  - `SELECT`/`INSERT`/`UPDATE` on `GOLD.*`
  - `SELECT` on `SILVER.*` for data lineage
- `ANALYTICS_READ`
  - `USAGE` on `DATABASE AMAZON_DB`, `SCHEMA GOLD`
  - `SELECT` on star schema tables (e.g., `GOLD.FACT_ORDERS`, `GOLD.DIM_*`)
  - no direct access to `BRONZE.*` or raw fields

### 3.3 dbeaver/BI role notes
- use secure views for users who should not see raw `customer_id` or `region` details.

## 4. Retention Policies

- Bronze raw data: retain 90 days (or minimum business requirement), auto-purged.
- Silver cleansed data: retain 180 days.
- Gold aggregated/public facts: retain 365 days, plus rolling archive to object storage if required.
- Implementation pattern (Snowflake Task):
  - daily job to `DELETE FROM BRONZE.ORDERS WHERE ingestion_timestamp < DATEADD(day, -90, CURRENT_DATE());`
  - similarly for silver/gold depending legal requirements.

## 5. GDPR / Privacy Compliance

### 5.1 Data subject rights
- Right to erasure (`DELETE`) from persistent tables:
  - mapping: `customer_id` (level key) → delete from `GOLD.FACT_ORDERS`, `GOLD.DIM_CUSTOMER`, plus downstream silver if required.
  - preserve audit logs of deletion actions in a secure audit table.
- Right to portability (`SELECT`) for requested record sets in machine-readable format (CSV/JSON extraction).

### 5.2 Data minimization
- Keep only fields required for analytics. Drop or tokenize any elements not required.

### 5.3 Consent and tracking
- If integrating real customer data from production, store consent flags separately and filter ingestion pipelines by consent status.

### 5.4 Audit logging
- Maintain an audit table (e.g., `GOVERNANCE.AUDIT_EVENTS`) for:
  - data access requests
  - deletion/portability operations
  - schema changes and role assignments

## 6. Implementation checklist

1. Create Snowflake roles and grant matrix with SQL.
2. Define and apply masking policy on PII columns.
3. Create schema-level policies for retention (Tasks + Streams).
4. Build configured factoring job in Airflow for `governance_purge` task.
5. Document and test privacy workflows (erasure + portability) in README.
