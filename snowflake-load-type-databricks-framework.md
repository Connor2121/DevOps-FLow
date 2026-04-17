# Snowflake load type — Databricks framework

[[_TOC_]]

## Overview

This page documents the Snowflake load type for our Databricks ingestion framework. It is intended for two audiences:

- **Framework users** (data engineers, analysts) who want to configure a pipeline that reads from Snowflake into our Databricks environment.
- **Network and security engineers** who need to understand how data physically moves between Snowflake and Databricks, and why our configuration uses private endpoints end-to-end.

Our Snowflake environment is **read-only** from the Databricks side. This documentation describes the read path exclusively — we do not write data back to Snowflake.

## What the Databricks Snowflake connector actually does

When framework code executes something like:

```python
df = (spark.read
    .format("snowflake")
    .options(**sfOptions)
    .option("query", "SELECT ... FROM INVESTMENTS.DECODE WHERE year = 2025")
    .load())
```

…the `.format("snowflake")` directive is not a direct pipe from Spark to Snowflake. It is an orchestration across three distinct components, each playing a specific role.

### The three components

**1. Spark driver (Databricks).** Opens a JDBC session to Snowflake, authenticates with the configured role, and sends SQL. The driver never sees bulk data — it only handles control traffic.

**2. Snowflake warehouse.** Executes the SQL (after the connector applies query pushdown — see below). Instead of streaming results back over JDBC, the warehouse issues a `COPY INTO '@stage/'` statement that unloads the result as compressed files to a staging area in cloud storage.

**3. Spark executors (Databricks).** Read the staged files directly from cloud storage in parallel, bypassing Snowflake's query engine entirely. The warehouse has already finished its work by the time executors start reading.

### Why this architecture exists

If Spark executors wrote rows directly to Snowflake (or read rows directly through JDBC), bulk operations would be unusably slow. JDBC is designed for transactional traffic, not bulk data transfer. By staging data through cloud storage:

- Snowflake parallelizes the unload across many workers.
- Spark parallelizes the read across many executors.
- Data moves at cloud storage speeds, not JDBC speeds.
- Neither system waits on the other's query engine.

This is the same pattern used by most modern data warehouse-to-Spark connectors (Redshift, BigQuery, Synapse all work similarly).

### Query pushdown — the most important optimization

The Spark Snowflake connector registers Catalyst rules that rewrite Spark DataFrame operations into a single SQL query executed in Snowflake. What you write in Spark is *not* what runs in Snowflake.

Example. If framework code does:

```python
df.filter(col("amount") > 1000).groupBy("region").sum("amount")
```

…the connector rewrites it to roughly:

```sql
SELECT region, SUM(amount) 
FROM (<original_query>)
WHERE amount > 1000 
GROUP BY region
```

Only the aggregated result is unloaded to the stage. Without pushdown, Spark would pull every row and aggregate locally — unusable for large tables.

**Operations that push down cleanly:** `filter` / `where`, `select`, `groupBy` with standard aggregations (`sum`, `count`, `avg`, `min`, `max`), `orderBy`, `limit`, joins between two Snowflake DataFrames from the same session, simple expressions and casts.

**Operations that break pushdown (execute in Spark after loading):** Python UDFs, joins between a Snowflake DataFrame and a non-Snowflake DataFrame (e.g., joining Snowflake with Delta), any function that doesn't have a direct Snowflake equivalent. Once pushdown is broken at a given point in the plan, everything downstream runs in Spark.

**How to verify pushdown is working:** Run `df.explain()` on any pipeline. Look for `SnowflakeRelation` in the physical plan — it will show the SQL that was actually sent. If the SQL includes your filters, projections, and aggregations inline, pushdown succeeded. If you see `SELECT *` followed by Spark operations, it did not.

**Reference:** [Using the Spark Connector — Pushdown](https://docs.snowflake.com/en/user-guide/spark-connector-use#pushdown)

## Internal stage (default behavior — not what we use)

When no staging location is configured, the connector uses a **Snowflake-managed internal stage**. This is the out-of-the-box behavior and is documented for awareness, but **our framework does not use this path** because it does not meet our network security requirements.

### How it works

The staging area is a user stage (`@~`) in cloud storage owned and managed by Snowflake, in the same cloud provider and region as the Snowflake account. Files land in a path like `@~/spark_connector_<session_id>/<query_id>/`.

Spark executors receive short-lived pre-signed URLs (on AWS) or SAS tokens (on Azure) from Snowflake, and use those to fetch the staged files directly from Snowflake's storage over HTTPS.

### Diagram — internal stage flow

*Embed the following SVG in the wiki page. Upload `snowflake_internal_stage_default_flow.svg` as an attachment via the wiki's attach-file feature, then reference it below.*

```
![Internal stage flow](/.attachments/snowflake_internal_stage_default_flow.svg)
```

### Why we do not use the internal stage path

1. **Bulk data traffic crosses our network boundary.** Executors connect to Snowflake-owned storage endpoints over public TLS. This bypasses our Azure Private Link configuration for storage.
2. **No visibility into staging data.** The Snowflake-owned bucket is not visible in our Azure portal, not subject to our lifecycle policies, and not covered by our monitoring.
3. **Our egress policy blocks it.** Databricks clusters in our workspace have egress restrictions that prevent reaching Snowflake's cloud storage endpoints. Using the default internal-stage path would fail immediately.

**Reference:** [Snowflake — Azure private endpoints for internal stages](https://docs.snowflake.com/en/user-guide/private-internal-stages-azure) (relevant if we ever wanted to use internal stages with private connectivity).

## External stage — our implementation

Our framework uses a **Snowflake external stage pointing at an ADLS Gen2 container in our Azure subscription**, with private endpoints on both ends. All bulk data transfer stays within our network perimeter.

### Infrastructure prerequisites (already provisioned)

The following components were set up by our Snowflake admins, Azure admins, and network team. They are documented here so users understand the stack; no user action is required.

#### Snowflake side

1. **Storage integration** — a Snowflake object that stores a managed identity our Azure admins granted write access to. Created with `CREATE STORAGE INTEGRATION ... USE_PRIVATELINK_ENDPOINT = TRUE`.
2. **External stage object** — references the storage integration and points at our ADLS container. Created with `CREATE STAGE ... STORAGE_INTEGRATION = ...`.
3. **Private connectivity endpoint** — provisioned on the Snowflake side using `SYSTEM$PROVISION_PRIVATELINK_ENDPOINT`, approved by our Azure admins in the Azure portal.
4. **Role grants** — our Databricks service account role has:
   - `USAGE` on the database and schema
   - `SELECT` on the source tables/views
   - `USAGE` on the warehouse
   - `USAGE` and `WRITE` on the external stage object (the `WRITE` is required so Snowflake can unload result files into the stage; it does not imply write access to any data tables)

#### Azure side

1. **ADLS Gen2 storage account** with a container reserved for Snowflake unload files (e.g., `snowflake-unload/databricks/`).
2. **Private endpoint from Snowflake's VNet to our storage account** — approved by our Azure admins. Snowflake writes unload files through this private endpoint.
3. **Private endpoint from our Databricks workspace to the same storage account** — the same private endpoint our other pipelines use. Databricks executors read unload files through this.
4. **Lifecycle policy on the unload container** — expires files after 24 hours to keep storage costs bounded. The connector cleans up after itself on newer versions, but the lifecycle policy is a backstop.

#### Network path summary

| Traffic | Source | Destination | Path |
|---|---|---|---|
| JDBC control | Databricks driver | Snowflake SQL endpoint | Azure Private Link to Snowflake |
| Unload write | Snowflake warehouse | Our ADLS container | Snowflake → ADLS private endpoint |
| Bulk read | Databricks executors | Our ADLS container | Databricks → ADLS private endpoint |

No traffic traverses the public internet.

### Diagram — external stage flow

*Embed the following SVG in the wiki page. Upload `snowflake_external_stage_private_endpoint_flow.svg` as an attachment via the wiki's attach-file feature, then reference it below.*

```
![External stage flow](/.attachments/snowflake_external_stage_private_endpoint_flow.svg)
```

### Step-by-step read flow

Walking through what happens when a framework pipeline executes:

1. **Driver opens JDBC session.** The Spark driver authenticates to Snowflake using our service account credentials (retrieved from Azure Key Vault via Databricks secret scope). The JDBC connection uses Azure Private Link to reach Snowflake's SQL endpoint — traffic does not leave our network.

2. **Connector rewrites the Spark plan.** DataFrame operations are translated into a single SQL query via query pushdown (see the pushdown section above).

3. **Driver sends SQL via JDBC.** The rewritten SQL is sent to Snowflake. This is the only traffic going to Snowflake over JDBC.

4. **Warehouse executes the query.** Our configured warehouse runs the SQL. If the warehouse is suspended, it resumes first (adds 10–60 seconds to cold queries).

5. **Warehouse issues `COPY INTO '@external_stage/'`.** Snowflake unloads the result set as multiple gzip-compressed CSV files (or Parquet, depending on connector version) into our ADLS container. The unload is parallelized across Snowflake workers. Files are written via Snowflake's private endpoint to our storage.

6. **Connector retrieves the file list.** The driver asks Snowflake for the list of files that were written. Snowflake returns paths under our container.

7. **Executors read files directly from ADLS.** Each Spark executor pulls some files through our Databricks-to-ADLS private endpoint. Files are decompressed, parsed, and materialized as DataFrame partitions. Snowflake is not involved in this step.

8. **Downstream Spark operations execute.** Anything that couldn't be pushed down (UDFs, joins with Delta tables, custom Python logic) runs on the loaded DataFrame.

9. **Cleanup.** On connector versions 2.10+, the staged files are deleted after the read completes. The container lifecycle policy expires anything left behind after 24 hours.

## Framework configuration reference

Framework users configure a Snowflake load by setting the following in the pipeline config. The framework wraps these into `sfOptions` and passes them to the connector.

### Required fields

| Field | Description | Example |
|---|---|---|
| `sf_database` | Snowflake database | `INVESTMENTS` |
| `sf_schema` | Snowflake schema | `PUBLIC` |
| `sf_warehouse` | Warehouse to run the query | `DATABRICKS_INGEST_WH` |
| `sf_role` | Snowflake role (must have appropriate grants) | `DATABRICKS_READER` |
| `query` or `dbtable` | Source SQL query or table name | See below |

### Source specification — `query` vs. `dbtable`

**Use `query` when:**
- The source requires Snowflake-specific SQL (CTEs, window functions, specific joins)
- You want explicit control over the SQL that Snowflake executes
- You're doing complex transformations that are easier in SQL than in Spark

**Use `dbtable` when:**
- You're pulling a whole table and relying on pushdown to filter it
- The source is a view that already encapsulates the logic

For most ingestion use cases in our framework, **`query` is the recommended default** because it makes the SQL explicit and portable.

### Options the framework sets automatically

Framework users do not need to set these — they are configured by the framework based on our standard infrastructure. Documented here for troubleshooting.

| Option | Value | Purpose |
|---|---|---|
| `sfUrl` | `<our_account>.privatelink.snowflakecomputing.com` | Private link SQL endpoint |
| `tempdir` | `wasbs://snowflake-unload@<our_account>.blob.core.windows.net/databricks/` | External stage storage location |
| `tempDirUseStage` | `true` | Use the named stage instead of internal |
| `tempDirStageName` | `SHARED_UTILITIES.STAGES.DATABRICKS_UNLOAD_STAGE` | Fully qualified external stage |
| `autopushdown` | `true` | Enable query pushdown (default) |
| `column_mapping` | `name` | Match columns by name |
| `preactions` | `ALTER SESSION SET QUERY_TAG = '<pipeline_name>'` | Tag queries for cost attribution |

**Reference for all options:** [Using the Spark Connector — Setting Configuration Options](https://docs.snowflake.com/en/user-guide/spark-connector-use#setting-configuration-options-for-the-connector)

### Authentication

Framework uses Snowflake key-pair authentication with the private key stored in Azure Key Vault and referenced via a Databricks secret scope. Username/password authentication is not used.

The private key is scoped to a service account role with the minimum grants described in the *Snowflake side* section above.

## Operational guidance

### Verifying pushdown

Run this in a notebook after loading a DataFrame:

```python
df.explain(extended=True)
```

Look for `SnowflakeRelation` in the output and confirm the SQL includes your filters and projections. If the SQL is just `SELECT * FROM source_table`, pushdown is broken — investigate before putting the pipeline in production.

### Monitoring queries in Snowflake

Queries initiated by the framework are tagged via the `QUERY_TAG` session parameter. To find recent framework queries in Snowflake:

```sql
SELECT query_id, user_name, role_name, warehouse_name, 
       execution_time/1000 as execution_seconds, 
       bytes_scanned, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag = '<pipeline_name>'
  AND start_time > DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

### Verifying the external stage is being used (not internal)

After a framework run, check Snowflake's query history for `COPY INTO` statements:

```sql
SELECT query_id, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%COPY INTO%'
  AND query_tag = '<pipeline_name>'
ORDER BY start_time DESC
LIMIT 10;
```

You should see `COPY INTO '@SHARED_UTILITIES.STAGES.DATABRICKS_UNLOAD_STAGE/...'`. If you see `COPY INTO '@~/...'` instead, the framework is falling back to the internal user stage — this indicates a configuration problem.

### Checking the ADLS container for staged files

During an active read, browse the `snowflake-unload/databricks/` prefix in our ADLS container. You should see compressed files appearing with UUID-based names. They should disappear within a few minutes of the pipeline completing (via connector cleanup) or within 24 hours (via lifecycle policy).

### Common failures and troubleshooting

**Symptom: "Cannot perform STAGE PUT. This session does not have a current schema."**
Cause: The session schema is not set. Our framework sets `sfSchema` in the linked service, so this should not occur. If it does, verify the service account role has `USAGE` on the schema specified in the config.

**Symptom: Pipeline hangs on read, no data flowing.**
Cause: Usually a network path issue. Check (1) can the cluster reach Snowflake via the private link endpoint? (2) can the cluster reach ADLS via the private endpoint? Network team: verify DNS resolution is returning private IPs from the cluster.

**Symptom: Query runs in Snowflake but the read fails with an access denied error on storage.**
Cause: The Databricks managed identity is missing `Storage Blob Data Reader` on the unload container. Confirm with Azure admins.

**Symptom: Pushdown is not working (full table scans).**
Cause: Usually a UDF or a Python-side transformation breaking the pushdown chain. Restructure the pipeline to do filtering/aggregation before any UDF or non-pushable operation.

**Symptom: Warehouse cost is higher than expected.**
Cause: Pushdown not working, or warehouse size too large for the workload. Check query history for bytes scanned and execution time. Right-size the warehouse — for ingestion, X-Small or Small is usually sufficient.

### Out-of-compliance cluster warning

If the Databricks cluster running framework pipelines shows a policy compliance warning, it generally indicates that the Snowflake connector library version or the Databricks Runtime version has drifted from the policy. Coordinate with the platform team before restarting the cluster, as a fix may require either updating the connector version or the cluster policy.

## Reference documentation

Links are grouped by component for targeted reading.

### Connector fundamentals

- [Snowflake Connector for Spark (overview)](https://docs.snowflake.com/en/user-guide/spark-connector) — top-level entry point
- [Overview of the Spark Connector — transfer modes](https://docs.snowflake.com/en/user-guide/spark-connector-overview) — the canonical page explaining internal vs. external transfer
- [Using the Spark Connector](https://docs.snowflake.com/en/user-guide/spark-connector-use) — all configuration options, pushdown behavior, and operational details
- [Installing and Configuring the Spark Connector](https://docs.snowflake.com/en/user-guide/spark-connector-install) — prerequisites and permissions

### Databricks-specific

- [Configuring Snowflake for Spark in Databricks (Snowflake docs)](https://docs.snowflake.com/en/user-guide/spark-connector-databricks)
- [Azure Databricks — Read and write data from Snowflake](https://learn.microsoft.com/en-us/azure/databricks/external-data/snowflake)
- [Spark Connector Release Notes](https://docs.snowflake.com/en/release-notes/clients-drivers/spark) — check minimum version for specific options like `tempDirStageName`

### External stage setup (Azure)

- [Configure an Azure container for loading data](https://docs.snowflake.com/en/user-guide/data-load-azure-config) — end-to-end storage integration and stage setup
- [CREATE STORAGE INTEGRATION](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) — SQL reference
- [CREATE STAGE](https://docs.snowflake.com/en/sql-reference/sql/create-stage) — SQL reference
- [Snowflake GRANT reference (for stage privileges)](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

### Private connectivity (Azure)

- [Private connectivity to external stages and Snowpipe automation for Microsoft Azure](https://docs.snowflake.com/en/user-guide/data-load-azure-private) — the page that matches our setup exactly
- [Manage private connectivity endpoints: Azure](https://docs.snowflake.com/en/user-guide/private-manage-endpoints-azure) — operational reference for provisioning endpoints
- [Azure Private Link and Snowflake (SQL endpoint)](https://docs.snowflake.com/en/user-guide/privatelink-azure) — private link for the Snowflake SQL endpoint itself
- [Azure private endpoints for internal stages](https://docs.snowflake.com/en/user-guide/private-internal-stages-azure) — reference only; not what we use

### Databricks private connectivity

- [Azure Databricks — Enable Azure Private Link](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/private-link) — workspace-level private link
- [Azure Databricks — Storage private endpoints](https://learn.microsoft.com/en-us/azure/databricks/security/network/storage/storage-private-link) — ADLS private endpoint configuration

### Source code

- [snowflakedb/spark-snowflake (GitHub)](https://github.com/snowflakedb/spark-snowflake) — connector source

## Change log

| Date | Author | Change |
|---|---|---|
| *(fill in)* | *(fill in)* | Initial version |

## Contacts

| Area | Team / Owner |
|---|---|
| Framework code and pipeline configuration | *(fill in)* |
| Snowflake account administration | *(fill in)* |
| Azure network and private endpoints | *(fill in)* |
| Databricks workspace administration | *(fill in)* |
