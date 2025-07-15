# SwiftLake
## Overview

SwiftLake is a Java library that bridges the gap between traditional SQL databases and cloud-native data lakes. By combining Apache Iceberg and DuckDB, it provides a lightweight, single-node solution that delivers SQL familiarity with cloud storage benefits, without the complexity of distributed systems.

## Key Features and Benefits

- **Query and Manage Cloud Storage**: SwiftLake brings familiar SQL queries and data management capabilities to object storage-based data lakes, providing a comfortable transition path for teams with RDBMS experience.

- **Efficient Data Operations**: Leveraging DuckDB's columnar processing and Iceberg's transaction management, SwiftLake delivers fast data operations for ingestion, querying, and complex transformations.

- **Flexible Deployment**: SwiftLake operates as a single-process application that connects DuckDB's lightweight engine with cloud storage, eliminating the need for distributed infrastructure for moderate workloads.
 
- **Core Data Lake Capabilities**: SwiftLake provides CRUD operations, SCD support, schema evolution, and time travel functionality on cloud storage.

- **Cloud Economics**: By using object storage for data and running compute only when needed, SwiftLake offers significant cost advantages over traditional database scaling approaches.

## When to Use SwiftLake

SwiftLake is ideal for:
- Organizations wanting SQL database familiarity with cloud storage economics
- Teams needing schema evolution, time travel, or SCD merge capabilities
- Scenarios where distributed processing frameworks would be overkill

By providing a middle ground between traditional databases and complex distributed systems, SwiftLake enables teams to modernize their data architecture with minimal disruption and maximal flexibility.


## SwiftLake Capabilities and Constraints

### Core Functionalities

- **Comprehensive Data Management**:
   - Execute queries
   - Perform write operations: insert, delete, update
   - Implement Slowly Changing Dimensions (SCD):
      - Type 1 merge
      - Type 2 merge

- **Dynamic Schema Evolution**:
   - Add, drop, rename, and reorder columns
   - Widen column types

- **Advanced Partitioning Strategies**:
   - Enhance query performance through intelligent data grouping
   - Support for multiple partition transforms:
      - Identity, bucket, truncate
      - Time-based: year, month, day, hour
   - Hidden partitioning capability
   - Partition evolution without data rewrite

### Performance Optimizations

- **Efficient Caching**: Optimize data access and query performance
- **MyBatis Integration**: Seamless interaction with MyBatis framework

### Current System Boundaries

- **File Format Compatibility**: Currently supports only Parquet format
- **Table Management Mode**:
   - Implements Copy-On-Write mode exclusively
   - Merge-On-Read not supported
- **Metadata Handling**:
   - Querying metadata tables not supported within SwiftLake
   - Snapshot and metadata management requires external engines (e.g., Spark)
- **Partitioning Limitation**: Cannot partition on columns from nested structs

> Note: For operations like data compaction, expiring snapshots, and deleting orphan files, use compatible external engines such as Apache Spark.

## Getting Started

### Including SwiftLake Dependency

To use SwiftLake in your project, add the following dependency to your build file:

#### Maven
Add this to your `pom.xml`:
```xml
<dependency>
    <groupId>com.arcesium.swiftlake</groupId>
    <artifactId>swiftlake-core</artifactId>
    <version>0.1.0</version>
</dependency>
```

#### Gradle
Add this to your `build.gradle`:
```gradle
implementation 'com.arcesium.swiftlake:swiftlake-core:0.1.0'
```

### Setup

1. Configure and create a Catalog:
```java
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

Map<String, String> properties = new HashMap<>();
properties.put("warehouse", "warehouse");
properties.put("type", "hadoop");
properties.put("io-impl", "com.arcesium.swiftlake.io.SwiftLakeHadoopFileIO");
Catalog catalog = CatalogUtil.buildIcebergCatalog("local", properties, new Configuration());
```

2. Build SwiftLakeEngine:
```java
import com.arcesium.swiftlake.SwiftLakeEngine;

SwiftLakeEngine swiftLakeEngine = SwiftLakeEngine.builderFor("demo").catalog(catalog).build();
```

### Creating a Table

1. Define the schema:
```java
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

Schema schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.required(3, "category", Types.StringType.get()),
    Types.NestedField.required(4, "date", Types.DateType.get())
);
PartitionSpec spec = PartitionSpec.builderFor(schema)
    .identity("date")
    .identity("category")
    .build();
```

2. Create the table:
```java
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

TableIdentifier name = TableIdentifier.of("db", "table");
Table table = catalog.createTable(name, schema, spec);
```

### Inserting Data

Use SQL to insert data:
```java
swiftLakeEngine.insertInto(table)
   .sql("SELECT * FROM (VALUES (1, 'a', 'category1', DATE'2025-01-01'), (2, 'b', 'category2', DATE'2025-01-01'), (3, 'c', 'category3', DATE'2025-03-01')) source(id, data, category, date)")
   .execute();
```

### Querying Data

Execute SQL queries using a JDBC-like interface:

```java
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

DataSource dataSource = swiftLakeEngine.createDataSource();
String selectSql = "SELECT * FROM db.table WHERE id = 2";
try (Connection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectSql)) {
   // Process the resultSet
}
```

You can also perform aggregations:

```java
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

DataSource dataSource = swiftLakeEngine.createDataSource();
String aggregateSql = "SELECT count(1) as count, data FROM db.table WHERE id > 0 GROUP BY data;";
try (Connection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(aggregateSql)) {
   // Process the resultSet
}
```
### AWS Integration

#### S3 Integration

To use SwiftLake with Amazon S3, you need to configure the S3 file system:

1. Add the below dependency:
##### Maven
```xml
<dependency>
    <groupId>com.arcesium.swiftlake</groupId>
    <artifactId>swiftlake-aws</artifactId>
    <version>0.1.0</version>
</dependency>
```

##### Gradle
```gradle
implementation 'com.arcesium.swiftlake:swiftlake-aws:0.1.0'
```

2. Configure S3 in your SwiftLake setup:
```java
Map<String, String> properties = new HashMap<>();
properties.put("warehouse", "s3://your-bucket-name/warehouse");
properties.put("io-impl", "com.arcesium.swiftlake.aws.SwiftLakeS3FileIO");
properties.put("client.region", "your-aws-region");
properties.put("s3.access-key-id", "YOUR_ACCESS_KEY");
properties.put("s3.secret-access-key", "YOUR_SECRET_KEY");
```

#### AWS Glue Catalog Integration

To use SwiftLake with AWS Glue Catalog:

Configure Glue Catalog in your SwiftLake setup:
```java
Map<String, String> properties = new HashMap<>();
properties.put("warehouse", "s3://your-bucket-name/warehouse");
properties.put("io-impl", "com.arcesium.swiftlake.aws.SwiftLakeS3FileIO");
properties.put("client.region", "your-aws-region");
properties.put("s3.access-key-id", "YOUR_ACCESS_KEY");
properties.put("s3.secret-access-key", "YOUR_SECRET_KEY");
properties.put("type", "glue");

Catalog catalog = CatalogUtil.buildIcebergCatalog("glue", properties, new Configuration());
SwiftLakeEngine swiftLakeEngine = SwiftLakeEngine.builderFor("demo").catalog(catalog).build();

// Create table, insert data, and query 

```

## Configuration

### SwiftLakeEngine Configuration

| Name                              | Default                                                          | Description |
|-----------------------------------|------------------------------------------------------------------|-------------|
| `localDir`                          | `<java.io.tmpdir>/<uuid>`                                        | Local storage where to write temp files. |
| `memoryLimitInMiB`                  | 90% of (Total memory - JVM Maximum Memory)                       | Maximum memory of the DuckDB instance |
| `memoryLimitFraction`               |                                               -                   | Fraction of total memory used for DuckDB instance. |
| `threads`                           | Available CPU cores `Runtime.getRuntime().availableProcessors()` | The number of total threads used by the DuckDB instance |
| `tempStorageLimitInMiB`             |                                              -                    | Maximum amount of disk space DuckDB can use for temporary storage |
| `maxPartitionWriterThreads`         | Same as threads                                                  | The number of total (Java) threads used in writing data to multiple partitions. |
| `totalFileSizePerScanLimitInMiB`    |                                             -                     | Maximum total file size (in MiB) of matched files allowed per table scan. Prevents excessive data processing after the scan. |
| `maxActiveConnections`              |                                            -                      | Maximum number of active connections allowed |
| `connectionCreationTimeoutInSeconds` |                                          -                        | Timeout (in seconds) for creating new connections, applicable only when maxActiveConnections is set. Ensures that connection attempts don't hang indefinitely when connection limits are enforced. |
| `queryTimeoutInSeconds`             |                                          -                        | Timeout (in seconds) for queries. Prevents long-running queries from impacting system performance. |
| `processTablesDefaultValue`         | `true`                                                           | Sets the default value for processing tables used in the queries. |
| `allowFullTableScan`                | `false`                                                          | Enables or disables full table scans. |
| `configureDuckDBExtensions`         |                                         -                         | Configures DuckDB extensions with the below options |
| &nbsp;&nbsp;&nbsp;&nbsp;`allowUnsignedExtensions`         | `false`                                                          | Allows the use of unsigned extensions |
| &nbsp;&nbsp;&nbsp;&nbsp;`allowCommunityExtensions`          | `false`                                                          | Allows the use of community provided extensions |
| &nbsp;&nbsp;&nbsp;&nbsp;`autoInstallKnownExtensions`        | `false`                                                          | Automatically installs and loads known extensions |
| `lockDuckDBConfiguration`           | `true`                                                             | Locks DuckDB configuration to prevent modifications. Ensures configuration integrity and security. |
| `cachingCatalog`                    |                                        -                          | Whether to cache Iceberg catalog entries (tables). This needs to be used carefully. Tables do not get refreshed until they are evicted from cache. It leads to reading stale data if there are commits after it is cached. Use this when reading stale data is acceptable for certain amount of time. |
| &nbsp;&nbsp;&nbsp;&nbsp;`namespaces`                        |               -                                                   | List of namespaces/databases that are considered for caching. |
| &nbsp;&nbsp;&nbsp;&nbsp;`fullyQualifiedTableNames`          |              -                                                    | List of fully qualified table names that are considered for caching. |
| &nbsp;&nbsp;&nbsp;&nbsp;`expirationIntervalInSeconds`       |             -                                                     | How long table entries are locally cached, in seconds. |
| `metricCollector`                   |                          -                                        | An implementation of the MetricCollector interface, responsible for collecting and posting metrics during operations. |
| `mybatisConfigPath`                 |                         -                                         | Classpath resource pointing to MyBatis XML configuration file. It is needed for the MyBatis integration. |
| `enableDebugFileUpload`             | `false`                                                          | Enables or disables uploading intermediate files generated using write operations. Useful for troubleshooting and debugging purposes |
| &nbsp;&nbsp;&nbsp;&nbsp;`uploadPath`                        |          -                                                        | Base path where debug files will be uploaded. |
| &nbsp;&nbsp;&nbsp;&nbsp;`tags`                              |         -                                                         | S3 tags to apply to the uploaded debug files. |

### Catalog Properties
#### SwiftLakeFileIOProperties
This table outlines the general file I/O configuration properties for SwiftLake. These properties control caching behavior, and metric collection for file operations.

| Property Key | Default Value                                            | Description |
|-------------------|----------------------------------------------------------|-------------|
| `io.manifest-cache.enabled` | `true`                                                   | Enables/disables the I/O manifest cache |
| `io.manifest-cache.expiration-interval-seconds` | 7200 (2 hours)                                           | Sets the I/O manifest cache expiration interval in seconds |
| `io.manifest-cache.max-total-bytes` | 268,435,456 (256 MB)                                     | Sets the maximum total bytes for the I/O manifest cache |
| `io.manifest-cache.max-content-length` | 16,777,216 (16 MB)                                       | Sets the maximum content length for the I/O manifest cache |
| `io.file-system-cache.enabled` | `true`                                                   | Enables/disables the I/O file system cache |
| `io.file-system-cache.base-path` | `java.io.tmpdir` + random UUID                           | Sets the base path for the I/O file system cache |
| `io.file-system-cache.max-total-bytes` | 536,870,912 (512 MB)                                     | Sets the maximum total bytes for the I/O file system cache |
| `io.file-system-cache.expiration-interval-seconds` | 172,800 (2 days)                                         | Sets the I/O file system cache expiration interval in seconds |
| `delegate-io-impl` |                             -                             | Specifies the delegate file I/O implementation class |
| `cache-io-provider` | `com.arcesium.swiftlake.io.SingletonCacheFileIOProvider` | Specifies the cache file I/O provider class |
| `metric-collector-provider` |                  -                                        | Specifies the metric collector provider class |

#### SwiftLakeS3FileIOProperties
This table presents the S3-specific configuration properties for SwiftLake. These properties are used to configure the connection to Amazon S3 or S3-compatible storage services.

| Property Key | Default Value | Description                                                                                                                                                  |
|--------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `s3.crt.target-throughput-gbps` | 10.0 | Target throughput in Gbps for the S3 CRT (Common Runtime) client. This setting helps optimize performance when creating the S3 CRT client.                   |
| `s3.crt.max-concurrency` | - | Maximum number of concurrent requests for the S3 CRT client. Used to control the level of parallelism in S3 operations.                                      |
| `s3.crt.max-native-memory-limit-bytes` | - | Maximum native memory limit in bytes for the S3 CRT client. Helps manage memory usage of the client.                                                         |
| `s3.crt.multipart.threshold-bytes` | - | Threshold in bytes for switching to multipart uploads in the S3 CRT client. Files larger than this size will use multipart uploads.                          |
| `s3.transfer-manager-provider` | `com.arcesium.swiftlake.aws.SingletonS3TransferManagerProvider` | Specifies the class name for the S3 transfer manager provider. This class is responsible for creating and managing S3TransferManager instances.              |
| `s3.transfer-manager-provider.*` | - | Prefix for additional properties specific to the configured S3 transfer manager provider. These properties are passed to the provider during initialization. |
| `s3.duckdb.s3-extension-enabled` | false | Enables or disables the DuckDB S3 extension integration. When true, allows for direct querying of S3 data using DuckDB.                                      |
| `s3.duckdb.s3-extension-threshold-bytes` | - | Minimum file size (in bytes) for using DuckDB S3 integration. If the file size is greater than or equal to this value, DuckDB S3 integration is used.        |

#### Configuring Caching
To enable and configure caching when creating a catalog, you can use the following properties:

```java
properties.put("io-impl", "com.arcesium.swiftlake.io.SwiftLakeCachedFileIO");
properties.put("delegate-io-impl", "com.arcesium.swiftlake.aws.SwiftLakeS3FileIO");
properties.put("cache-io-provider", "com.arcesium.swiftlake.io.SingletonCacheFileIOProvider");
```
Here's what each property does:
1. `io-impl`: Set to `com.arcesium.swiftlake.io.SwiftLakeCachedFileIO` to use the cached file I/O implementation.
2. `delegate-io-impl`: Set to `com.arcesium.swiftlake.aws.SwiftLakeS3FileIO` to specify that the underlying storage is S3.
3. `cache-io-provider`: Set to `com.arcesium.swiftlake.io.SingletonCacheFileIOProvider` to use the singleton cache file I/O provider.

By setting these properties, you enable a caching layer on top of the S3 storage. The `SwiftLakeCachedFileIO` will handle caching, while the `SwiftLakeS3FileIO` will manage the actual S3 operations. The `SingletonCacheFileIOProvider` ensures that a single instance of the cache is used across the application.

You can further customize the caching behavior using the `io.manifest-cache` and `io.file-system-cache` properties listed in the SwiftLakeFileIOProperties table above. For example:

```java
properties.put("io.manifest-cache.max-total-bytes", "536870912"); // 512 MB
properties.put("io.file-system-cache.expiration-interval-seconds", "86400"); // 1 day
```

These settings allow you to fine-tune the caching mechanism to suit your specific performance and resource requirements.

## Writes
### Insert Operation
The insert operation appends new data to the target table in SwiftLake.

#### Basic Usage

```java
swiftLakeEngine.insertInto(table)
    .sql(sourceSql)
    .execute();
```

#### Configuration Options

| Method | Default | Description                                                                                 |
|--------|---------|---------------------------------------------------------------------------------------------|
| `sql` | - | The SELECT statement SQL query to retrieve input data for insertion                         |
| `mybatisStatement` | - |                                                                                             |
| &nbsp;&nbsp;&nbsp;&nbsp;`id` | - | Identifier of the MyBatis SELECT statement to retrieve input data for insertion             |
| &nbsp;&nbsp;&nbsp;&nbsp;`parameter` | - | Parameters to replace in the MyBatis SQL query                                              |
| `columns` | All columns | List of column names to include in the insertion. Unspecified columns will be set to NULL.  |
| `executeSqlOnceOnly` | `false` | When `true`, query results are persisted to avoid repeated execution                        |
| `skipDataSorting` | `false` | When `true`, skips sorting data before insertion                                            |
| `sqlSessionFactory` | Engine-level factory | SqlSessionFactory to use for MyBatis integration                                            |
| `processSourceTables` | Engine-level default | Whether to process Iceberg tables present in the source SQL                                 |
| `isolationLevel` | `SERIALIZABLE` | Specifies the Iceberg isolation level to use                                                |
| `branch` | `main` | Specifies the Iceberg branch to use                                                         |

#### Execution Optimization

- `executeSqlOnceOnly(boolean flag)`: When `true`, caches query results to avoid repeated execution. Useful for expensive queries or non-deterministic data sources.
- `skipDataSorting(boolean flag)`: When `true`, bypasses the default data sorting step. Can improve performance but may affect data organization.

### Insert Overwrite

Insert Overwrite operation overwrites existing data in the table with new data, based on the given filter. This operation deletes files that contain only rows matching the filter and fails if a file contains a mix of matching and non-matching rows.

#### Basic Usage

```java
swiftLakeEngine.insertOverwrite(tableName)
    .overwriteByFilterSql("date = DATE'2025-01-01'")
    .sql(sourceSql)
    .execute();
```

#### Overwrite Filter Configuration

| Name | Default | Description |
|------|---------|-------------|
| `overwriteByFilterSql` | - | SQL predicate to filter files |
| `overwriteByFilterColumns` | - | Alternative to `overwriteByFilterSql`, specifying columns to filter. It dynamically creates the filter conditions based on distinct source data values |
| `overwriteByFilter` | - | Alternative condition specification using Expressions APIs |


#### Best Practices

1. Always double-check your filter conditions to ensure you're overwriting the intended data.
2. Ensure that your source data fully covers the range specified by the overwrite filter to avoid partial updates.

### Update Operation
SwiftLake provides an update operation through the `SwiftLakeEngine` class. This operation allows you to modify existing records in a table based on specified conditions.

#### Basic Usage

```java
swiftLakeEngine.update(table)
    .conditionSql("id = 1")
    .updateSets(Map.of("data", "aa"))
    .execute();
```

#### Configuration Options

| Name | Default | Description |
|------|---------|-------------|
| `conditionSql` | - | SQL predicate to match records for modification |
| `condition` | - | Alternative condition specification using Expressions APIs |
| `updateSets` | - | Map of column names to new values for updating records |
| `skipDataSorting` | `false` | When set to true, skips sorting data before insertion |
| `isolationLevel` | `serializable` | Specifies the Iceberg isolation level to use |
| `branch` | `main` | Specifies the Iceberg branch to use |

### Delete Operation

The Delete operation in SwiftLake allows you to remove records from a target table based on specified conditions. 

```java
swiftLakeEngine.deleteFrom(tableName)
    .conditionSql("id = 1 AND date = DATE'2025-01-01'")
    .execute();
```

#### Configuration

| Name | Default | Description |
|------|---------|-------------|
| `conditionSql` | - | SQL predicate to match records for deletion |
| `condition` | - | Alternative condition specification using Expressions APIs |
| `skipDataSorting` | `false` | When set to true, skips sorting data while rewriting existing records |
| `isolationLevel` | `serializable` | Specifies the Iceberg isolation level to use |
| `branch` | `main` | Specifies the Iceberg branch to use |

### SCD1Merge Operation

The SCD1Merge functionality in SwiftLakeEngine allows you to perform Slowly Changing Dimension Type 1 (SCD1) merges on Iceberg tables. This operation combines insert, update, and delete operations, enabling you to update existing records, insert new ones, and delete records based on matching conditions and an operation column in the source.

```java
String sourceSql = "SELECT * FROM (VALUES (1, 'a', 'category1', DATE'2025-01-01', 'INSERT'), " +
                   "(2, 'b', 'category2', DATE'2025-01-01', 'INSERT')) " +
                   "source(id, data, category, date, operation)";

swiftLakeEngine.applyChangesAsSCD1(tableName)
    .tableFilterSql("date = DATE'2025-01-01'")
    .sourceSql(sourceSql)
    .keyColumns(List.of("id", "category", "date"))
    .operationTypeColumn("operation", "DELETE")
    .execute();
```

#### Configuration Options

| Name | Default | Description                                                                        |
|------|---------|------------------------------------------------------------------------------------|
| `tableFilterSql` | - | SQL predicate to filter target table records before merge                          |
| `tableFilterColumns` | - | Alternative to `tableFilterSql`, specifying columns to filter target table records |
| `tableFilter` | - | Alternative condition specification using Expressions APIs                         |
| `sourceSql` | - | SELECT statement SQL query to retrieve input data for merge                        |
| `sourceMybatisStatement` | - |               |
| &nbsp;&nbsp;&nbsp;&nbsp;`id` | - | Identifier of the MyBatis SELECT statement to retrieve input data                  |
| &nbsp;&nbsp;&nbsp;&nbsp;`parameter` | - | Parameters to replace in the MyBatis SQL query                                     |
| `keyColumns` | - | Primary key columns used to match source and target records                        |
| `operationTypeColumn` | - |                                              |
| &nbsp;&nbsp;&nbsp;&nbsp;`column` | - | Column specifying merge operation type                                             |
| &nbsp;&nbsp;&nbsp;&nbsp;`deleteOperationValue` | - | Value indicating a delete operation                                                |
| `columns` | All columns of the table | List of column names to include in the merge                                       |
| `executeSourceSqlOnceOnly` | `false` | Set to true for partitioned tables with expensive or non-deterministic queries     |
| `skipDataSorting` | `false` | When set to true, skips sorting data before insertion                              |
| `sqlSessionFactory` | SwiftLake Engine-level SqlSessionFactory | Optional SqlSessionFactory for MyBatis integration                                 |
| `processSourceTables` | SwiftLake Engine-level processTablesDefaultValue | Process tables present in the source SQL                                           |
| `isolationLevel` | `serializable` | Specifies the Iceberg isolation level to use                                       |
| `branch` | `main` | Specifies the Iceberg branch to use                                                |

#### Best Practices

1. Always specify key columns to ensure accurate matching between source and target records.
2. Use table filtering to optimize performance, especially for large tables.
3. Consider using `executeSourceSqlOnceOnly` for complex or non-deterministic source queries.

### SCD2 Merge Operation 

The SCD2 (Slowly Changing Dimension Type 2) merge operation in SwiftLakeEngine preserves historical records, maintaining a temporal history of changes. This allows for efficient auditing and analysis of data over time.

#### Changes Mode

In Changes Mode, the SCD2 merge operation processes incremental changes based on change events. It expects input data containing records with an Operation Column specifying the change type (INSERT/UPDATE, DELETE). The operation automatically detects inserts and updates through record matching.

```java
String sourceSql = "SELECT * FROM (VALUES (1, 'a', 'category1', DATE'2025-01-01', 'INSERT'), " +
                   "(2, 'b', 'category2', DATE'2025-01-01', 'INSERT')) " +
                   "source(id, data, category, date, operation)";

swiftLakeEngine.applyChangesAsSCD2(tableName)
    .tableFilterSql("date = DATE'2025-01-01'")
    .sourceSql(sourceSql)
    .generateEffectiveTimestamp(true)
    .keyColumns(List.of("id", "category", "date"))
    .operationTypeColumn("operation", "DELETE")
    .effectivePeriodColumns("effective_start", "effective_end")
    .execute();
```

##### Configuration Options

| Name | Default | Description                                                                        |
|------|---------|------------------------------------------------------------------------------------|
| `tableFilterSql` | - | SQL predicate to filter target table records before merge                          |
| `tableFilterColumns` | - | Alternative to `tableFilterSql`, specifying columns to filter target table records |
| `tableFilter` | - | Alternative condition specification using Expressions APIs                         |
| `sourceSql` | - | SELECT statement SQL query to retrieve input data for merge                        |
| `sourceMybatisStatement` | - |                           |
| &nbsp;&nbsp;&nbsp;&nbsp;`id` | - | Identifier of the MyBatis SELECT statement to retrieve input data                  |
| &nbsp;&nbsp;&nbsp;&nbsp;`parameter` | - | Parameters to replace in the MyBatis SQL query                                     |
| `effectiveTimestamp` | - | Effective timestamp for changes                                           |
| `generateEffectiveTimestamp` | - | Automatically generates `effectiveTimestamp` using `LocalDateTime.now()`           |
| `keyColumns` | - | Primary key columns used to match source and target records                        |
| `operationTypeColumn` | - |                                              |
| &nbsp;&nbsp;&nbsp;&nbsp;`column` | - | Column specifying merge operation type                                             |
| &nbsp;&nbsp;&nbsp;&nbsp;`deleteOperationValue` | - | Value indicating a delete operation                                                |
| `effectivePeriodColumns` | - | Specifies effective start and end column names                                     |
| `currentFlagColumn` | - | Sets the column used to indicate the current (active) record                       |
| `columns` | All columns of the table | List of column names to include in the merge                                       |
| `changeTrackingColumns` | All non-key columns | List of columns used to identify changes                                           |
| `changeTrackingMetadata` | - | Map of column names to their change tracking metadata. Change tracking metadata contains maximum allowed delta values and null replacement values.                              |
| `executeSourceSqlOnceOnly` | `false` | Set to true for partitioned tables with expensive or non-deterministic queries     |
| `skipDataSorting` | `false` | When set to true, skips sorting data before insertion                              |
| `sqlSessionFactory` | SwiftLake Engine-level SqlSessionFactory | Optional SqlSessionFactory for MyBatis integration                                 |
| `processSourceTables` | SwiftLake Engine-level processTablesDefaultValue | Process tables present in the source SQL                                           |
| `isolationLevel` | `serializable` | Specifies the Iceberg isolation level to use                                       |
| `branch` | `main` | Specifies the Iceberg branch to use                                                |

##### Best Practices

1. Use table filtering to optimize performance, especially for large tables.
2. Carefully choose key columns to ensure accurate matching between source and target records.
3. Consider using `executeSourceSqlOnceOnly` for complex or non-deterministic source queries.
4. Implement appropriate change tracking to capture all relevant modifications.

#### Snapshot Mode

The SCD2 (Slowly Changing Dimension Type 2) merge operation in Snapshot mode performs merge based on snapshot comparisons. This mode identifies changes by comparing the input snapshot data with the snapshot data existing in the table as of a given effective timestamp, enabling efficient detection of inserts, updates, and deletes between the two snapshots.

```java
String sourceSql = "SELECT * FROM (VALUES (3, 'c', 'category1', DATE'2025-01-01'), " +
                   "(2, 'bb', 'category2', DATE'2025-01-01')) " +
                   "source(id, data, category, date)";

swiftLakeEngine.applySnapshotAsSCD2(tableName)
    .tableFilterSql("date = DATE'2025-01-01'")
    .sourceSql(sourceSql)
    .generateEffectiveTimestamp(true)
    .keyColumns(List.of("id", "category", "date"))
    .effectivePeriodColumns("effective_start", "effective_end")
    .execute();
```

##### Configuration Options

| Name | Default | Description                                                                                                               |
|------|---------|---------------------------------------------------------------------------------------------------------------------------|
| `tableFilterSql` | - | SQL predicate to retrieve the existing snapshot data from the table for comparison                                        |
| `tableFilter` | - | Alternative condition specification using Expressions APIs                                                                |
| `sourceSql` | - | SELECT statement SQL query to retrieve input data for merge                                                               |
| `sourceMybatisStatement` | - |                                                                                                                           |
| &nbsp;&nbsp;&nbsp;&nbsp;`id` | - | Identifier of the MyBatis SELECT statement to retrieve input data                                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;`parameter` | - | Parameters to replace in the MyBatis SQL query                                                                            |
| `effectiveTimestamp` | - | Effective timestamp for changes                                                                                  |
| `generateEffectiveTimestamp` | - | Automatically generates `effectiveTimestamp` using `LocalDateTime.now()`                                                  |
| `keyColumns` | - | Primary key columns used to match source and target records                                                               |
| `effectivePeriodColumns` | - | Specifies effective start and end column names                                                                            |
| `currentFlagColumn` | - | Sets the column used to indicate the current (active) record                       |
| `columns` | All columns of the table | List of column names to include in the merge                                                                              |
| `changeTrackingColumns` | All non-key columns | List of columns used to identify changes                                                                                  |
| `changeTrackingMetadata` | - | Map of column names to their change tracking metadata, including maximum allowed delta values and null replacement values |
| `executeSourceSqlOnceOnly` | `false` | Set to true for partitioned tables with expensive or non-deterministic queries                                            |
| `skipDataSorting` | `false` | When set to true, skips sorting data before insertion                                                                     |
| `sqlSessionFactory` | SwiftLake Engine-level SqlSessionFactory | Optional SqlSessionFactory for MyBatis integration                                                                        |
| `processSourceTables` | SwiftLake Engine-level processTablesDefaultValue | Process tables present in the source SQL                                                                                  |
| `isolationLevel` | `serializable` | Specifies the Iceberg isolation level to use                                                                              |
| `branch` | `main` | Specifies the Iceberg branch to use                                                                                       |

##### Best Practices

1. Carefully define your tableFilter to ensure you're comparing the correct subset of existing data.
2. Use `changeTrackingMetadata` to fine-tune change detection, especially for numeric columns where small variations might not be considered significant changes.
3. Consider using `executeSourceSqlOnceOnly` for complex or expensive source queries to improve performance.
4. Implement appropriate change tracking to capture all relevant modifications.

#### Out-of-Order Records Detection

The SCD2 (Slowly Changing Dimension Type 2) merge process includes a crucial verification step to detect out-of-order records. This feature ensures the integrity and accuracy of historical data tracking.

Key points:
- Verifies that the effective timestamp is later than all existing records
- Prevents batch updates with effective timestamps earlier than or equal to already recorded changes
- Ensures strict chronological progression in the table

The system checks the effective timestamp provided against the existing data. If any record in the dimension table has a timestamp later than or equal to this merge timestamp, it's flagged as an out-of-order merge attempt.

## Queries

SwiftLake Library supports DuckDB SQL queries on SwiftLake tables, offering optimized performance and flexible querying options.

### Basic Query Syntax

To query a SwiftLake table, prefix the table name with the database name:

```sql
SELECT * FROM db.table WHERE id = 1
```

### Query Optimization

The library optimizes queries by:
- Pushing down table filters to Iceberg scan
- Enabling effective partition filtering
- Utilizing file filtering using partition data and column statistics from Iceberg metadata

This results in efficient querying and minimal data transfer.

### Table Filtering Methods

1. Direct WHERE clause:
   ```sql
   SELECT * FROM db.table WHERE id = 1
   ```

2. Joining with a filter table:
   ```sql
   SELECT a.date, a.id, a.category, a.data
   FROM db.table a
   JOIN filter_table b ON (a.id = b.id AND a.category=b.category)
   WHERE a.date BETWEEN DATE'2025-01-01' AND DATE'2025-04-01'
   ORDER BY a.date, a.id, a.category
   ```

### Complex Queries

SwiftLake tables must be wrapped within their own subquery when used in complex queries with other joins or filters. This includes scenarios with multiple joins, sub-queries, or complex WHERE clauses.

### Query Syntax

Supported queries adhere to DuckDB syntax. Refer to DuckDB documentation for detailed syntax information.

### Parsing Compatibility

To avoid parsing errors with SwiftLake's SQL parser, use parse markers around SQL queries containing Iceberg tables:

```sql
COPY (
--SWIFTLAKE_PARSE_BEGIN--
SELECT * FROM db.table WHERE id = 1
--SWIFTLAKE_PARSE_END--   
) TO 'data.parquet';
```

### JDBC Querying

Use SwiftLake's JDBC-style interface for querying:

```java
DataSource dataSource = swiftLakeEngine.createDataSource();
try (Connection connection = dataSource.getConnection();
     Statement statement = connection.createStatement();
     ResultSet resultSet = statement.executeQuery("SELECT * FROM db.table WHERE id = 1")) {
    // Process results
}
```

### MyBatis Integration

SwiftLake supports MyBatis for object-relational mapping:

```java
public interface MybatisMapper {
    @Select("SELECT * FROM db.table WHERE id=#{id} AND category=#{category}")
    List<Map<String, Object>> getData(@Param("id") Integer id, @Param("category") String category);
}
```
```java
SwiftLakeMybatisConfiguration configuration = swiftLakeEngine.createMybatisConfiguration();
configuration.addMapper(MybatisMapper.class);
SqlSessionFactory sqlSessionFactory = swiftLakeEngine.createSqlSessionFactory(configuration);
try (SqlSession session = sqlSessionFactory.openSession()) {
    MybatisMapper mybatisMapper = session.getMapper(MybatisMapper.class);
    return mybatisMapper.getData(id, category);
}
```

### Time Travel Queries

Execute queries at a specific point in time or version:

```sql
-- Time travel to October 26, 1986 at 01:21:00
SELECT * FROM db."table$timestamp_1986-10-26T01:21:00";

-- Time travel to snapshot with id 10963874102873L
SELECT * FROM db."table$snapshot_10963874102873";

-- Time travel to the head snapshot of audit-branch
SELECT * FROM db."table$branch_audit-branch";

-- Time travel to the snapshot referenced by the tag historical-snapshot
SELECT * FROM db."table$tag_historical-snapshot";
```

These time travel capabilities allow for querying historical data states, which is crucial for auditing, debugging, and analyzing data changes over time.

## Contributing

We welcome contributions from the open-source community. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Building

SwiftLake library is organized into the following modules:

- `swiftlake-core`: Contains implementations of the core SwiftLake engine
- `swiftlake-aws`: An optional module for working with S3 storage

This project uses Gradle as the build tool. To build and work with the project:

1. Ensure you have JDK 17 or later installed.
2. Build the project with all tests: `./gradlew build`
3. Build the project without running tests: `./gradlew build -x test -x integrationTest`
4. Format the code according to the project's style guidelines: `./gradlew spotlessApply`
   Note: The project uses Error Prone for static code analysis and Spotless for code formatting.

## License

This library is licensed under the [Apache-2.0 license](LICENSE).
