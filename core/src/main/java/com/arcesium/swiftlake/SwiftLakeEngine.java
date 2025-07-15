/*
 * Copyright (c) 2025, Arcesium LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcesium.swiftlake;

import com.arcesium.swiftlake.commands.Delete;
import com.arcesium.swiftlake.commands.Insert;
import com.arcesium.swiftlake.commands.SCD1Merge;
import com.arcesium.swiftlake.commands.SCD2Merge;
import com.arcesium.swiftlake.commands.Update;
import com.arcesium.swiftlake.common.FileUtil;
import com.arcesium.swiftlake.common.SwiftLakeConfiguration;
import com.arcesium.swiftlake.common.SwiftLakeException;
import com.arcesium.swiftlake.common.ValidationException;
import com.arcesium.swiftlake.dao.CommonDao;
import com.arcesium.swiftlake.expressions.Expression;
import com.arcesium.swiftlake.metrics.DuckDBRuntimeMetrics;
import com.arcesium.swiftlake.metrics.MetricCollector;
import com.arcesium.swiftlake.metrics.PartitionData;
import com.arcesium.swiftlake.mybatis.SwiftLakeMybatisConfiguration;
import com.arcesium.swiftlake.mybatis.SwiftLakeSqlSessionFactory;
import com.arcesium.swiftlake.sql.IcebergScanExecutor;
import com.arcesium.swiftlake.sql.SchemaEvolution;
import com.arcesium.swiftlake.sql.SqlQueryProcessor;
import com.arcesium.swiftlake.sql.SwiftLakeConnection;
import com.arcesium.swiftlake.sql.SwiftLakeDataSource;
import com.arcesium.swiftlake.writer.TableBatchTransaction;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.LocationUtil;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SwiftLakeEngine class represents the core engine for SwiftLake operations. */
public class SwiftLakeEngine implements AutoCloseable {
  /** Marks the start of SQL that should be parsed by SwiftLake */
  public static final String SWIFTLAKE_PARSE_BEGIN_MARKER = "--SWIFTLAKE_PARSE_BEGIN--";

  /** Marks the end of SQL that should be parsed by SwiftLake */
  public static final String SWIFTLAKE_PARSE_END_MARKER = "--SWIFTLAKE_PARSE_END--";

  private static final Logger LOGGER = LoggerFactory.getLogger(SwiftLakeEngine.class);
  private final Catalog catalog;
  private final Catalog cachingCatalog;
  private final SwiftLakeConfiguration configuration;
  private final SchemaEvolution schemaEvolution;
  private final DuckDBConnection duckDBConnection;
  private final SwiftLakeSqlSessionFactory internalSqlSessionFactory;
  private final CommonDao commonDao;
  private SwiftLakeSqlSessionFactory sqlSessionFactory;
  private final ExecutorService partitionWriterThreadPool;
  private final SqlQueryProcessor sqlQueryProcessor;
  private final IcebergScanExecutor icebergScanExecutor;
  private final Timer timer;
  private final Semaphore maxConnectionsSemaphore;
  private List<AutoCloseable> autoCloseables;

  /**
   * Constructor for SwiftLakeEngine.
   *
   * @param configuration The SwiftLakeConfiguration to initialize the engine with.
   */
  private SwiftLakeEngine(SwiftLakeConfiguration configuration) {
    this.configuration = configuration;
    fillDefaults(this.configuration);
    this.catalog = configuration.getCatalog();
    if (!this.configuration.getCachingCatalogTableNames().isEmpty()
        || !this.configuration.getCachingCatalogNamespaces().isEmpty()) {
      this.cachingCatalog =
          CachingCatalog.wrap(
              this.catalog,
              this.configuration.getCachingCatalogExpirationIntervalInSeconds() * 1_000L);
    } else {
      this.cachingCatalog = null;
    }
    this.schemaEvolution = new SchemaEvolution();
    this.duckDBConnection = (DuckDBConnection) this.createDuckDBInstance();
    this.timer = createTimer();
    if (configuration.getMaxActiveConnections() != null) {
      this.maxConnectionsSemaphore = new Semaphore(configuration.getMaxActiveConnections());
    } else {
      this.maxConnectionsSemaphore = null;
    }
    if (this.configuration.getConfigureFunc() != null) {
      this.autoCloseables = this.configuration.getConfigureFunc().apply(this);
    }
    this.configureDuckDB();
    this.internalSqlSessionFactory = createInternalSqlSessionFactory();
    this.commonDao = new CommonDao(this);
    if (configuration.getMybatisConfigPath() != null) {
      this.sqlSessionFactory =
          new SwiftLakeSqlSessionFactory(
              this,
              configuration.getMybatisConfigPath(),
              configuration.getProcessTablesDefaultValue());
    }
    if (this.configuration.getMaxPartitionWriterThreads() == null) {
      this.configuration.setMaxPartitionWriterThreads(this.configuration.getThreads());
    }
    this.partitionWriterThreadPool =
        Executors.newFixedThreadPool(this.configuration.getMaxPartitionWriterThreads());
    this.sqlQueryProcessor = new SqlQueryProcessor(this);
    this.icebergScanExecutor = new IcebergScanExecutor(this, configuration);
  }

  private void fillDefaults(SwiftLakeConfiguration configuration) {
    // Set default local directory if not provided
    if (configuration.getLocalDir() != null) {
      configuration.setLocalDir(LocationUtil.stripTrailingSlash(configuration.getLocalDir()));
    } else {
      configuration.setLocalDir(
          System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString());
    }
    // Set default number of threads if not provided
    if (configuration.getThreads() == null) {
      configuration.setThreads(Runtime.getRuntime().availableProcessors());
    }
    // Set default memory limit if not provided
    if (configuration.getMemoryLimitInMiB() == null) {
      int memory;
      long physicalMemoryBytes =
          ((com.sun.management.OperatingSystemMXBean)
                  java.lang.management.ManagementFactory.getOperatingSystemMXBean())
              .getTotalMemorySize();
      if (configuration.getMemoryLimitFraction() != null) {
        memory =
            ((Double)
                    (physicalMemoryBytes
                        * configuration.getMemoryLimitFraction()
                        / FileUtil.MIB_FACTOR))
                .intValue();
      } else {
        long maxJvmMemoryBytes = Runtime.getRuntime().maxMemory();
        long remaining = physicalMemoryBytes - maxJvmMemoryBytes;
        memory = ((Double) (remaining * 0.9 / FileUtil.MIB_FACTOR)).intValue();
      }
      configuration.setMemoryLimitInMiB(Math.max(1, memory));
    }
    // Set default connection creation timeout if max active connections is set
    if (configuration.getMaxActiveConnections() != null
        && configuration.getConnectionCreationTimeoutInSeconds() == null) {
      configuration.setConnectionCreationTimeoutInSeconds(10 * 60);
    }
  }

  /**
   * Closes the SwiftLakeEngine and releases all resources. This method should be called when the
   * engine is no longer needed.
   */
  @Override
  public void close() {
    try {
      this.partitionWriterThreadPool.shutdown();
      this.duckDBConnection.close();
      this.timer.cancel();
      FileUtils.deleteDirectory(new File(this.configuration.getLocalDir()));
      if (this.autoCloseables != null) {
        for (AutoCloseable closeable : autoCloseables) {
          closeable.close();
        }
      }
    } catch (Exception e) {
      LOGGER.error("An error occurred while closing SwiftLakeEngine instance.");
    }
  }

  /**
   * Parses a table identifier from a string representation.
   *
   * @param name The string representation of the table identifier.
   * @return A TableIdentifier object.
   */
  public TableIdentifier getTableIdentifier(String name) {
    return TableIdentifier.parse(name);
  }

  /**
   * Returns the catalog used by this SwiftLakeEngine.
   *
   * @return The Catalog instance.
   */
  public Catalog getCatalog() {
    return catalog;
  }

  /**
   * Returns the local directory path used for temporary files.
   *
   * @return The local directory path.
   */
  public String getLocalDir() {
    return configuration.getLocalDir();
  }

  /**
   * Retrieves the application ID from the configuration.
   *
   * @return The application ID as a String.
   */
  public String getApplicationId() {
    return configuration.getApplicationId();
  }

  /**
   * Returns the memory limit in MiB for DuckDB operations.
   *
   * @return The memory limit in MiB.
   */
  public int getMemoryLimitInMiB() {
    return configuration.getMemoryLimitInMiB();
  }

  /**
   * Returns the number of threads to use for parallel operations.
   *
   * @return The number of threads.
   */
  public int getThreads() {
    return configuration.getThreads();
  }

  /**
   * Returns the query timeout in seconds.
   *
   * @return The query timeout in seconds, or null if not set.
   */
  public Integer getQueryTimeoutInSeconds() {
    return configuration.getQueryTimeoutInSeconds();
  }

  /**
   * Returns the CommonDao used for database operations.
   *
   * @return The CommonDao instance.
   */
  public CommonDao getCommonDao() {
    return commonDao;
  }

  /**
   * Retrieves the SchemaEvolution instance.
   *
   * @return The SchemaEvolution object associated with this SwiftLakeEngine.
   */
  public SchemaEvolution getSchemaEvolution() {
    return schemaEvolution;
  }

  /**
   * Retrieves the SqlQueryProcessor instance.
   *
   * @return The SqlQueryProcessor object associated with this SwiftLakeEngine.
   */
  public SqlQueryProcessor getSqlQueryProcessor() {
    return sqlQueryProcessor;
  }

  /**
   * Retrieves the IcebergScanExecutor instance.
   *
   * @return The IcebergScanExecutor object associated with this SwiftLakeEngine.
   */
  public IcebergScanExecutor getIcebergScanExecutor() {
    return icebergScanExecutor;
  }

  /**
   * Returns the SwiftLakeSqlSessionFactory used for SQL operations.
   *
   * @return The SwiftLakeSqlSessionFactory instance.
   */
  public SwiftLakeSqlSessionFactory getSqlSessionFactory() {
    return this.sqlSessionFactory;
  }

  /**
   * Returns the internal SwiftLakeSqlSessionFactory used for internal SQL operations.
   *
   * @return The internal SwiftLakeSqlSessionFactory instance.
   */
  public SwiftLakeSqlSessionFactory getInternalSqlSessionFactory() {
    return this.internalSqlSessionFactory;
  }

  /**
   * Returns the Timer used for scheduling tasks.
   *
   * @return The Timer instance.
   */
  public Timer getTimer() {
    return timer;
  }

  /**
   * Returns the MetricCollector used for collecting metrics.
   *
   * @return The MetricCollector instance.
   */
  public MetricCollector getMetricCollector() {
    return configuration.getMetricCollector();
  }

  /**
   * Retrieves the default value for processing tables.
   *
   * @return The default boolean value for processing tables.
   */
  public boolean getProcessTablesDefaultValue() {
    return configuration.getProcessTablesDefaultValue();
  }

  /**
   * Gets the path for uploading debug files.
   *
   * @return A String representing the debug file upload path.
   */
  public String getDebugFileUploadPath() {
    return configuration.getDebugFileUploadPath();
  }

  /**
   * Loads a table by name.
   *
   * @param name The name of the table to load.
   * @return The loaded Table instance.
   */
  public Table getTable(String name) {
    return getTable(name, false);
  }

  /**
   * Loads a table by name, optionally refreshing the metadata.
   *
   * @param name The name of the table to load.
   * @param shouldRefresh Whether to refresh the table metadata.
   * @return The loaded Table instance.
   */
  public Table getTable(String name, boolean shouldRefresh) {
    TableIdentifier tableIdentifier = getTableIdentifier(name);
    Table table = null;
    if (shouldRefresh) {
      table = this.catalog.loadTable(tableIdentifier);
    }
    if (configuration.getCachingCatalogTableNames().contains(name)) {
      table = this.cachingCatalog.loadTable(tableIdentifier);
    } else if (configuration
        .getCachingCatalogNamespaces()
        .contains(tableIdentifier.namespace().toString())) {
      table = this.cachingCatalog.loadTable(tableIdentifier);
    } else {
      table = this.catalog.loadTable(tableIdentifier);
    }

    return table;
  }

  /**
   * Creates a new DuckDB instance with the given properties.
   *
   * @return A new Connection to the DuckDB instance.
   */
  public Connection createDuckDBInstance() {
    Connection instance = null;
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      instance = DriverManager.getConnection("jdbc:duckdb:");
    } catch (SQLException | ClassNotFoundException e) {
      throw new SwiftLakeException(e, "An error occurred while creating DuckDB instance.");
    }
    return instance;
  }

  private void configureDuckDB() {
    String tmpDir = configuration.getLocalDir() + "/duckdb_tmp_dir";
    new File(tmpDir).mkdirs();
    try (Statement stmt = duckDBConnection.createStatement()) {
      String query =
          String.format(
              "SET GLOBAL TimeZone=UTC; SET GLOBAL temp_directory='%s'; SET GLOBAL memory_limit='%dMiB'; SET GLOBAL threads TO %d; %s;"
                  + "SET allow_community_extensions=%s; SET autoinstall_known_extensions=%s; SET autoload_known_extensions=%s; SET lock_configuration=%s;",
              tmpDir,
              configuration.getMemoryLimitInMiB(),
              configuration.getThreads(),
              configuration.getTempStorageLimitInMiB() == null
                  ? ""
                  : String.format(
                      "SET GLOBAL max_temp_directory_size TO '%dMiB'",
                      configuration.getTempStorageLimitInMiB()),
              configuration.isAllowCommunityExtensions(),
              configuration.isAutoInstallKnownExtensions(),
              configuration.isAutoInstallKnownExtensions(),
              configuration.isLockDuckDBConfiguration());
      stmt.executeUpdate(query);
      LOGGER.info("DuckDB Configuration : " + query);

    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An error occurred while configuring DuckDB.");
    }
  }

  /**
   * Loads DuckDB extensions from the classpath.
   *
   * @param stmt The SQL statement object to execute queries.
   * @param basePath The base path in the classpath where extension files are located.
   * @param extensions A list of extension names to load.
   * @throws SwiftLakeException If an error occurs while loading the extensions.
   */
  public void loadDuckDBExtensionsFromClassPath(
      Statement stmt, String basePath, List<String> extensions) {
    try {
      String platform = null;
      try (ResultSet rs = stmt.executeQuery("PRAGMA platform")) {
        rs.next();
        platform = rs.getString(1);
        LOGGER.debug("Platform identified as {}", platform);
      }

      String fileSystemBasePath =
          FileUtil.concatPaths(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
      for (String extension : extensions) {
        String fileName = extension + ".duckdb_extension.gz";
        String sourcePath = FileUtil.concatPaths(basePath, platform, fileName);
        String targetPath = FileUtil.concatPaths(fileSystemBasePath, platform, fileName);
        URL extResource = SwiftLakeEngine.class.getResource(sourcePath);
        if (extResource == null) {
          throw new ValidationException("Unable to find the extension file %s", sourcePath);
        }
        new File(targetPath).mkdirs();
        try (final InputStream is = extResource.openStream()) {
          Files.copy(is, Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING);
        }
      }

      loadDuckDBExtensions(stmt, fileSystemBasePath, extensions);
    } catch (IOException | SQLException e) {
      throw new SwiftLakeException(
          e, "An exception occurred while loading DuckDB extensions from class path.");
    }
  }

  /**
   * Loads DuckDB extensions from the file system.
   *
   * @param stmt The SQL statement object to execute queries.
   * @param basePath The base path in the file system where extension files are located.
   * @param extensions A list of extension names to load.
   * @throws SwiftLakeException If an error occurs while loading the extensions.
   */
  public void loadDuckDBExtensions(Statement stmt, String basePath, List<String> extensions) {
    try {
      String platform = null;
      try (ResultSet rs = stmt.executeQuery("PRAGMA platform")) {
        rs.next();
        platform = rs.getString(1);
        LOGGER.debug("Platform identified as {}", platform);
      }

      for (String extension : extensions) {
        String path = FileUtil.concatPaths(basePath, platform, extension + ".duckdb_extension");
        if (!Files.exists(Paths.get(path))) {
          if (Files.exists(Paths.get(path + ".gz"))) {
            unzipFile(path + ".gz", path);
          } else {
            throw new ValidationException("Unable to find the extension file %s", path);
          }
        }
        stmt.execute(String.format("LOAD '%s';", path));
      }
    } catch (SQLException e) {
      throw new SwiftLakeException(e, "An exception occurred while loading DuckDB extensions.");
    }
  }

  private void unzipFile(String gzipFile, String newFile) {
    try (FileInputStream fis = new FileInputStream(gzipFile);
        GZIPInputStream gis = new GZIPInputStream(fis);
        FileOutputStream fos = new FileOutputStream(newFile); ) {

      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) != -1) {
        fos.write(buffer, 0, len);
      }
    } catch (IOException e) {
      throw new SwiftLakeException(e, "An error occurred while extracting the file %s.", gzipFile);
    }
  }

  /**
   * Creates a new DataSource for SwiftLake operations.
   *
   * @return A new SwiftLakeDataSource instance.
   */
  public DataSource createDataSource() {
    return new SwiftLakeDataSource(this, configuration.getProcessTablesDefaultValue());
  }

  /**
   * Creates a new DataSource for SwiftLake operations.
   *
   * @param processTables Whether to process tables when creating the DataSource.
   * @return A new SwiftLakeDataSource instance.
   */
  public DataSource createDataSource(boolean processTables) {
    return new SwiftLakeDataSource(this, processTables);
  }

  /**
   * Creates a SqlSessionFactory using the specified configuration path.
   *
   * @param configPath The path to the configuration file.
   * @return A new SqlSessionFactory instance.
   */
  public SqlSessionFactory createSqlSessionFactory(String configPath) {
    return new SwiftLakeSqlSessionFactory(
        this, configPath, configuration.getProcessTablesDefaultValue());
  }

  /**
   * Creates a SqlSessionFactory using the specified configuration path and processTables flag.
   *
   * @param configPath The path to the configuration file.
   * @param processTables A boolean flag indicating whether to process tables.
   * @return A new SqlSessionFactory instance.
   */
  public SqlSessionFactory createSqlSessionFactory(String configPath, boolean processTables) {
    return new SwiftLakeSqlSessionFactory(this, configPath, processTables);
  }

  /**
   * Creates a SwiftLakeMybatisConfiguration instance.
   *
   * @return A new SwiftLakeMybatisConfiguration instance.
   */
  public SwiftLakeMybatisConfiguration createMybatisConfiguration() {
    return new SwiftLakeMybatisConfiguration(this);
  }

  /**
   * Creates a SqlSessionFactory using the specified SwiftLakeMybatisConfiguration.
   *
   * @param configuration The SwiftLakeMybatisConfiguration to use.
   * @return A new SqlSessionFactory instance.
   */
  public SqlSessionFactory createSqlSessionFactory(SwiftLakeMybatisConfiguration configuration) {
    return new SwiftLakeSqlSessionFactory(
        configuration, this.configuration.getProcessTablesDefaultValue());
  }

  /**
   * Creates a SqlSessionFactory using the specified SwiftLakeMybatisConfiguration and processTables
   * flag.
   *
   * @param configuration The SwiftLakeMybatisConfiguration to use.
   * @param processTables A boolean flag indicating whether to process tables.
   * @return A new SqlSessionFactory instance.
   */
  public SqlSessionFactory createSqlSessionFactory(
      SwiftLakeMybatisConfiguration configuration, boolean processTables) {
    return new SwiftLakeSqlSessionFactory(configuration, processTables);
  }

  /**
   * Creates a new SwiftLakeConnection.
   *
   * @return A new SwiftLakeConnection instance.
   */
  public SwiftLakeConnection createConnection() {
    return createConnection(configuration.getProcessTablesDefaultValue());
  }

  /**
   * Creates a new SwiftLakeConnection with the option to process tables.
   *
   * @param processTables Whether to process tables referenced in SQL statements
   * @return A new SwiftLakeConnection instance.
   */
  public SwiftLakeConnection createConnection(boolean processTables) {
    return new SwiftLakeConnection(this, createDuckDBConnection(), processTables);
  }

  /**
   * Creates a new DuckDBConnection.
   *
   * @return A new DuckDBConnection instance.
   */
  public DuckDBConnection createDuckDBConnection() {
    try {
      if (maxConnectionsSemaphore == null
          || maxConnectionsSemaphore.tryAcquire(
              1, configuration.getConnectionCreationTimeoutInSeconds(), TimeUnit.SECONDS)) {
        return duplicateDuckDBConnection();
      } else {
        throw new ValidationException(
            "Connection creation timed out after %d seconds",
            configuration.getConnectionCreationTimeoutInSeconds());
      }
    } catch (Exception e) {
      if (e instanceof ValidationException validationException) {
        throw validationException;
      } else {
        throw new SwiftLakeException(e, "An exception occurred while creating new connection.");
      }
    }
  }

  private DuckDBConnection duplicateDuckDBConnection() throws SQLException {
    synchronized (this.duckDBConnection) {
      return (DuckDBConnection) this.duckDBConnection.duplicate();
    }
  }

  /**
   * Closes a DuckDBConnection and releases associated resources.
   *
   * @param conn The DuckDBConnection to close.
   * @throws SQLException If an error occurs while closing the connection.
   */
  public void closeDuckDBConnection(DuckDBConnection conn) throws SQLException {
    if (maxConnectionsSemaphore != null) {
      maxConnectionsSemaphore.release();
    }
    conn.close();
  }

  /**
   * Returns the ExecutorService used for partition writer operations.
   *
   * @return The ExecutorService for partition writers.
   */
  public ExecutorService getPartitionWriterThreadPool() {
    return partitionWriterThreadPool;
  }

  /**
   * Gets partition-level record counts for the specified table and partition data.
   *
   * @param tableName The name of the table.
   * @param partitionDataList The list of partition data to process.
   * @return A list of pairs containing partition data and record counts.
   */
  public List<Pair<PartitionData, Long>> getPartitionLevelRecordCounts(
      String tableName, List<PartitionData> partitionDataList) {
    return getPartitionLevelRecordCounts(this.getTable(tableName), partitionDataList);
  }

  /**
   * Gets partition-level record counts for the specified table and partition data.
   *
   * @param table The Table instance.
   * @param partitionDataList The list of partition data to process.
   * @return A list of pairs containing partition data and record counts.
   */
  public List<Pair<PartitionData, Long>> getPartitionLevelRecordCounts(
      Table table, List<PartitionData> partitionDataList) {
    return icebergScanExecutor.getPartitionLevelRecordCounts(table, partitionDataList);
  }

  /**
   * Gets partition-level record counts for the specified table and expression.
   *
   * @param table The Table instance.
   * @param expression Expression to filter on
   * @return A list of pairs containing partition data and record counts.
   */
  public List<Pair<PartitionData, Long>> getPartitionLevelRecordCounts(
      Table table, Expression expression) {
    return icebergScanExecutor.getPartitionLevelRecordCounts(table, expression);
  }

  /**
   * Retrieves DuckDB runtime metrics.
   *
   * @return A DuckDBRuntimeMetrics instance containing runtime metrics.
   */
  public DuckDBRuntimeMetrics getDuckDBRuntimeMetrics() {
    List<Map<String, Object>> metricsList = commonDao.getDuckDBRuntimeMetrics();
    if (metricsList.isEmpty()) {
      return null;
    }
    Map<String, Object> metrics = metricsList.get(0);
    return new DuckDBRuntimeMetrics(
        FileUtil.parseDataSizeString(metrics.get("memory_limit").toString()),
        FileUtil.parseDataSizeString(metrics.get("memory_usage").toString()),
        FileUtil.parseDataSizeString(metrics.get("max_temp_directory_size").toString()),
        (long) metrics.get("temp_files_size"));
  }

  /**
   * Initiates an INSERT operation into the specified table.
   *
   * @param tableName The name of the table to insert into.
   * @return A SetInputDataSql object to set input data for the INSERT operation.
   */
  public Insert.SetInputDataSql insertInto(String tableName) {
    return Insert.into(this, tableName);
  }

  /**
   * Initiates an INSERT operation into the specified table.
   *
   * @param table The Table object representing the table to insert into.
   * @return A SetInputDataSql object to set input data for the INSERT operation.
   */
  public Insert.SetInputDataSql insertInto(Table table) {
    return Insert.into(this, table);
  }

  /**
   * Initiates an INSERT operation into the specified table batch transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to insert into.
   * @return A SetInputDataSql object to set input data for the INSERT operation.
   */
  public Insert.SetInputDataSql insertInto(TableBatchTransaction tableBatchTransaction) {
    return Insert.into(this, tableBatchTransaction);
  }

  /**
   * Initiates an INSERT OVERWRITE operation for the specified table.
   *
   * @param tableName The name of the table to overwrite.
   * @return A SetOverwriteByFilter object to set overwrite filter for the INSERT OVERWRITE
   *     operation.
   */
  public Insert.SetOverwriteByFilter insertOverwrite(String tableName) {
    return Insert.overwrite(this, tableName);
  }

  /**
   * Initiates an INSERT OVERWRITE operation for the specified table.
   *
   * @param table The Table object representing the table to overwrite.
   * @return A SetOverwriteByFilter object to set overwrite filter for the INSERT OVERWRITE
   *     operation.
   */
  public Insert.SetOverwriteByFilter insertOverwrite(Table table) {
    return Insert.overwrite(this, table);
  }

  /**
   * Initiates an UPDATE operation on the specified table.
   *
   * @param tableName The name of the table to update.
   * @return A SetCondition object to set conditions for the UPDATE operation.
   */
  public Update.SetCondition update(String tableName) {
    return Update.on(this, tableName);
  }

  /**
   * Initiates an UPDATE operation on the specified table.
   *
   * @param table The Table object representing the table to update.
   * @return A SetCondition object to set conditions for the UPDATE operation.
   */
  public Update.SetCondition update(Table table) {
    return Update.on(this, table);
  }

  /**
   * Initiates an UPDATE operation on the specified table batch transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to update.
   * @return A SetCondition object to set conditions for the UPDATE operation.
   */
  public Update.SetCondition update(TableBatchTransaction tableBatchTransaction) {
    return Update.on(this, tableBatchTransaction);
  }

  /**
   * Initiates a DELETE operation on the specified table.
   *
   * @param tableName The name of the table to delete from.
   * @return A SetCondition object to set conditions for the DELETE operation.
   */
  public Delete.SetCondition deleteFrom(String tableName) {
    return Delete.from(this, tableName);
  }

  /**
   * Initiates a DELETE operation on the specified table.
   *
   * @param table The Table object representing the table to delete from.
   * @return A SetCondition object to set conditions for the DELETE operation.
   */
  public Delete.SetCondition deleteFrom(Table table) {
    return Delete.from(this, table);
  }

  /**
   * Initiates a DELETE operation on the specified table batch transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to delete from.
   * @return A SetCondition object to set conditions for the DELETE operation.
   */
  public Delete.SetCondition deleteFrom(TableBatchTransaction tableBatchTransaction) {
    return Delete.from(this, tableBatchTransaction);
  }

  /**
   * Initiates an SCD1 (Slowly Changing Dimension Type 1) merge operation on the specified table.
   *
   * @param tableName The name of the table to apply SCD1 changes to.
   * @return A SetTableFilter object to set table filter for the SCD1 merge operation.
   */
  public SCD1Merge.SetTableFilter applyChangesAsSCD1(String tableName) {
    return SCD1Merge.applyChanges(this, tableName);
  }

  /**
   * Initiates an SCD1 (Slowly Changing Dimension Type 1) merge operation on the specified table.
   *
   * @param table The Table object representing the table to apply SCD1 changes to.
   * @return A SetTableFilter object to set table filter for the SCD1 merge operation.
   */
  public SCD1Merge.SetTableFilter applyChangesAsSCD1(Table table) {
    return SCD1Merge.applyChanges(this, table);
  }

  /**
   * Initiates an SCD1 (Slowly Changing Dimension Type 1) merge operation on the specified table
   * batch transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to apply SCD1 changes to.
   * @return A SetTableFilter object to set table filter for the SCD1 merge operation.
   */
  public SCD1Merge.SetTableFilter applyChangesAsSCD1(TableBatchTransaction tableBatchTransaction) {
    return SCD1Merge.applyChanges(this, tableBatchTransaction);
  }

  /**
   * Initiates an SCD2 (Slowly Changing Dimension Type 2) merge operation on the specified table.
   *
   * @param tableName The name of the table to apply SCD2 changes to.
   * @return A SetTableFilter object to set table filter for the SCD2 merge operation.
   */
  public SCD2Merge.SetTableFilter applyChangesAsSCD2(String tableName) {
    return SCD2Merge.applyChanges(this, tableName);
  }

  /**
   * Initiates an SCD2 (Slowly Changing Dimension Type 2) merge operation on the specified table.
   *
   * @param table The Table object representing the table to apply SCD2 changes to.
   * @return A SetTableFilter object to set table filter for the SCD2 merge operation.
   */
  public SCD2Merge.SetTableFilter applyChangesAsSCD2(Table table) {
    return SCD2Merge.applyChanges(this, table);
  }

  /**
   * Initiates an SCD2 (Slowly Changing Dimension Type 2) merge operation on the specified table
   * batch transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to apply SCD2 changes to.
   * @return A SetTableFilter object to set table filter for the SCD2 merge operation.
   */
  public SCD2Merge.SetTableFilter applyChangesAsSCD2(TableBatchTransaction tableBatchTransaction) {
    return SCD2Merge.applyChanges(this, tableBatchTransaction);
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) to the specified table.
   *
   * @param tableName The name of the table to apply the snapshot to.
   * @return A SnapshotModeSetTableFilter object for further configuration.
   */
  public SCD2Merge.SnapshotModeSetTableFilter applySnapshotAsSCD2(String tableName) {
    return SCD2Merge.applySnapshot(this, tableName);
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) to the specified table.
   *
   * @param table The Table object to apply the snapshot to.
   * @return A SnapshotModeSetTableFilter object for further configuration.
   */
  public SCD2Merge.SnapshotModeSetTableFilter applySnapshotAsSCD2(Table table) {
    return SCD2Merge.applySnapshot(this, table);
  }

  /**
   * Applies a snapshot as SCD2 (Slowly Changing Dimension Type 2) to the specified table batch
   * transaction.
   *
   * @param tableBatchTransaction The TableBatchTransaction object to apply the snapshot to.
   * @return A SnapshotModeSetTableFilter object for further configuration.
   */
  public SCD2Merge.SnapshotModeSetTableFilter applySnapshotAsSCD2(
      TableBatchTransaction tableBatchTransaction) {
    return SCD2Merge.applySnapshot(this, tableBatchTransaction);
  }

  /**
   * Creates an internal SqlSessionFactory for data operations.
   *
   * @return SwiftLakeSqlSessionFactory instance
   * @throws SwiftLakeException if an error occurs during factory creation
   */
  private SwiftLakeSqlSessionFactory createInternalSqlSessionFactory() {
    InputStream inputStream = null;
    try {
      inputStream = Resources.getResourceAsStream("dao/mybatis-config.xml");
      XMLConfigBuilder parser =
          new XMLConfigBuilder(SwiftLakeMybatisConfiguration.class, inputStream, null, null);
      SwiftLakeMybatisConfiguration config = (SwiftLakeMybatisConfiguration) parser.parse();
      config.setSwiftLakeEngine(this);
      return new SwiftLakeSqlSessionFactory(config, false);
    } catch (Exception e) {
      throw new SwiftLakeException(e, "An error occurred while building SqlSessionFactory object.");
    } finally {
      ErrorContext.instance().reset();
      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        // Intentionally ignore.
      }
    }
  }

  /**
   * Creates a Timer for scheduled tasks.
   *
   * @return Timer instance
   */
  private Timer createTimer() {
    final ClassLoader prevContextCL = Thread.currentThread().getContextClassLoader();
    try {
      /*
      Scheduled tasks would not need to use .getContextClassLoader, so we just reset it to null
      */
      Thread.currentThread().setContextClassLoader(null);

      return new Timer("SwiftLake-Timer", true);
    } finally {
      Thread.currentThread().setContextClassLoader(prevContextCL);
    }
  }

  /**
   * Creates a new Builder instance for constructing a SwiftLakeEngine.
   *
   * @param applicationId The application ID to use for the SwiftLakeEngine.
   * @return A new Builder instance.
   */
  public static Builder builderFor(String applicationId) {
    return new Builder(applicationId);
  }

  /** Builder class for constructing SwiftLakeEngine instances. */
  public static class Builder {
    private final SwiftLakeConfiguration configuration;

    /**
     * Constructor for the Builder class.
     *
     * @param applicationId The application ID to use for the SwiftLakeEngine.
     */
    private Builder(String applicationId) {
      configuration = new SwiftLakeConfiguration();
      configuration.setApplicationId(applicationId);
      configuration.setCachingCatalogTableNames(new HashSet<>());
      configuration.setCachingCatalogNamespaces(new HashSet<>());
      configuration.setAllowCommunityExtensions(false);
      configuration.setAutoInstallKnownExtensions(false);
      configuration.setLockDuckDBConfiguration(true);
      configuration.setProcessTablesDefaultValue(true);
    }

    /**
     * Sets the catalog for the SwiftLakeEngine.
     *
     * @param catalog The Catalog instance to set.
     * @return This Builder instance.
     */
    public Builder catalog(Catalog catalog) {
      this.configuration.setCatalog(catalog);
      return this;
    }

    /**
     * Sets the local directory for the SwiftLakeEngine.
     *
     * @param localDir The local directory path.
     * @return This Builder instance.
     */
    public Builder localDir(String localDir) {
      this.configuration.setLocalDir(localDir);
      return this;
    }

    /**
     * Sets the memory limit in MiB for DuckDB within the SwiftLakeEngine.
     *
     * @param memoryLimitInMiB The memory limit in MiB for DuckDB.
     * @return This Builder instance.
     */
    public Builder memoryLimitInMiB(Integer memoryLimitInMiB) {
      if (memoryLimitInMiB == null) {
        return this;
      }
      ValidationException.check(memoryLimitInMiB > 0, "Memory limit value must be greater than 0.");
      this.configuration.setMemoryLimitInMiB(memoryLimitInMiB);
      return this;
    }

    /**
     * Sets the memory limit as a fraction of total system memory for DuckDB within the
     * SwiftLakeEngine. This is an alternative to setting memoryLimitInMiB.
     *
     * @param memoryLimitFraction The memory limit fraction for DuckDB (0 &lt; value &lt;= 1).
     * @return This Builder instance.
     */
    public Builder memoryLimitFraction(Double memoryLimitFraction) {
      if (memoryLimitFraction == null) {
        return this;
      }
      ValidationException.check(
          memoryLimitFraction > 0 && memoryLimitFraction <= 1,
          "Memory limit fraction value must be greater than 0 and less than or equal to 1");
      this.configuration.setMemoryLimitFraction(memoryLimitFraction);
      return this;
    }

    /**
     * Sets the number of threads for DuckDB to use within the SwiftLakeEngine.
     *
     * @param threads The number of threads for DuckDB.
     * @return This Builder instance.
     */
    public Builder threads(Integer threads) {
      if (threads == null) {
        return this;
      }
      ValidationException.check(threads > 0, "Threads value must be greater than 0.");
      this.configuration.setThreads(threads);
      return this;
    }

    /**
     * Sets the temporary storage limit in MiB for DuckDB within the SwiftLakeEngine.
     *
     * @param tempStorageLimitInMiB The temporary storage limit in MiB for DuckDB.
     * @return This Builder instance.
     */
    public Builder tempStorageLimitInMiB(Integer tempStorageLimitInMiB) {
      if (tempStorageLimitInMiB == null) {
        return this;
      }
      ValidationException.check(
          tempStorageLimitInMiB > 0, "Temporary storage limit value must be greater than 0.");
      this.configuration.setTempStorageLimitInMiB(tempStorageLimitInMiB);
      return this;
    }

    /**
     * Sets the MyBatis configuration path for the SwiftLakeEngine.
     *
     * @param mybatisConfigPath The MyBatis configuration path.
     * @return This Builder instance.
     */
    public Builder mybatisConfigPath(String mybatisConfigPath) {
      this.configuration.setMybatisConfigPath(mybatisConfigPath);
      return this;
    }

    /**
     * Sets the maximum number of partition writer threads for the SwiftLakeEngine.
     *
     * @param maxPartitionWriterThreads The maximum number of partition writer threads.
     * @return This Builder instance.
     */
    public Builder maxPartitionWriterThreads(int maxPartitionWriterThreads) {
      ValidationException.check(
          maxPartitionWriterThreads > 0, "Partition writer threads value must be greater than 0");
      this.configuration.setMaxPartitionWriterThreads(maxPartitionWriterThreads);
      return this;
    }

    /**
     * Configures the caching catalog for the SwiftLakeEngine.
     *
     * @param namespaces The list of namespaces to cache.
     * @param fullyQualifiedTableNames The list of fully qualified table names to cache.
     * @param expirationIntervalInSeconds The expiration interval in seconds for cached items.
     * @return This Builder instance.
     */
    public Builder cachingCatalog(
        List<String> namespaces,
        List<String> fullyQualifiedTableNames,
        int expirationIntervalInSeconds) {
      if (namespaces == null && fullyQualifiedTableNames == null) {
        return this;
      }
      ValidationException.check(
          expirationIntervalInSeconds > 0,
          "Caching catalog expiration seconds must be greater than 0");
      if (namespaces != null) {
        this.configuration.getCachingCatalogNamespaces().addAll(namespaces);
      }
      if (fullyQualifiedTableNames != null) {
        this.configuration.getCachingCatalogTableNames().addAll(fullyQualifiedTableNames);
      }
      this.configuration.setCachingCatalogExpirationIntervalInSeconds(expirationIntervalInSeconds);
      return this;
    }

    /**
     * Sets the total file size per scan limit in MiB for the SwiftLakeEngine.
     *
     * @param totalFileSizePerScanLimitInMiB The total file size per scan limit in MiB.
     * @return This Builder instance.
     */
    public Builder totalFileSizePerScanLimitInMiB(Integer totalFileSizePerScanLimitInMiB) {
      if (totalFileSizePerScanLimitInMiB == null) return this;
      ValidationException.check(
          totalFileSizePerScanLimitInMiB > 0,
          "Total file size per scan limit value must be greater than 0.");
      this.configuration.setTotalFileSizePerScanLimitInMiB(totalFileSizePerScanLimitInMiB);
      return this;
    }

    /**
     * Sets the maximum number of active connections for the SwiftLakeEngine.
     *
     * @param maxActiveConnections The maximum number of active connections.
     * @return This Builder instance.
     */
    public Builder maxActiveConnections(Integer maxActiveConnections) {
      if (maxActiveConnections == null) return this;
      ValidationException.check(
          maxActiveConnections > 0, "Maximum active connections value must be greater than 0.");
      this.configuration.setMaxActiveConnections(maxActiveConnections);
      return this;
    }

    /**
     * Sets the connection creation timeout in seconds for the SwiftLakeEngine.
     *
     * @param connectionCreationTimeoutInSeconds The connection creation timeout in seconds.
     * @return This Builder instance.
     */
    public Builder connectionCreationTimeoutInSeconds(Integer connectionCreationTimeoutInSeconds) {
      if (connectionCreationTimeoutInSeconds == null) return this;
      ValidationException.check(
          connectionCreationTimeoutInSeconds >= 0,
          "Connection creation timeout value must be greater than or equal to 0.");
      this.configuration.setConnectionCreationTimeoutInSeconds(connectionCreationTimeoutInSeconds);
      return this;
    }

    /**
     * Sets the query timeout in seconds for the SwiftLakeEngine.
     *
     * @param queryTimeoutInSeconds The query timeout in seconds.
     * @return This Builder instance.
     */
    public Builder queryTimeoutInSeconds(Integer queryTimeoutInSeconds) {
      if (queryTimeoutInSeconds == null) return this;
      ValidationException.check(
          queryTimeoutInSeconds >= 0, "Query timeout value must be greater than or equal to 0.");
      this.configuration.setQueryTimeoutInSeconds(queryTimeoutInSeconds);
      return this;
    }

    /**
     * Enables debug file upload for the SwiftLakeEngine.
     *
     * @param uploadPath The path for uploading debug files.
     * @return This Builder instance.
     */
    public Builder enableDebugFileUpload(String uploadPath) {
      this.configuration.setDebugFileUploadPath(uploadPath);
      return this;
    }

    /**
     * Sets the metric collector for the SwiftLakeEngine.
     *
     * @param metricCollector The MetricCollector instance to use.
     * @return This Builder instance.
     */
    public Builder metricCollector(MetricCollector metricCollector) {
      this.configuration.setMetricCollector(metricCollector);
      return this;
    }

    /**
     * Configures DuckDB extensions for the SwiftLakeEngine.
     *
     * @param allowCommunityExtensions Whether to allow community extensions.
     * @param autoInstallKnownExtensions Whether to auto-install known extensions.
     * @return This Builder instance.
     */
    public Builder configureDuckDBExtensions(
        boolean allowCommunityExtensions, boolean autoInstallKnownExtensions) {
      this.configuration.setAllowCommunityExtensions(allowCommunityExtensions);
      this.configuration.setAutoInstallKnownExtensions(autoInstallKnownExtensions);
      return this;
    }

    /**
     * Sets whether to lock the DuckDB configuration.
     *
     * @param lockDuckDBConfiguration true to lock the DuckDB configuration, false otherwise
     * @return the Builder instance for method chaining
     */
    public Builder lockDuckDBConfiguration(boolean lockDuckDBConfiguration) {
      this.configuration.setLockDuckDBConfiguration(lockDuckDBConfiguration);
      return this;
    }

    /**
     * Sets whether to process tables by default for the SwiftLakeEngine.
     *
     * @param processTablesDefaultValue True to process tables by default, false otherwise.
     * @return This Builder instance.
     */
    public Builder processTablesDefaultValue(boolean processTablesDefaultValue) {
      this.configuration.setProcessTablesDefaultValue(processTablesDefaultValue);
      return this;
    }

    /**
     * Sets whether to allow full table scans.
     *
     * @param allowFullTableScan true to allow full table scans, false otherwise
     * @return the Builder instance for method chaining
     */
    public Builder allowFullTableScan(boolean allowFullTableScan) {
      this.configuration.setAllowFullTableScan(allowFullTableScan);
      return this;
    }

    /**
     * Sets a custom configuration function for DuckDB within the SwiftLakeEngine. This function is
     * called before the DuckDB configuration is locked.
     *
     * @param configureFunc A function that takes a SwiftLakeEngine instance and returns a List of
     *     AutoCloseable. This function allows for final customization of DuckDB for ad hoc use
     *     cases. It can be used to set DuckDB-specific options or perform additional setup. The
     *     returned AutoCloseables will be managed by the engine and closed when appropriate.
     * @return This Builder instance for method chaining.
     */
    public Builder configure(Function<SwiftLakeEngine, List<AutoCloseable>> configureFunc) {
      this.configuration.setConfigureFunc(configureFunc);
      return this;
    }

    /**
     * Builds and returns a new SwiftLakeEngine instance.
     *
     * @return A new SwiftLakeEngine instance
     * @throws ValidationException if the catalog is null
     */
    public SwiftLakeEngine build() {
      ValidationException.checkNotNull(configuration.getCatalog(), "Catalog cannot be null.");
      return new SwiftLakeEngine(configuration);
    }
  }
}
