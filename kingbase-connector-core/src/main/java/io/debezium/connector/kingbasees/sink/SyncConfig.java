package io.debezium.connector.kingbasees.sink;

import org.slf4j.Logger;

import java.util.List;

/**
 * 同步任务运行配置。
 * 统一从 JVM 参数/环境变量读取，避免配置散落在多个类中。
 */
final class SyncConfig {

    // 基础运行参数
    final String connectorName;
    final String workDir;
    final String offsetFile;
    final String historyFile;
    final long offsetFlushMs;

    // Kingbase 源端参数
    final String kbHost;
    final String kbPort;
    final String kbUser;
    final String kbPassword;
    final String kbDb;
    final String kbServerId;
    final String kbServerName;
    final String kbPlugin;
    final String kbSnapshotMode;
    final String kbSlotName;
    final boolean kbSlotDropOnStop;
    final boolean kbSlotInit;
    final boolean kbSlotRecreate;
    final boolean kbReplicaIdentityFull;
    final boolean kbReplicaIdentityFullFailFast;
    final List<SourceTableId> kbReplicaIdentityFullTables;

    // 过滤范围
    final String tableIncludeListRaw;
    final String schemaIncludeListRaw;
    final List<SourceTableId> tableIncludeList;
    final List<String> schemaIncludeList;

    // Doris 目标端参数
    final String dorisHost;
    final String dorisPort;
    final String dorisUser;
    final String dorisPassword;
    final String dorisDatabase;
    final String dorisDatabasePrefix;
    final String dorisTablePrefix;
    final String dorisTableSuffix;
    final String dorisSchemaTableSeparator;
    final RouteMode dorisRouteMode;
    final boolean dorisAutoCreateDatabase;
    final boolean dorisAutoCreateTable;
    final boolean dorisAutoAddColumns;
    final int dorisBuckets;
    final int dorisReplicationNum;
    final boolean skipDeleteWithoutPk;
    final List<SourceTableId> dorisStartupDropTables;
    final List<SourceTableId> dorisStartupTruncateTables;
    final boolean dorisStartupDropAllIncluded;
    final boolean dorisStartupTruncateAllIncluded;

    // 事件增强与输出策略
    final OutputMode outputMode;
    final DeleteSyncMode deleteSyncMode;
    final String logicalDeleteColumn;
    final int enhancedBatchSize;
    final String enhancedOutputFile;
    final DeltaNullStrategy deltaNullStrategy;
    final boolean includeChangedFields;
    final boolean includeDeltas;
    final boolean tombstoneAsDelete;

    private SyncConfig(
            String connectorName,
            String workDir,
            String offsetFile,
            String historyFile,
            long offsetFlushMs,
            String kbHost,
            String kbPort,
            String kbUser,
            String kbPassword,
            String kbDb,
            String kbServerId,
            String kbServerName,
            String kbPlugin,
            String kbSnapshotMode,
            String kbSlotName,
            boolean kbSlotDropOnStop,
            boolean kbSlotInit,
            boolean kbSlotRecreate,
            boolean kbReplicaIdentityFull,
            boolean kbReplicaIdentityFullFailFast,
            List<SourceTableId> kbReplicaIdentityFullTables,
            String tableIncludeListRaw,
            String schemaIncludeListRaw,
            List<SourceTableId> tableIncludeList,
            List<String> schemaIncludeList,
            String dorisHost,
            String dorisPort,
            String dorisUser,
            String dorisPassword,
            String dorisDatabase,
            String dorisDatabasePrefix,
            String dorisTablePrefix,
            String dorisTableSuffix,
            String dorisSchemaTableSeparator,
            RouteMode dorisRouteMode,
            boolean dorisAutoCreateDatabase,
            boolean dorisAutoCreateTable,
            boolean dorisAutoAddColumns,
            int dorisBuckets,
            int dorisReplicationNum,
            boolean skipDeleteWithoutPk,
            List<SourceTableId> dorisStartupDropTables,
            List<SourceTableId> dorisStartupTruncateTables,
            boolean dorisStartupDropAllIncluded,
            boolean dorisStartupTruncateAllIncluded,
            OutputMode outputMode,
            DeleteSyncMode deleteSyncMode,
            String logicalDeleteColumn,
            int enhancedBatchSize,
            String enhancedOutputFile,
            DeltaNullStrategy deltaNullStrategy,
            boolean includeChangedFields,
            boolean includeDeltas,
            boolean tombstoneAsDelete) {
        this.connectorName = connectorName;
        this.workDir = workDir;
        this.offsetFile = offsetFile;
        this.historyFile = historyFile;
        this.offsetFlushMs = offsetFlushMs;

        this.kbHost = kbHost;
        this.kbPort = kbPort;
        this.kbUser = kbUser;
        this.kbPassword = kbPassword;
        this.kbDb = kbDb;
        this.kbServerId = kbServerId;
        this.kbServerName = kbServerName;
        this.kbPlugin = kbPlugin;
        this.kbSnapshotMode = kbSnapshotMode;
        this.kbSlotName = kbSlotName;
        this.kbSlotDropOnStop = kbSlotDropOnStop;
        this.kbSlotInit = kbSlotInit;
        this.kbSlotRecreate = kbSlotRecreate;
        this.kbReplicaIdentityFull = kbReplicaIdentityFull;
        this.kbReplicaIdentityFullFailFast = kbReplicaIdentityFullFailFast;
        this.kbReplicaIdentityFullTables = kbReplicaIdentityFullTables;

        this.tableIncludeListRaw = tableIncludeListRaw;
        this.schemaIncludeListRaw = schemaIncludeListRaw;
        this.tableIncludeList = tableIncludeList;
        this.schemaIncludeList = schemaIncludeList;

        this.dorisHost = dorisHost;
        this.dorisPort = dorisPort;
        this.dorisUser = dorisUser;
        this.dorisPassword = dorisPassword;
        this.dorisDatabase = dorisDatabase;
        this.dorisDatabasePrefix = dorisDatabasePrefix;
        this.dorisTablePrefix = dorisTablePrefix;
        this.dorisTableSuffix = dorisTableSuffix;
        this.dorisSchemaTableSeparator = dorisSchemaTableSeparator;
        this.dorisRouteMode = dorisRouteMode;
        this.dorisAutoCreateDatabase = dorisAutoCreateDatabase;
        this.dorisAutoCreateTable = dorisAutoCreateTable;
        this.dorisAutoAddColumns = dorisAutoAddColumns;
        this.dorisBuckets = dorisBuckets;
        this.dorisReplicationNum = dorisReplicationNum;
        this.skipDeleteWithoutPk = skipDeleteWithoutPk;
        this.dorisStartupDropTables = dorisStartupDropTables;
        this.dorisStartupTruncateTables = dorisStartupTruncateTables;
        this.dorisStartupDropAllIncluded = dorisStartupDropAllIncluded;
        this.dorisStartupTruncateAllIncluded = dorisStartupTruncateAllIncluded;

        this.outputMode = outputMode;
        this.deleteSyncMode = deleteSyncMode;
        this.logicalDeleteColumn = logicalDeleteColumn;
        this.enhancedBatchSize = enhancedBatchSize;
        this.enhancedOutputFile = enhancedOutputFile;
        this.deltaNullStrategy = deltaNullStrategy;
        this.includeChangedFields = includeChangedFields;
        this.includeDeltas = includeDeltas;
        this.tombstoneAsDelete = tombstoneAsDelete;
    }

    static SyncConfig load() {
        String workDir = SinkSupport.getSetting("sync.work.dir", "SYNC_WORK_DIR", "/tmp/debezium/kingbase-doris");
        String offsetFile = SinkSupport.getSetting("sync.offset.file", "SYNC_OFFSET_FILE", workDir + "/offset.dat");
        String historyFile = SinkSupport.getSetting("sync.history.file", "SYNC_HISTORY_FILE", workDir + "/history.dat");

        String tableIncludeRaw = SinkSupport.normalizeCsvList(SinkSupport.getSetting("kb.tables", "KB_TABLES", ""));
        String schemaIncludeRaw = SinkSupport.normalizeCsvList(SinkSupport.getSetting("kb.schemas", "KB_SCHEMAS", ""));

        return new SyncConfig(
                SinkSupport.getSetting("sync.connector.name", "SYNC_CONNECTOR_NAME", "kingbase-doris-sync"),
                workDir,
                offsetFile,
                historyFile,
                SinkSupport.parseLong(SinkSupport.getSetting("sync.offset.flush.ms", "SYNC_OFFSET_FLUSH_MS", "10000"), 10000L),
                SinkSupport.getSetting("kb.host", "KB_HOST", "127.0.0.1"),
                SinkSupport.getSetting("kb.port", "KB_PORT", "54321"),
                SinkSupport.getSetting("kb.user", "KB_USER", "kingbase"),
                SinkSupport.getSetting("kb.password", "KB_PASSWORD", "123456"),
                SinkSupport.getSetting("kb.db", "KB_DB", "test"),
                SinkSupport.getSetting("kb.server.id", "KB_SERVER_ID", "54001"),
                SinkSupport.getSetting("kb.server.name", "KB_SERVER_NAME", "kingbase-server"),
                SinkSupport.getSetting("kb.plugin", "KB_PLUGIN_NAME", "decoderbufs"),
                SinkSupport.getSetting("kb.snapshot.mode", "KB_SNAPSHOT_MODE", "initial"),
                SinkSupport.getSetting("kb.slot.name", "KB_SLOT_NAME", "dbz_kingbase_slot"),
                SinkSupport.parseBoolean(SinkSupport.getSetting("kb.slot.drop.on.stop", "KB_SLOT_DROP_ON_STOP", "false")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("kb.slot.init", "KB_SLOT_INIT", "true")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("kb.slot.recreate", "KB_SLOT_RECREATE", "false")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("kb.replica.identity.full", "KB_REPLICA_IDENTITY_FULL", "false")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("kb.replica.identity.full.fail-fast", "KB_REPLICA_IDENTITY_FULL_FAIL_FAST", "false")),
                SinkSupport.parseTableList(SinkSupport.normalizeCsvList(SinkSupport.getSetting("kb.replica.identity.full.tables", "KB_REPLICA_IDENTITY_FULL_TABLES", ""))),
                tableIncludeRaw,
                schemaIncludeRaw,
                SinkSupport.parseTableList(tableIncludeRaw),
                SinkSupport.parseStringList(schemaIncludeRaw),
                SinkSupport.getSetting("doris.host", "DORIS_HOST", "127.0.0.1"),
                SinkSupport.getSetting("doris.port", "DORIS_PORT", "9030"),
                SinkSupport.getSetting("doris.user", "DORIS_USER", "root"),
                SinkSupport.getSetting("doris.password", "DORIS_PASSWORD", ""),
                SinkSupport.getSetting("doris.database", "DORIS_DATABASE", "cdc"),
                SinkSupport.getSetting("doris.database.prefix", "DORIS_DATABASE_PREFIX", "cdc_"),
                SinkSupport.getSetting("doris.table.prefix", "DORIS_TABLE_PREFIX", ""),
                SinkSupport.getSetting("doris.table.suffix", "DORIS_TABLE_SUFFIX", ""),
                SinkSupport.getSetting("doris.schema.table.separator", "DORIS_SCHEMA_TABLE_SEPARATOR", "__"),
                parseRouteMode(SinkSupport.getSetting("doris.route.mode", "DORIS_ROUTE_MODE", "schema_table")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.auto.create.database", "DORIS_AUTO_CREATE_DATABASE", "true")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.auto.create.table", "DORIS_AUTO_CREATE_TABLE", "true")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.auto.add.columns", "DORIS_AUTO_ADD_COLUMNS", "true")),
                SinkSupport.parseInt(SinkSupport.getSetting("doris.buckets", "DORIS_BUCKETS", "10"), 10),
                SinkSupport.parseInt(SinkSupport.getSetting("doris.replication.num", "DORIS_REPLICATION_NUM", "1"), 1),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.skip.delete.without.pk", "DORIS_SKIP_DELETE_WITHOUT_PK", "true")),
                SinkSupport.parseTableList(SinkSupport.normalizeCsvList(SinkSupport.getSetting("doris.startup.drop.tables", "DORIS_STARTUP_DROP_TABLES", ""))),
                SinkSupport.parseTableList(SinkSupport.normalizeCsvList(SinkSupport.getSetting("doris.startup.truncate.tables", "DORIS_STARTUP_TRUNCATE_TABLES", ""))),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.startup.drop.all.included", "DORIS_STARTUP_DROP_ALL_INCLUDED", "false")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("doris.startup.truncate.all.included", "DORIS_STARTUP_TRUNCATE_ALL_INCLUDED", "false")),
                OutputMode.fromCode(SinkSupport.getSetting("sync.output.mode", "SYNC_OUTPUT_MODE", "jdbc_dml")),
                DeleteSyncMode.fromCode(SinkSupport.getSetting("sync.delete.mode", "SYNC_DELETE_MODE", "physical_delete")),
                SinkSupport.getSetting("doris.logical.delete.column", "DORIS_LOGICAL_DELETE_COLUMN", "__DORIS_DELETE_SIGN__"),
                SinkSupport.parseInt(SinkSupport.getSetting("sync.enhanced.batch.size", "SYNC_ENHANCED_BATCH_SIZE", "1000"), 1000),
                SinkSupport.getSetting("sync.enhanced.output.file", "SYNC_ENHANCED_OUTPUT_FILE", ""),
                DeltaNullStrategy.fromCode(SinkSupport.getSetting("sync.delta.null.strategy", "SYNC_DELTA_NULL_STRATEGY", "skip")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("sync.changed.fields.enabled", "SYNC_CHANGED_FIELDS_ENABLED", "true")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("sync.deltas.enabled", "SYNC_DELTAS_ENABLED", "true")),
                SinkSupport.parseBoolean(SinkSupport.getSetting("sync.tombstone.as.delete", "SYNC_TOMBSTONE_AS_DELETE", "false"))
        );
    }

    String sourceJdbcUrl() {
        return "jdbc:kingbase8://" + kbHost + ":" + kbPort + "/" + kbDb;
    }

    String dorisJdbcUrl() {
        return "jdbc:mysql://" + dorisHost + ":" + dorisPort +
                "/?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true";
    }

    TargetTable route(SourceTableId sourceTableId) {
        if (dorisRouteMode == RouteMode.SCHEMA_AS_DB) {
            String db = SinkSupport.sanitizeName(dorisDatabasePrefix + sourceTableId.getSchema());
            String table = SinkSupport.sanitizeName(dorisTablePrefix + sourceTableId.getTable() + dorisTableSuffix);
            return new TargetTable(db, table);
        }

        String db = SinkSupport.sanitizeName(dorisDatabase);
        String tableName = sourceTableId.getSchema() + dorisSchemaTableSeparator + sourceTableId.getTable();
        tableName = dorisTablePrefix + tableName + dorisTableSuffix;
        return new TargetTable(db, SinkSupport.sanitizeName(tableName));
    }

    void printSummary(Logger logger) {
        logger.info("[同步引擎] ===== Kingbase 到 Doris 同步配置 =====");
        logger.info("[同步引擎] 源端地址={}:{}/{}, 复制槽={}, 快照模式={}", kbHost, kbPort, kbDb, kbSlotName, kbSnapshotMode);
        logger.info("[同步引擎] 源表范围={}", tableIncludeListRaw.isEmpty() ? "<全部>" : tableIncludeListRaw);
        logger.info("[同步引擎] 源 schema 范围={}", schemaIncludeListRaw.isEmpty() ? "<全部>" : schemaIncludeListRaw);
        logger.info("[同步引擎] 目标端地址={}:{}, 路由模式={}", dorisHost, dorisPort, describeRouteMode(dorisRouteMode));
        logger.info("[同步引擎] Doris 自动建库/建表={}/{}, 自动补列={}",
                dorisAutoCreateDatabase, dorisAutoCreateTable, dorisAutoAddColumns);
        logger.info("[同步引擎] 启动时 drop 表={}, truncate 表={}",
                SinkSupport.joinTables(dorisStartupDropTables), SinkSupport.joinTables(dorisStartupTruncateTables));
        logger.info("[同步引擎] 启动时对纳入范围表执行 drop={}, truncate={}",
                dorisStartupDropAllIncluded, dorisStartupTruncateAllIncluded);

        logger.info("[同步引擎] 输出模式={}，删除策略={}，逻辑删除列={}",
                outputMode.getCode(), deleteSyncMode.getCode(), logicalDeleteColumn);
        logger.info("[同步引擎] 增强输出批次={}，输出文件={}", enhancedBatchSize,
                SinkSupport.isBlank(enhancedOutputFile) ? "<未配置>" : enhancedOutputFile);
        logger.info("[同步引擎] changed_fields={}，deltas={}，delta空值策略={}，tombstone按删除处理={}",
                includeChangedFields, includeDeltas, deltaNullStrategy.getCode(), tombstoneAsDelete);
    }

    private static String describeRouteMode(RouteMode mode) {
        if (mode == RouteMode.SCHEMA_AS_DB) {
            return "schema_as_db（源 schema -> 目标库名）";
        }
        return "schema_table（固定库 + schema__table）";
    }

    private static RouteMode parseRouteMode(String value) {
        String v = SinkSupport.lower(value);
        if ("schema_as_db".equals(v)) {
            return RouteMode.SCHEMA_AS_DB;
        }
        return RouteMode.SCHEMA_TABLE;
    }
}
