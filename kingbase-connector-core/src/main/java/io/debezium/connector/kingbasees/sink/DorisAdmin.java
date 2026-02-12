package io.debezium.connector.kingbasees.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Doris 侧管理职责：
 * 1) 启动前 drop/truncate
 * 2) 自动建库建表
 * 3) 自动补列
 */
final class DorisAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisAdmin.class);

    private final Connection dorisConnection;
    private final SyncConfig config;
    private final SourceAdmin sourceAdmin;

    DorisAdmin(Connection dorisConnection, SyncConfig config, SourceAdmin sourceAdmin) {
        this.dorisConnection = dorisConnection;
        this.config = config;
        this.sourceAdmin = sourceAdmin;
    }

    void applyStartupActions() throws SQLException {
        Set<SourceTableId> dropTargets = new LinkedHashSet<SourceTableId>();
        Set<SourceTableId> truncateTargets = new LinkedHashSet<SourceTableId>();

        dropTargets.addAll(config.dorisStartupDropTables);
        truncateTargets.addAll(config.dorisStartupTruncateTables);

        List<SourceTableId> includedTables = resolveIncludedTables();
        if (config.dorisStartupDropAllIncluded) {
            dropTargets.addAll(includedTables);
        }
        if (config.dorisStartupTruncateAllIncluded) {
            truncateTargets.addAll(includedTables);
        }
        LOGGER.info("[Doris管理] 启动动作准备完成：drop 目标数={}，truncate 目标数={}", dropTargets.size(), truncateTargets.size());

        for (SourceTableId table : dropTargets) {
            TargetTable target = config.route(table);
            ensureDatabase(target.getDatabase());
            exec("DROP TABLE IF EXISTS " + target.qualifiedName());
            LOGGER.info("[Doris管理] 已删除目标表：{}", target);
        }

        for (SourceTableId table : truncateTargets) {
            TargetTable target = config.route(table);
            ensureDatabase(target.getDatabase());
            if (!tableExists(target)) {
                continue;
            }
            exec("TRUNCATE TABLE " + target.qualifiedName());
            LOGGER.info("[Doris管理] 已清空目标表：{}", target);
        }
    }

    void ensureTargetTable(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
        if (config.dorisAutoCreateDatabase) {
            ensureDatabase(target.getDatabase());
        }

        boolean exists = tableExists(target);
        if (!exists) {
            if (!config.dorisAutoCreateTable) {
                throw new IllegalStateException("目标表不存在且未开启自动建表：" + target);
            }
            createTable(target, sourceMeta);
            return;
        }

        if (config.dorisAutoAddColumns) {
            addMissingColumns(target, sourceMeta);
        }
    }

    private List<SourceTableId> resolveIncludedTables() throws SQLException {
        if (!config.tableIncludeList.isEmpty()) {
            return config.tableIncludeList;
        }
        if (!config.schemaIncludeList.isEmpty()) {
            return sourceAdmin.listTablesBySchemas(config.schemaIncludeList);
        }
        return Collections.emptyList();
    }

    private void createTable(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
        StringJoiner columnsSql = new StringJoiner(",\n  ");
        for (SourceColumn column : sourceMeta.getColumns()) {
            columnsSql.add(SinkSupport.backtick(column.getName()) + " " + column.getDorisType() + (column.isNullable() ? " NULL" : " NOT NULL"));
        }
        if (isLogicalDeleteEnabled() && !sourceHasColumn(sourceMeta, config.logicalDeleteColumn)) {
            columnsSql.add(SinkSupport.backtick(config.logicalDeleteColumn) + " TINYINT NOT NULL DEFAULT 0");
        }

        String distCol = sourceMeta.getPrimaryKeys().isEmpty()
                ? sourceMeta.getColumns().get(0).getName()
                : sourceMeta.getPrimaryKeys().get(0);

        String keyClause;
        String properties;
        if (sourceMeta.getPrimaryKeys().isEmpty()) {
            keyClause = "DUPLICATE KEY(" + SinkSupport.backtick(distCol) + ")";
            properties = "\"replication_num\"=\"" + config.dorisReplicationNum + "\"";
        }
        else {
            String pkCols = sourceMeta.getPrimaryKeys().stream().map(SinkSupport::backtick).collect(Collectors.joining(", "));
            keyClause = "UNIQUE KEY(" + pkCols + ")";
            properties = "\"replication_num\"=\"" + config.dorisReplicationNum + "\", " +
                    "\"enable_unique_key_merge_on_write\"=\"true\"";
        }

        String sql = "CREATE TABLE IF NOT EXISTS " + target.qualifiedName() + " (\n  " +
                columnsSql.toString() + "\n)\n" +
                keyClause + "\n" +
                "DISTRIBUTED BY HASH(" + SinkSupport.backtick(distCol) + ") BUCKETS " + config.dorisBuckets + "\n" +
                "PROPERTIES(" + properties + ")";

        exec(sql);
        LOGGER.info("[Doris管理] 已创建目标表：{}", target);
    }

    private void addMissingColumns(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
        Set<String> existing = listTargetColumns(target);
        for (SourceColumn column : sourceMeta.getColumns()) {
            if (existing.contains(SinkSupport.lower(column.getName()))) {
                continue;
            }
            String sql = "ALTER TABLE " + target.qualifiedName() + " ADD COLUMN " +
                    SinkSupport.backtick(column.getName()) + " " + column.getDorisType() + (column.isNullable() ? " NULL" : " NOT NULL");
            exec(sql);
            LOGGER.info("[Doris管理] 已补充目标字段：{}.{}", target, column.getName());
        }

        if (isLogicalDeleteEnabled()
                && !sourceHasColumn(sourceMeta, config.logicalDeleteColumn)
                && !existing.contains(SinkSupport.lower(config.logicalDeleteColumn))) {
            String sql = "ALTER TABLE " + target.qualifiedName() + " ADD COLUMN " +
                    SinkSupport.backtick(config.logicalDeleteColumn) + " TINYINT NOT NULL DEFAULT 0";
            exec(sql);
            LOGGER.info("[Doris管理] 已补充逻辑删除标记字段：{}.{}", target, config.logicalDeleteColumn);
        }
    }

    private Set<String> listTargetColumns(TargetTable target) throws SQLException {
        String sql = "SHOW COLUMNS FROM " + target.qualifiedName();
        Set<String> cols = new LinkedHashSet<String>();
        try (Statement st = dorisConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                cols.add(SinkSupport.lower(rs.getString(1)));
            }
        }
        return cols;
    }

    private boolean tableExists(TargetTable target) throws SQLException {
        String db = target.getDatabase();
        String table = target.getTable().replace("'", "''");
        String sql = "SHOW TABLES FROM " + SinkSupport.backtick(db) + " LIKE '" + table + "'";
        try (Statement st = dorisConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        }
    }

    private void ensureDatabase(String database) throws SQLException {
        if (!config.dorisAutoCreateDatabase) {
            return;
        }
        exec("CREATE DATABASE IF NOT EXISTS " + SinkSupport.backtick(database));
    }

    private void exec(String sql) throws SQLException {
        try (Statement st = dorisConnection.createStatement()) {
            st.execute(sql);
        }
    }

    private boolean isLogicalDeleteEnabled() {
        return config.deleteSyncMode == DeleteSyncMode.LOGICAL_DELETE_SIGN;
    }

    private static boolean sourceHasColumn(SourceTableMeta sourceMeta, String columnName) {
        String target = SinkSupport.lower(columnName);
        for (SourceColumn column : sourceMeta.getColumns()) {
            if (target.equals(SinkSupport.lower(column.getName()))) {
                return true;
            }
        }
        return false;
    }
}
