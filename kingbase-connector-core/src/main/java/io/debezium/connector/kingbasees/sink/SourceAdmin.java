package io.debezium.connector.kingbasees.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 源端管理职责：
 * 1) 复制槽管理
 * 2) REPLICA IDENTITY FULL 设置
 * 3) 源表结构元数据加载
 */
final class SourceAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceAdmin.class);

    private final Connection connection;
    private final SyncConfig config;
    private final Map<SourceTableId, SourceTableMeta> metaCache = new ConcurrentHashMap<SourceTableId, SourceTableMeta>();

    SourceAdmin(Connection connection, SyncConfig config) {
        this.connection = connection;
        this.config = config;
    }

    void initSlotIfNeeded() throws SQLException {
        if (!config.kbSlotInit) {
            return;
        }

        boolean exists = slotExists(config.kbSlotName);
        if (exists && config.kbSlotRecreate) {
            execSingleValue("SELECT sys_drop_replication_slot(?)", config.kbSlotName);
            exists = false;
            LOGGER.info("[源端管理] 已删除复制槽：{}", config.kbSlotName);
        }

        if (!exists) {
            execSingleValue("SELECT * FROM sys_create_logical_replication_slot(?, ?)", config.kbSlotName, config.kbPlugin);
            LOGGER.info("[源端管理] 已创建复制槽：{}，插件={}", config.kbSlotName, config.kbPlugin);
        }
        else {
            LOGGER.info("[源端管理] 复制槽已存在：{}", config.kbSlotName);
        }
    }

    void applyReplicaIdentityFullIfNeeded() throws SQLException {
        if (!config.kbReplicaIdentityFull) {
            return;
        }

        List<SourceTableId> tables = resolveReplicaIdentityTargets();
        if (tables.isEmpty()) {
            LOGGER.info("[源端管理] 已启用 REPLICA IDENTITY FULL，但未解析出可执行的表");
            return;
        }

        for (SourceTableId tableId : tables) {
            String sql = "ALTER TABLE " + tableId.toQuotedName() + " REPLICA IDENTITY FULL";
            try (Statement st = connection.createStatement()) {
                st.execute(sql);
                LOGGER.info("[源端管理] 已设置 REPLICA IDENTITY FULL：{}", tableId);
            }
            catch (SQLException e) {
                if (config.kbReplicaIdentityFullFailFast) {
                    throw e;
                }
                LOGGER.warn("[源端管理] 设置 REPLICA IDENTITY FULL 失败：{}，原因={}", tableId, e.getMessage());
            }
        }
    }

    SourceTableMeta loadTableMeta(SourceTableId tableId) throws SQLException {
        SourceTableMeta cached = metaCache.get(tableId);
        if (cached != null) {
            return cached;
        }

        String colSql = "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale, " +
                "character_maximum_length, is_nullable " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position";

        List<SourceColumn> columns = new ArrayList<SourceColumn>();
        try (PreparedStatement ps = connection.prepareStatement(colSql)) {
            ps.setString(1, tableId.getSchema());
            ps.setString(2, tableId.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("column_name");
                    String dataType = SinkSupport.lower(rs.getString("data_type"));
                    String udtName = SinkSupport.lower(rs.getString("udt_name"));
                    Integer precision = SinkSupport.toInteger(rs, "numeric_precision");
                    Integer scale = SinkSupport.toInteger(rs, "numeric_scale");
                    Integer length = SinkSupport.toInteger(rs, "character_maximum_length");
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
                    String dorisType = SinkSupport.mapToDorisType(dataType, udtName, precision, scale, length);
                    columns.add(new SourceColumn(name, dataType, udtName, dorisType, nullable));
                }
            }
        }

        if (columns.isEmpty()) {
            throw new IllegalStateException("源表不存在可用字段：" + tableId);
        }

        List<String> primaryKeys = new ArrayList<String>();
        String pkSql = "SELECT kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "  ON tc.constraint_name = kcu.constraint_name " +
                " AND tc.table_schema = kcu.table_schema " +
                " AND tc.table_name = kcu.table_name " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "  AND tc.table_schema = ? " +
                "  AND tc.table_name = ? " +
                "ORDER BY kcu.ordinal_position";

        try (PreparedStatement ps = connection.prepareStatement(pkSql)) {
            ps.setString(1, tableId.getSchema());
            ps.setString(2, tableId.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    primaryKeys.add(rs.getString("column_name"));
                }
            }
        }

        SourceTableMeta meta = new SourceTableMeta(tableId, columns, primaryKeys);
        metaCache.put(tableId, meta);
        LOGGER.info("[源端管理] 源表元数据加载完成：{}，字段数={}，主键数={}", tableId, columns.size(), primaryKeys.size());
        return meta;
    }

    List<SourceTableId> listTablesBySchemas(List<String> schemas) throws SQLException {
        if (schemas.isEmpty()) {
            return Collections.emptyList();
        }

        String placeholders = schemas.stream().map(s -> "?").collect(Collectors.joining(", "));
        String sql = "SELECT table_schema, table_name " +
                "FROM information_schema.tables " +
                "WHERE table_type IN ('BASE TABLE', 'FOREIGN TABLE') " +
                "  AND table_schema IN (" + placeholders + ") " +
                "ORDER BY table_schema, table_name";

        List<SourceTableId> tables = new ArrayList<SourceTableId>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int i = 1;
            for (String schema : schemas) {
                ps.setString(i++, schema);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(new SourceTableId(rs.getString("table_schema"), rs.getString("table_name")));
                }
            }
        }
        return tables;
    }

    private List<SourceTableId> resolveReplicaIdentityTargets() throws SQLException {
        if (!config.kbReplicaIdentityFullTables.isEmpty()) {
            return config.kbReplicaIdentityFullTables;
        }
        if (!config.tableIncludeList.isEmpty()) {
            return config.tableIncludeList;
        }
        if (!config.schemaIncludeList.isEmpty()) {
            return listTablesBySchemas(config.schemaIncludeList);
        }
        return Collections.emptyList();
    }

    private boolean slotExists(String slotName) throws SQLException {
        String[] checks = new String[]{
                "SELECT 1 FROM sys_catalog.sys_replication_slots WHERE slot_name = ?",
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = ?"
        };
        for (String sql : checks) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, slotName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return true;
                    }
                }
            }
            catch (SQLException ignored) {
            }
        }
        return false;
    }

    private void execSingleValue(String sql, Object... values) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                ps.setObject(i + 1, values[i]);
            }
            ps.execute();
        }
    }
}
