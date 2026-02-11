package io.debezium.connector.kingbasees.demo.service;

import io.debezium.connector.kingbasees.demo.SyncProperties;
import io.debezium.connector.kingbasees.demo.model.ColumnMeta;
import io.debezium.connector.kingbasees.demo.model.TableId;
import io.debezium.connector.kingbasees.demo.model.TableMeta;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.debezium.connector.kingbasees.demo.util.DemoUtil.getNullableInt;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.lower;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.mapToDorisType;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.parseStringList;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.parseTableList;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceMetadataService {

    private final SyncProperties properties;

    public Connection openSourceConnection() throws SQLException {
        SyncProperties.Source source = properties.getSource();
        String url = "jdbc:kingbase8://" + source.getHost() + ":" + source.getPort() + "/" + source.getDatabase();
        return DriverManager.getConnection(url, source.getUser(), source.getPassword());
    }

    /**
     * 优先使用 include-tables；若为空则根据 include-schemas 扫描源表。
     */
    public List<TableId> resolveIncludedTables(Connection sourceConn) throws SQLException {
        List<TableId> fromTables = parseTableList(properties.getSource().getIncludeTables());
        if (!fromTables.isEmpty()) {
            return fromTables;
        }

        List<String> schemas = parseStringList(properties.getSource().getIncludeSchemas());
        if (schemas.isEmpty()) {
            return Collections.emptyList();
        }

        String placeholders = schemas.stream().map(s -> "?").collect(Collectors.joining(","));
        String sql = "SELECT table_schema, table_name " +
                "FROM information_schema.tables " +
                "WHERE table_type IN ('BASE TABLE', 'FOREIGN TABLE') " +
                "  AND table_schema IN (" + placeholders + ") " +
                "ORDER BY table_schema, table_name";

        List<TableId> tables = new ArrayList<TableId>();
        try (PreparedStatement ps = sourceConn.prepareStatement(sql)) {
            int i = 1;
            for (String schema : schemas) {
                ps.setString(i++, schema);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(new TableId(rs.getString("table_schema"), rs.getString("table_name")));
                }
            }
        }
        return tables;
    }

    /**
     * 启动前按配置初始化/重建 slot，避免人工手动维护。
     */
    public void initSlotIfNeeded(Connection sourceConn) throws SQLException {
        SyncProperties.Source source = properties.getSource();
        if (!source.isInitSlot()) {
            return;
        }

        boolean exists = slotExists(sourceConn, source.getSlotName());
        if (exists && source.isRecreateSlot()) {
            execSingleValue(sourceConn, "SELECT sys_drop_replication_slot(?)", source.getSlotName());
            exists = false;
            log.info("[Source] dropped slot {}", source.getSlotName());
        }

        if (!exists) {
            execSingleValue(sourceConn, "SELECT * FROM sys_create_logical_replication_slot(?, ?)", source.getSlotName(), source.getPluginName());
            log.info("[Source] created slot {} (plugin={})", source.getSlotName(), source.getPluginName());
        }
        else {
            log.info("[Source] slot already exists: {}", source.getSlotName());
        }
    }

    /**
     * 为 delete/update before 提供完整上下文，减少后续同步歧义。
     */
    public void applyReplicaIdentityFull(Connection sourceConn, List<TableId> tables) throws SQLException {
        for (TableId table : tables) {
            String sql = "ALTER TABLE " + table.asSourceQualified() + " REPLICA IDENTITY FULL";
            try (Statement st = sourceConn.createStatement()) {
                st.execute(sql);
                log.info("[Source] REPLICA IDENTITY FULL applied: {}", table);
            }
            catch (SQLException e) {
                if (properties.getSource().isReplicaIdentityFullFailFast()) {
                    throw e;
                }
                log.warn("[Source] REPLICA IDENTITY FULL failed: {}, reason={}", table, e.getMessage());
            }
        }
    }

    public List<TableMeta> loadTableMetas(Connection sourceConn, List<TableId> tableIds) throws SQLException {
        List<TableMeta> metas = new ArrayList<TableMeta>();
        for (TableId tableId : tableIds) {
            metas.add(loadTableMeta(sourceConn, tableId));
        }
        return metas;
    }

    public TableMeta loadTableMeta(Connection sourceConn, TableId tableId) throws SQLException {
        String colSql = "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale, " +
                "character_maximum_length, is_nullable " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position";

        List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
        try (PreparedStatement ps = sourceConn.prepareStatement(colSql)) {
            ps.setString(1, tableId.getSchema());
            ps.setString(2, tableId.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String col = rs.getString("column_name");
                    String dataType = lower(rs.getString("data_type"));
                    String udtName = lower(rs.getString("udt_name"));
                    Integer precision = getNullableInt(rs, "numeric_precision");
                    Integer scale = getNullableInt(rs, "numeric_scale");
                    Integer length = getNullableInt(rs, "character_maximum_length");
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
                    columns.add(new ColumnMeta(col, mapToDorisType(dataType, udtName, precision, scale, length), nullable));
                }
            }
        }

        if (columns.isEmpty()) {
            throw new IllegalStateException("No columns found for " + tableId);
        }

        List<String> primaryKeys = loadPrimaryKeys(sourceConn, tableId);
        return new TableMeta(tableId, columns, primaryKeys);
    }

    private List<String> loadPrimaryKeys(Connection sourceConn, TableId tableId) throws SQLException {
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

        List<String> primaryKeys = new ArrayList<String>();
        try (PreparedStatement ps = sourceConn.prepareStatement(pkSql)) {
            ps.setString(1, tableId.getSchema());
            ps.setString(2, tableId.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    primaryKeys.add(rs.getString("column_name"));
                }
            }
        }
        return primaryKeys;
    }

    private boolean slotExists(Connection sourceConn, String slotName) {
        String[] checks = new String[]{
                "SELECT 1 FROM sys_catalog.sys_replication_slots WHERE slot_name = ?",
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = ?"
        };
        for (String sql : checks) {
            try (PreparedStatement ps = sourceConn.prepareStatement(sql)) {
                ps.setString(1, slotName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return true;
                    }
                }
            }
            catch (Exception ignored) {
            }
        }
        return false;
    }

    private void execSingleValue(Connection conn, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            ps.execute();
        }
    }
}

