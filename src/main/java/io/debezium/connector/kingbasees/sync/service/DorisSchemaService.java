package io.debezium.connector.kingbasees.sync.service;

import io.debezium.connector.kingbasees.sync.SyncProperties;
import io.debezium.connector.kingbasees.sync.model.ColumnMeta;
import io.debezium.connector.kingbasees.sync.model.DorisTable;
import io.debezium.connector.kingbasees.sync.model.TableId;
import io.debezium.connector.kingbasees.sync.model.TableMeta;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.debezium.connector.kingbasees.sync.util.SyncUtil.lower;
import static io.debezium.connector.kingbasees.sync.util.SyncUtil.sanitizeName;

@Slf4j
@Service
@RequiredArgsConstructor
public class DorisSchemaService {

    private final SyncProperties properties;

    public Connection openDorisConnection() throws SQLException {
        SyncProperties.Doris doris = properties.getDoris();
        return DriverManager.getConnection(doris.getJdbcUrl(), doris.getUser(), doris.getPassword());
    }

    /**
     * 根据配置把源表映射到目标 Doris 库表。
     */
    public DorisTable route(TableId sourceTable) {
        SyncProperties.Doris doris = properties.getDoris();
        String routeMode = lower(doris.getRouteMode());
        if ("schema_as_db".equals(routeMode)) {
            return new DorisTable(
                    sanitizeName(doris.getDatabasePrefix() + sourceTable.getSchema()),
                    sanitizeName(doris.getTablePrefix() + sourceTable.getTable() + doris.getTableSuffix())
            );
        }
        String table = doris.getTablePrefix() + sourceTable.getSchema() + doris.getSchemaTableSeparator() + sourceTable.getTable() + doris.getTableSuffix();
        return new DorisTable(sanitizeName(doris.getDatabase()), sanitizeName(table));
    }

    public void bootstrapTables(Connection dorisConn, List<TableMeta> metas) throws SQLException {
        if (!properties.getDoris().isAutoCreateTables()) {
            log.info("[Doris] auto-create disabled, skip table bootstrap");
            return;
        }

        for (TableMeta meta : metas) {
            DorisTable target = route(meta.getTableId());
            exec(dorisConn, "CREATE DATABASE IF NOT EXISTS " + target.dbQuoted());

            if (properties.getDoris().isDropTablesBeforeSync()) {
                exec(dorisConn, "DROP TABLE IF EXISTS " + target.qualified());
                log.info("[Doris] dropped table before sync: {}", target);
            }

            createOrAlterTable(dorisConn, target, meta);
        }
    }

    private void createOrAlterTable(Connection dorisConn, DorisTable target, TableMeta meta) throws SQLException {
        if (!tableExists(dorisConn, target)) {
            createTable(dorisConn, target, meta);
            return;
        }
        addMissingColumns(dorisConn, target, meta);
    }

    private void createTable(Connection dorisConn, DorisTable target, TableMeta meta) throws SQLException {
        StringJoiner columnSql = new StringJoiner(",\n  ");
        for (ColumnMeta c : meta.getColumns()) {
            columnSql.add(quote(c.getName()) + " " + c.getDorisType() + (c.isNullable() ? " NULL" : " NOT NULL"));
        }

        String distCol = meta.getPrimaryKeys().isEmpty() ? meta.getColumns().get(0).getName() : meta.getPrimaryKeys().get(0);
        String keyClause;
        String propertiesClause;
        if (meta.getPrimaryKeys().isEmpty()) {
            keyClause = "DUPLICATE KEY(" + quote(distCol) + ")";
            propertiesClause = "\"replication_num\"=\"" + properties.getDoris().getReplicationNum() + "\"";
        }
        else {
            String pkColumns = meta.getPrimaryKeys().stream().map(DorisSchemaService::quote).collect(Collectors.joining(", "));
            keyClause = "UNIQUE KEY(" + pkColumns + ")";
            propertiesClause = "\"replication_num\"=\"" + properties.getDoris().getReplicationNum() + "\", " +
                    "\"enable_unique_key_merge_on_write\"=\"true\"";
        }

        String createSql = "CREATE TABLE IF NOT EXISTS " + target.qualified() + " (\n  " +
                columnSql.toString() + "\n)\n" +
                keyClause + "\n" +
                "DISTRIBUTED BY HASH(" + quote(distCol) + ") BUCKETS " + properties.getDoris().getBuckets() + "\n" +
                "PROPERTIES(" + propertiesClause + ")";
        exec(dorisConn, createSql);
        log.info("[Doris] created table: {}", target);
    }

    private void addMissingColumns(Connection dorisConn, DorisTable target, TableMeta meta) throws SQLException {
        Set<String> existing = listColumns(dorisConn, target);
        for (ColumnMeta c : meta.getColumns()) {
            if (existing.contains(lower(c.getName()))) {
                continue;
            }
            String alter = "ALTER TABLE " + target.qualified() + " ADD COLUMN " +
                    quote(c.getName()) + " " + c.getDorisType() + (c.isNullable() ? " NULL" : " NOT NULL");
            exec(dorisConn, alter);
            log.info("[Doris] added missing column: {}.{}", target, c.getName());
        }
    }

    private boolean tableExists(Connection dorisConn, DorisTable table) throws SQLException {
        String sql = "SHOW TABLES FROM " + table.dbQuoted() + " LIKE '" + table.getTable().replace("'", "''") + "'";
        try (Statement st = dorisConn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        }
    }

    private Set<String> listColumns(Connection dorisConn, DorisTable table) throws SQLException {
        String sql = "SHOW COLUMNS FROM " + table.qualified();
        Set<String> columns = new LinkedHashSet<String>();
        try (Statement st = dorisConn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                columns.add(lower(rs.getString(1)));
            }
        }
        return columns;
    }

    private void exec(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
