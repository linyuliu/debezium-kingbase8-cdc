package io.debezium.connector.kingbasees.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.kingbasees.PostgresConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class KingbaseToDorisSyncApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KingbaseToDorisSyncApp() {
    }

    public static void main(String[] args) throws Exception {
        SyncConfig config = SyncConfig.load();
        config.printSummary();
        ensureWorkDir(config.workDir);

        Class.forName("cn.com.kingbase.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection sourceConnection = DriverManager.getConnection(
                config.sourceJdbcUrl(),
                config.kbUser,
                config.kbPassword
        );
        Connection dorisConnection = DriverManager.getConnection(
                config.dorisJdbcUrl(),
                config.dorisUser,
                config.dorisPassword
        );

        DebeziumEngine<ChangeEvent<String, String>> engine = null;
        ExecutorService executor = null;
        SyncWriter writer = null;
        try {
            SourceAdmin sourceAdmin = new SourceAdmin(sourceConnection, config);
            DorisAdmin dorisAdmin = new DorisAdmin(dorisConnection, config, sourceAdmin);

            sourceAdmin.initSlotIfNeeded();
            sourceAdmin.applyReplicaIdentityFullIfNeeded();
            dorisAdmin.applyStartupActions();

            writer = new SyncWriter(sourceAdmin, dorisAdmin, dorisConnection, config);
            engine = buildEngine(config, writer);
            executor = Executors.newSingleThreadExecutor();
            executor.submit(engine);
            addShutdownHook(engine);
            awaitTermination(executor);
        }
        finally {
            closeQuietly(engine);
            closeQuietly(writer);
            closeQuietly(sourceConnection);
            closeQuietly(dorisConnection);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private static DebeziumEngine<ChangeEvent<String, String>> buildEngine(SyncConfig config, SyncWriter writer) {
        Properties props = new Properties();
        props.setProperty("name", config.connectorName);
        props.setProperty("connector.class", PostgresConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        props.setProperty("offset.storage.file.filename", config.offsetFile);
        props.setProperty("offset.flush.interval.ms", String.valueOf(config.offsetFlushMs));
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", config.historyFile);

        props.setProperty("plugin.name", config.kbPlugin);
        props.setProperty("database.hostname", config.kbHost);
        props.setProperty("database.port", config.kbPort);
        props.setProperty("database.user", config.kbUser);
        props.setProperty("database.password", config.kbPassword);
        props.setProperty("database.server.id", config.kbServerId);
        props.setProperty("database.server.name", config.kbServerName);
        props.setProperty("database.dbname", config.kbDb);
        props.setProperty("snapshot.mode", config.kbSnapshotMode);
        props.setProperty("slot.name", config.kbSlotName);
        props.setProperty("slot.drop.on.stop", String.valueOf(config.kbSlotDropOnStop));
        props.setProperty("tombstones.on.delete", "false");
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("binary.handling.mode", "base64");
        props.setProperty("include.schema.changes", "false");

        if (!config.tableIncludeListRaw.isEmpty()) {
            props.setProperty("table.include.list", config.tableIncludeListRaw);
        }
        if (!config.schemaIncludeListRaw.isEmpty()) {
            props.setProperty("schema.include.list", config.schemaIncludeListRaw);
        }

        return DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(writer::handle)
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        System.err.println("[sync] Debezium exited with error: " + message);
                        error.printStackTrace(System.err);
                    }
                })
                .build();
    }

    private static void ensureWorkDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Failed to create work dir: " + dirPath);
        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeQuietly(engine)));
    }

    private static void awaitTermination(ExecutorService executor) throws InterruptedException {
        int loops = 0;
        executor.shutdown();
        while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            loops++;
            if (loops % 6 == 0) {
                System.out.println("[sync] running... waiting for CDC events");
            }
        }
    }

    private static void closeQuietly(Object closable) {
        try {
            if (closable instanceof DebeziumEngine) {
                ((DebeziumEngine<?>) closable).close();
            }
            else if (closable instanceof Closeable) {
                ((Closeable) closable).close();
            }
            else if (closable instanceof Connection) {
                ((Connection) closable).close();
            }
        }
        catch (Exception ignored) {
        }
    }

    private static final class SyncWriter implements Closeable {

        private final SourceAdmin sourceAdmin;
        private final DorisAdmin dorisAdmin;
        private final Connection dorisConnection;
        private final SyncConfig config;
        private final Map<SourceTableId, TableRuntime> tableCache = new ConcurrentHashMap<SourceTableId, TableRuntime>();

        private SyncWriter(SourceAdmin sourceAdmin, DorisAdmin dorisAdmin, Connection dorisConnection, SyncConfig config) {
            this.sourceAdmin = sourceAdmin;
            this.dorisAdmin = dorisAdmin;
            this.dorisConnection = dorisConnection;
            this.config = config;
        }

        private void handle(ChangeEvent<String, String> event) {
            CdcEvent cdcEvent;
            try {
                cdcEvent = CdcEvent.parse(event);
            }
            catch (Exception e) {
                System.err.println("[sync] failed to parse CDC event, skip. destination=" + event.destination());
                e.printStackTrace(System.err);
                return;
            }
            if (cdcEvent == null || cdcEvent.tableId == null) {
                return;
            }

            String op = cdcEvent.op;
            if (op == null || op.isEmpty()) {
                return;
            }

            try {
                TableRuntime runtime = tableCache.get(cdcEvent.tableId);
                if (runtime == null) {
                    runtime = initRuntime(cdcEvent.tableId);
                    tableCache.put(cdcEvent.tableId, runtime);
                }

                if ("c".equals(op) || "u".equals(op) || "r".equals(op)) {
                    upsert(runtime, cdcEvent.after != null ? cdcEvent.after : cdcEvent.before);
                }
                else if ("d".equals(op)) {
                    delete(runtime, cdcEvent);
                }
            }
            catch (Exception e) {
                System.err.println("[sync] write failed for " + cdcEvent.tableId + ", op=" + op);
                e.printStackTrace(System.err);
            }
        }

        private TableRuntime initRuntime(SourceTableId tableId) throws SQLException {
            SourceTableMeta sourceTableMeta = sourceAdmin.loadTableMeta(tableId);
            TargetTable targetTable = config.route(tableId);
            dorisAdmin.ensureTargetTable(targetTable, sourceTableMeta);
            String upsertSql = buildUpsertSql(targetTable, sourceTableMeta.columns);
            String deleteSql = buildDeleteSql(targetTable, sourceTableMeta.primaryKeys);

            System.out.println("[sync] table route prepared: " + tableId + " -> " + targetTable.database + "." + targetTable.table);
            return new TableRuntime(sourceTableMeta, targetTable, upsertSql, deleteSql);
        }

        private void upsert(TableRuntime runtime, JsonNode row) throws SQLException {
            if (row == null || row.isNull()) {
                return;
            }
            PreparedStatement ps = dorisConnection.prepareStatement(runtime.upsertSql);
            try {
                int i = 1;
                for (SourceColumn col : runtime.meta.columns) {
                    bind(ps, i++, row.get(col.name), col);
                }
                ps.executeUpdate();
            }
            finally {
                ps.close();
            }
        }

        private void delete(TableRuntime runtime, CdcEvent event) throws SQLException {
            if (runtime.deleteSql == null || runtime.deleteSql.isEmpty()) {
                if (config.skipDeleteWithoutPk) {
                    return;
                }
                throw new IllegalStateException("Delete event cannot be applied because PK is missing: " + runtime.target);
            }

            PreparedStatement ps = dorisConnection.prepareStatement(runtime.deleteSql);
            try {
                int i = 1;
                for (String pk : runtime.meta.primaryKeys) {
                    JsonNode keyNode = event.key != null ? event.key.get(pk) : null;
                    if ((keyNode == null || keyNode.isNull()) && event.before != null) {
                        keyNode = event.before.get(pk);
                    }
                    if (keyNode == null || keyNode.isNull()) {
                        throw new IllegalStateException("Delete key missing for pk=" + pk + ", table=" + runtime.target);
                    }
                    SourceColumn column = runtime.meta.columnMap.get(pk);
                    bind(ps, i++, keyNode, column);
                }
                ps.executeUpdate();
            }
            finally {
                ps.close();
            }
        }

        private static String buildUpsertSql(TargetTable target, List<SourceColumn> columns) {
            StringJoiner colJoiner = new StringJoiner(", ");
            StringJoiner valJoiner = new StringJoiner(", ");
            for (SourceColumn c : columns) {
                colJoiner.add(backtick(c.name));
                valJoiner.add("?");
            }
            return "INSERT INTO " + target.qualified() + " (" + colJoiner.toString() + ") VALUES (" + valJoiner.toString() + ")";
        }

        private static String buildDeleteSql(TargetTable target, List<String> primaryKeys) {
            if (primaryKeys == null || primaryKeys.isEmpty()) {
                return "";
            }
            StringJoiner where = new StringJoiner(" AND ");
            for (String pk : primaryKeys) {
                where.add(backtick(pk) + " = ?");
            }
            return "DELETE FROM " + target.qualified() + " WHERE " + where.toString();
        }

        private static void bind(PreparedStatement ps, int idx, JsonNode node, SourceColumn column) throws SQLException {
            if (node == null || node.isNull()) {
                ps.setNull(idx, Types.NULL);
                return;
            }

            String dorisType = column.dorisType;
            if (dorisType.startsWith("BIGINT") || dorisType.startsWith("INT") || dorisType.startsWith("SMALLINT") || dorisType.startsWith("TINYINT")) {
                if (node.isNumber()) {
                    ps.setLong(idx, node.longValue());
                }
                else {
                    ps.setLong(idx, Long.parseLong(node.asText()));
                }
                return;
            }
            if (dorisType.startsWith("DOUBLE") || dorisType.startsWith("FLOAT")) {
                if (node.isNumber()) {
                    ps.setDouble(idx, node.doubleValue());
                }
                else {
                    ps.setDouble(idx, Double.parseDouble(node.asText()));
                }
                return;
            }
            if (dorisType.startsWith("DECIMAL")) {
                ps.setBigDecimal(idx, new BigDecimal(node.asText()));
                return;
            }
            if (dorisType.startsWith("BOOLEAN")) {
                if (node.isBoolean()) {
                    ps.setBoolean(idx, node.booleanValue());
                }
                else {
                    ps.setBoolean(idx, Boolean.parseBoolean(node.asText()));
                }
                return;
            }
            if (node.isContainerNode()) {
                ps.setString(idx, node.toString());
                return;
            }
            ps.setString(idx, node.asText());
        }

        @Override
        public void close() {
        }
    }

    private static final class SourceAdmin {

        private final Connection connection;
        private final SyncConfig config;
        private final Map<SourceTableId, SourceTableMeta> metaCache = new ConcurrentHashMap<SourceTableId, SourceTableMeta>();

        private SourceAdmin(Connection connection, SyncConfig config) {
            this.connection = connection;
            this.config = config;
        }

        private void initSlotIfNeeded() throws SQLException {
            if (!config.kbSlotInit) {
                return;
            }

            boolean exists = slotExists(config.kbSlotName);
            if (exists && config.kbSlotRecreate) {
                execSingleValue("SELECT sys_drop_replication_slot(?)", config.kbSlotName);
                exists = false;
                System.out.println("[source] dropped slot: " + config.kbSlotName);
            }

            if (!exists) {
                execSingleValue("SELECT * FROM sys_create_logical_replication_slot(?, ?)", config.kbSlotName, config.kbPlugin);
                System.out.println("[source] created slot: " + config.kbSlotName + ", plugin=" + config.kbPlugin);
            }
            else {
                System.out.println("[source] slot already exists: " + config.kbSlotName);
            }
        }

        private void applyReplicaIdentityFullIfNeeded() throws SQLException {
            if (!config.kbReplicaIdentityFull) {
                return;
            }
            List<SourceTableId> tables = resolveReplicaIdentityTargets();
            if (tables.isEmpty()) {
                System.out.println("[source] replica identity full enabled but no table resolved");
                return;
            }
            for (SourceTableId tableId : tables) {
                String sql = "ALTER TABLE " + tableId.doubleQuoted() + " REPLICA IDENTITY FULL";
                try (Statement st = connection.createStatement()) {
                    st.execute(sql);
                    System.out.println("[source] set REPLICA IDENTITY FULL: " + tableId);
                }
                catch (SQLException e) {
                    if (config.kbReplicaIdentityFullFailFast) {
                        throw e;
                    }
                    System.err.println("[source] failed setting REPLICA IDENTITY FULL: " + tableId + ", " + e.getMessage());
                }
            }
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

        private SourceTableMeta loadTableMeta(SourceTableId tableId) throws SQLException {
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
                ps.setString(1, tableId.schema);
                ps.setString(2, tableId.table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("column_name");
                        String dataType = lower(rs.getString("data_type"));
                        String udtName = lower(rs.getString("udt_name"));
                        Integer precision = toInteger(rs, "numeric_precision");
                        Integer scale = toInteger(rs, "numeric_scale");
                        Integer length = toInteger(rs, "character_maximum_length");
                        boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
                        String dorisType = mapToDorisType(dataType, udtName, precision, scale, length);
                        columns.add(new SourceColumn(name, dataType, udtName, dorisType, nullable));
                    }
                }
            }

            if (columns.isEmpty()) {
                throw new IllegalStateException("No columns found for source table: " + tableId);
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
                ps.setString(1, tableId.schema);
                ps.setString(2, tableId.table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        primaryKeys.add(rs.getString("column_name"));
                    }
                }
            }

            SourceTableMeta meta = new SourceTableMeta(tableId, columns, primaryKeys);
            metaCache.put(tableId, meta);
            return meta;
        }

        private List<SourceTableId> listTablesBySchemas(List<String> schemas) throws SQLException {
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

    private static final class DorisAdmin {

        private final Connection dorisConnection;
        private final SyncConfig config;
        private final SourceAdmin sourceAdmin;

        private DorisAdmin(Connection dorisConnection, SyncConfig config, SourceAdmin sourceAdmin) {
            this.dorisConnection = dorisConnection;
            this.config = config;
            this.sourceAdmin = sourceAdmin;
        }

        private void applyStartupActions() throws SQLException {
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

            for (SourceTableId table : dropTargets) {
                TargetTable target = config.route(table);
                ensureDatabase(target.database);
                exec("DROP TABLE IF EXISTS " + target.qualified());
                System.out.println("[doris] dropped table: " + target);
            }

            for (SourceTableId table : truncateTargets) {
                TargetTable target = config.route(table);
                ensureDatabase(target.database);
                if (!tableExists(target)) {
                    continue;
                }
                exec("TRUNCATE TABLE " + target.qualified());
                System.out.println("[doris] truncated table: " + target);
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

        private void ensureTargetTable(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
            if (config.dorisAutoCreateDatabase) {
                ensureDatabase(target.database);
            }

            boolean exists = tableExists(target);
            if (!exists) {
                if (!config.dorisAutoCreateTable) {
                    throw new IllegalStateException("Target table missing and auto-create disabled: " + target);
                }
                createTable(target, sourceMeta);
                return;
            }

            if (config.dorisAutoAddColumns) {
                addMissingColumns(target, sourceMeta);
            }
        }

        private void createTable(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
            StringJoiner columnsSql = new StringJoiner(",\n  ");
            for (SourceColumn c : sourceMeta.columns) {
                columnsSql.add(backtick(c.name) + " " + c.dorisType + (c.nullable ? " NULL" : " NOT NULL"));
            }

            String distCol = sourceMeta.primaryKeys.isEmpty()
                    ? sourceMeta.columns.get(0).name
                    : sourceMeta.primaryKeys.get(0);
            String keyClause;
            String properties;
            if (sourceMeta.primaryKeys.isEmpty()) {
                keyClause = "DUPLICATE KEY(" + backtick(distCol) + ")";
                properties = "\"replication_num\"=\"" + config.dorisReplicationNum + "\"";
            }
            else {
                String pkCols = sourceMeta.primaryKeys.stream().map(KingbaseToDorisSyncApp::backtick).collect(Collectors.joining(", "));
                keyClause = "UNIQUE KEY(" + pkCols + ")";
                properties = "\"replication_num\"=\"" + config.dorisReplicationNum + "\", " +
                        "\"enable_unique_key_merge_on_write\"=\"true\"";
            }

            String sql = "CREATE TABLE IF NOT EXISTS " + target.qualified() + " (\n  " +
                    columnsSql.toString() + "\n)\n" +
                    keyClause + "\n" +
                    "DISTRIBUTED BY HASH(" + backtick(distCol) + ") BUCKETS " + config.dorisBuckets + "\n" +
                    "PROPERTIES(" + properties + ")";

            exec(sql);
            System.out.println("[doris] created table: " + target);
        }

        private void addMissingColumns(TargetTable target, SourceTableMeta sourceMeta) throws SQLException {
            Set<String> existing = listTargetColumns(target);
            for (SourceColumn column : sourceMeta.columns) {
                if (existing.contains(lower(column.name))) {
                    continue;
                }
                String sql = "ALTER TABLE " + target.qualified() + " ADD COLUMN " +
                        backtick(column.name) + " " + column.dorisType + (column.nullable ? " NULL" : " NOT NULL");
                exec(sql);
                System.out.println("[doris] added column: " + target + "." + column.name);
            }
        }

        private Set<String> listTargetColumns(TargetTable target) throws SQLException {
            String sql = "SHOW COLUMNS FROM " + target.qualified();
            Set<String> cols = new LinkedHashSet<String>();
            try (Statement st = dorisConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    cols.add(lower(rs.getString(1)));
                }
            }
            return cols;
        }

        private boolean tableExists(TargetTable target) throws SQLException {
            String db = target.database;
            String table = target.table.replace("'", "''");
            String sql = "SHOW TABLES FROM " + backtick(db) + " LIKE '" + table + "'";
            try (Statement st = dorisConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                return rs.next();
            }
        }

        private void ensureDatabase(String database) throws SQLException {
            if (!config.dorisAutoCreateDatabase) {
                return;
            }
            exec("CREATE DATABASE IF NOT EXISTS " + backtick(database));
        }

        private void exec(String sql) throws SQLException {
            try (Statement st = dorisConnection.createStatement()) {
                st.execute(sql);
            }
        }
    }

    private static final class CdcEvent {
        private final SourceTableId tableId;
        private final String op;
        private final JsonNode key;
        private final JsonNode before;
        private final JsonNode after;

        private CdcEvent(SourceTableId tableId, String op, JsonNode key, JsonNode before, JsonNode after) {
            this.tableId = tableId;
            this.op = op;
            this.key = key;
            this.before = before;
            this.after = after;
        }

        private static CdcEvent parse(ChangeEvent<String, String> event) throws IOException {
            if (event == null || event.value() == null || event.value().trim().isEmpty()) {
                return null;
            }

            JsonNode valueRoot = MAPPER.readTree(event.value());
            JsonNode payload = payload(valueRoot);
            if (payload == null || payload.isNull()) {
                return null;
            }

            JsonNode source = payload.get("source");
            if (source == null || source.isNull()) {
                return null;
            }

            String schema = text(source, "schema");
            String table = text(source, "table");
            if (isBlank(schema) || isBlank(table)) {
                return null;
            }

            String op = text(payload, "op");
            JsonNode before = payload.get("before");
            JsonNode after = payload.get("after");

            JsonNode keyPayload = null;
            if (event.key() != null && !event.key().trim().isEmpty()) {
                JsonNode keyRoot = MAPPER.readTree(event.key());
                keyPayload = payload(keyRoot);
            }

            return new CdcEvent(new SourceTableId(schema, table), op, keyPayload, before, after);
        }

        private static JsonNode payload(JsonNode root) {
            if (root == null || root.isNull()) {
                return null;
            }
            JsonNode payload = root.get("payload");
            return payload == null ? root : payload;
        }
    }

    private static final class TableRuntime {
        private final SourceTableMeta meta;
        private final TargetTable target;
        private final String upsertSql;
        private final String deleteSql;

        private TableRuntime(SourceTableMeta meta, TargetTable target, String upsertSql, String deleteSql) {
            this.meta = meta;
            this.target = target;
            this.upsertSql = upsertSql;
            this.deleteSql = deleteSql;
        }
    }

    private static final class SourceTableMeta {
        private final SourceTableId id;
        private final List<SourceColumn> columns;
        private final List<String> primaryKeys;
        private final Map<String, SourceColumn> columnMap;

        private SourceTableMeta(SourceTableId id, List<SourceColumn> columns, List<String> primaryKeys) {
            this.id = id;
            this.columns = columns;
            this.primaryKeys = primaryKeys;
            Map<String, SourceColumn> map = new HashMap<String, SourceColumn>();
            for (SourceColumn c : columns) {
                map.put(c.name, c);
            }
            this.columnMap = map;
        }
    }

    private static final class SourceColumn {
        private final String name;
        private final String sourceDataType;
        private final String sourceUdtName;
        private final String dorisType;
        private final boolean nullable;

        private SourceColumn(String name, String sourceDataType, String sourceUdtName, String dorisType, boolean nullable) {
            this.name = name;
            this.sourceDataType = sourceDataType;
            this.sourceUdtName = sourceUdtName;
            this.dorisType = dorisType;
            this.nullable = nullable;
        }
    }

    private static final class TargetTable {
        private final String database;
        private final String table;

        private TargetTable(String database, String table) {
            this.database = database;
            this.table = table;
        }

        private String qualified() {
            return backtick(database) + "." + backtick(table);
        }

        @Override
        public String toString() {
            return database + "." + table;
        }
    }

    private static final class SourceTableId {
        private final String schema;
        private final String table;

        private SourceTableId(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        private String doubleQuoted() {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SourceTableId)) {
                return false;
            }
            SourceTableId that = (SourceTableId) o;
            return Objects.equals(schema, that.schema) && Objects.equals(table, that.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, table);
        }

        @Override
        public String toString() {
            return schema + "." + table;
        }
    }

    private enum RouteMode {
        SCHEMA_TABLE,
        SCHEMA_AS_DB
    }

    private static final class SyncConfig {

        private final String connectorName;
        private final String workDir;
        private final String offsetFile;
        private final String historyFile;
        private final long offsetFlushMs;

        private final String kbHost;
        private final String kbPort;
        private final String kbUser;
        private final String kbPassword;
        private final String kbDb;
        private final String kbServerId;
        private final String kbServerName;
        private final String kbPlugin;
        private final String kbSnapshotMode;
        private final String kbSlotName;
        private final boolean kbSlotDropOnStop;
        private final boolean kbSlotInit;
        private final boolean kbSlotRecreate;
        private final boolean kbReplicaIdentityFull;
        private final boolean kbReplicaIdentityFullFailFast;
        private final List<SourceTableId> kbReplicaIdentityFullTables;

        private final String tableIncludeListRaw;
        private final String schemaIncludeListRaw;
        private final List<SourceTableId> tableIncludeList;
        private final List<String> schemaIncludeList;

        private final String dorisHost;
        private final String dorisPort;
        private final String dorisUser;
        private final String dorisPassword;
        private final String dorisDatabase;
        private final String dorisDatabasePrefix;
        private final String dorisTablePrefix;
        private final String dorisTableSuffix;
        private final String dorisSchemaTableSeparator;
        private final RouteMode dorisRouteMode;
        private final boolean dorisAutoCreateDatabase;
        private final boolean dorisAutoCreateTable;
        private final boolean dorisAutoAddColumns;
        private final int dorisBuckets;
        private final int dorisReplicationNum;
        private final boolean skipDeleteWithoutPk;
        private final List<SourceTableId> dorisStartupDropTables;
        private final List<SourceTableId> dorisStartupTruncateTables;
        private final boolean dorisStartupDropAllIncluded;
        private final boolean dorisStartupTruncateAllIncluded;

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
                boolean dorisStartupTruncateAllIncluded
        ) {
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
        }

        private static SyncConfig load() {
            String workDir = getSetting("sync.work.dir", "SYNC_WORK_DIR", "/tmp/debezium/kingbase-doris");
            String offsetFile = getSetting("sync.offset.file", "SYNC_OFFSET_FILE", workDir + "/offset.dat");
            String historyFile = getSetting("sync.history.file", "SYNC_HISTORY_FILE", workDir + "/history.dat");

            String tableIncludeRaw = normalizeCsvList(getSetting("kb.tables", "KB_TABLES", ""));
            String schemaIncludeRaw = normalizeCsvList(getSetting("kb.schemas", "KB_SCHEMAS", ""));

            return new SyncConfig(
                    getSetting("sync.connector.name", "SYNC_CONNECTOR_NAME", "kingbase-doris-sync"),
                    workDir,
                    offsetFile,
                    historyFile,
                    parseLong(getSetting("sync.offset.flush.ms", "SYNC_OFFSET_FLUSH_MS", "10000"), 10000L),
                    getSetting("kb.host", "KB_HOST", "127.0.0.1"),
                    getSetting("kb.port", "KB_PORT", "54321"),
                    getSetting("kb.user", "KB_USER", "kingbase"),
                    getSetting("kb.password", "KB_PASSWORD", "123456"),
                    getSetting("kb.db", "KB_DB", "test"),
                    getSetting("kb.server.id", "KB_SERVER_ID", "54001"),
                    getSetting("kb.server.name", "KB_SERVER_NAME", "kingbase-server"),
                    getSetting("kb.plugin", "KB_PLUGIN_NAME", "decoderbufs"),
                    getSetting("kb.snapshot.mode", "KB_SNAPSHOT_MODE", "initial"),
                    getSetting("kb.slot.name", "KB_SLOT_NAME", "dbz_kingbase_slot"),
                    parseBoolean(getSetting("kb.slot.drop.on.stop", "KB_SLOT_DROP_ON_STOP", "false")),
                    parseBoolean(getSetting("kb.slot.init", "KB_SLOT_INIT", "true")),
                    parseBoolean(getSetting("kb.slot.recreate", "KB_SLOT_RECREATE", "false")),
                    parseBoolean(getSetting("kb.replica.identity.full", "KB_REPLICA_IDENTITY_FULL", "false")),
                    parseBoolean(getSetting("kb.replica.identity.full.fail-fast", "KB_REPLICA_IDENTITY_FULL_FAIL_FAST", "false")),
                    parseTableList(normalizeCsvList(getSetting("kb.replica.identity.full.tables", "KB_REPLICA_IDENTITY_FULL_TABLES", ""))),
                    tableIncludeRaw,
                    schemaIncludeRaw,
                    parseTableList(tableIncludeRaw),
                    parseStringList(schemaIncludeRaw),
                    getSetting("doris.host", "DORIS_HOST", "127.0.0.1"),
                    getSetting("doris.port", "DORIS_PORT", "9030"),
                    getSetting("doris.user", "DORIS_USER", "root"),
                    getSetting("doris.password", "DORIS_PASSWORD", ""),
                    getSetting("doris.database", "DORIS_DATABASE", "cdc"),
                    getSetting("doris.database.prefix", "DORIS_DATABASE_PREFIX", "cdc_"),
                    getSetting("doris.table.prefix", "DORIS_TABLE_PREFIX", ""),
                    getSetting("doris.table.suffix", "DORIS_TABLE_SUFFIX", ""),
                    getSetting("doris.schema.table.separator", "DORIS_SCHEMA_TABLE_SEPARATOR", "__"),
                    parseRouteMode(getSetting("doris.route.mode", "DORIS_ROUTE_MODE", "schema_table")),
                    parseBoolean(getSetting("doris.auto.create.database", "DORIS_AUTO_CREATE_DATABASE", "true")),
                    parseBoolean(getSetting("doris.auto.create.table", "DORIS_AUTO_CREATE_TABLE", "true")),
                    parseBoolean(getSetting("doris.auto.add.columns", "DORIS_AUTO_ADD_COLUMNS", "true")),
                    parseInt(getSetting("doris.buckets", "DORIS_BUCKETS", "10"), 10),
                    parseInt(getSetting("doris.replication.num", "DORIS_REPLICATION_NUM", "1"), 1),
                    parseBoolean(getSetting("doris.skip.delete.without.pk", "DORIS_SKIP_DELETE_WITHOUT_PK", "true")),
                    parseTableList(normalizeCsvList(getSetting("doris.startup.drop.tables", "DORIS_STARTUP_DROP_TABLES", ""))),
                    parseTableList(normalizeCsvList(getSetting("doris.startup.truncate.tables", "DORIS_STARTUP_TRUNCATE_TABLES", ""))),
                    parseBoolean(getSetting("doris.startup.drop.all.included", "DORIS_STARTUP_DROP_ALL_INCLUDED", "false")),
                    parseBoolean(getSetting("doris.startup.truncate.all.included", "DORIS_STARTUP_TRUNCATE_ALL_INCLUDED", "false"))
            );
        }

        private String sourceJdbcUrl() {
            return "jdbc:kingbase8://" + kbHost + ":" + kbPort + "/" + kbDb;
        }

        private String dorisJdbcUrl() {
            return "jdbc:mysql://" + dorisHost + ":" + dorisPort +
                    "/?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true";
        }

        private TargetTable route(SourceTableId sourceTableId) {
            if (dorisRouteMode == RouteMode.SCHEMA_AS_DB) {
                String db = sanitizeName(dorisDatabasePrefix + sourceTableId.schema);
                String table = sanitizeName(dorisTablePrefix + sourceTableId.table + dorisTableSuffix);
                return new TargetTable(db, table);
            }
            String db = sanitizeName(dorisDatabase);
            String tableName = sourceTableId.schema + dorisSchemaTableSeparator + sourceTableId.table;
            tableName = dorisTablePrefix + tableName + dorisTableSuffix;
            return new TargetTable(db, sanitizeName(tableName));
        }

        private void printSummary() {
            System.out.println("[sync] ===== Kingbase -> Doris Sync =====");
            System.out.println("[sync] source=" + kbHost + ":" + kbPort + "/" + kbDb + ", slot=" + kbSlotName + ", snapshot.mode=" + kbSnapshotMode);
            System.out.println("[sync] source tables=" + (tableIncludeListRaw.isEmpty() ? "<all>" : tableIncludeListRaw));
            System.out.println("[sync] source schemas=" + (schemaIncludeListRaw.isEmpty() ? "<all>" : schemaIncludeListRaw));
            System.out.println("[sync] doris=" + dorisHost + ":" + dorisPort + ", route.mode=" + dorisRouteMode.name().toLowerCase(Locale.ROOT));
            System.out.println("[sync] doris auto create db/table=" + dorisAutoCreateDatabase + "/" + dorisAutoCreateTable + ", auto add columns=" + dorisAutoAddColumns);
            System.out.println("[sync] startup drop tables=" + joinTables(dorisStartupDropTables) + ", truncate tables=" + joinTables(dorisStartupTruncateTables));
            System.out.println("[sync] startup drop all included=" + dorisStartupDropAllIncluded + ", truncate all included=" + dorisStartupTruncateAllIncluded);
        }
    }

    private static String joinTables(List<SourceTableId> tables) {
        if (tables == null || tables.isEmpty()) {
            return "<none>";
        }
        return tables.stream().map(SourceTableId::toString).collect(Collectors.joining(","));
    }

    private static String text(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        return v.asText();
    }

    private static Integer toInteger(ResultSet rs, String field) throws SQLException {
        int v = rs.getInt(field);
        return rs.wasNull() ? null : v;
    }

    private static String mapToDorisType(String dataType, String udtName, Integer precision, Integer scale, Integer length) {
        String t = !isBlank(dataType) ? dataType : udtName;
        t = lower(t);

        if (containsAny(t, "int2", "smallint", "smallserial")) {
            return "SMALLINT";
        }
        if (containsAny(t, "int8", "bigint", "bigserial")) {
            return "BIGINT";
        }
        if (containsAny(t, "int4", "integer", "serial") || "int".equals(t)) {
            return "INT";
        }
        if (containsAny(t, "float4", "real")) {
            return "FLOAT";
        }
        if (containsAny(t, "float8", "double")) {
            return "DOUBLE";
        }
        if (containsAny(t, "numeric", "decimal", "money")) {
            int s = scale == null || scale < 0 ? 4 : scale;
            return "DECIMAL(38," + Math.min(s, 18) + ")";
        }
        if (containsAny(t, "bool")) {
            return "BOOLEAN";
        }
        if (containsAny(t, "date")) {
            return "DATE";
        }
        if (containsAny(t, "timestamp", "time")) {
            return "DATETIME";
        }
        if (containsAny(t, "char", "text", "json", "uuid", "xml", "inet", "cidr", "macaddr")) {
            return "STRING";
        }
        if (containsAny(t, "bytea", "blob", "binary")) {
            return "STRING";
        }
        if (length != null && length > 0 && length <= 65533) {
            return "VARCHAR(" + length + ")";
        }
        return "STRING";
    }

    private static boolean containsAny(String text, String... values) {
        if (text == null) {
            return false;
        }
        for (String v : values) {
            if (text.contains(v)) {
                return true;
            }
        }
        return false;
    }

    private static RouteMode parseRouteMode(String value) {
        String v = lower(value);
        if ("schema_as_db".equals(v)) {
            return RouteMode.SCHEMA_AS_DB;
        }
        return RouteMode.SCHEMA_TABLE;
    }

    private static String backtick(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String lower(String text) {
        return text == null ? "" : text.toLowerCase(Locale.ROOT);
    }

    private static String sanitizeName(String raw) {
        if (raw == null || raw.isEmpty()) {
            return raw;
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(Character.toLowerCase(c));
            }
            else {
                sb.append('_');
            }
        }
        String result = sb.toString();
        if (result.isEmpty()) {
            return "t";
        }
        if (Character.isDigit(result.charAt(0))) {
            return "t_" + result;
        }
        return result;
    }

    private static String getSetting(String sysKey, String envKey, String defaultValue) {
        String sysValue = System.getProperty(sysKey);
        if (!isBlank(sysValue)) {
            return sysValue.trim();
        }
        String envValue = System.getenv(envKey);
        if (!isBlank(envValue)) {
            return envValue.trim();
        }
        return defaultValue;
    }

    private static boolean parseBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        }
        catch (Exception ignored) {
            return defaultValue;
        }
    }

    private static long parseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(value);
        }
        catch (Exception ignored) {
            return defaultValue;
        }
    }

    private static String normalizeCsvList(String raw) {
        if (raw == null) {
            return "";
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));
    }

    private static List<String> parseStringList(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }
        List<String> list = new ArrayList<String>();
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (!v.isEmpty()) {
                list.add(v);
            }
        }
        return list;
    }

    private static List<SourceTableId> parseTableList(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }
        Map<String, SourceTableId> unique = new LinkedHashMap<String, SourceTableId>();
        String[] parts = csv.split(",");
        for (String p : parts) {
            String text = p.trim();
            if (text.isEmpty()) {
                continue;
            }
            String[] seg = text.split("\\.");
            if (seg.length != 2) {
                System.err.println("[sync] ignore non schema.table item: " + text);
                continue;
            }
            SourceTableId id = new SourceTableId(seg[0], seg[1]);
            unique.put(id.toString(), id);
        }
        return new ArrayList<SourceTableId>(unique.values());
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
