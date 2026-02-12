package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CDC 写入执行器：
 * 1) 解析 CDC 事件（含 tombstone）
 * 2) 事件增强转换（op/before/after/changed_fields/deltas）
 * 3) 按配置执行 JDBC DML 与/或增强 JSON 批量输出
 */
final class SyncWriter implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncWriter.class);

    private final SourceAdmin sourceAdmin;
    private final DorisAdmin dorisAdmin;
    private final Connection dorisConnection;
    private final SyncConfig config;
    private final DebeziumRecordTransformer transformer;
    private final EnhancedJsonBatchEmitter enhancedEmitter;

    private final Map<SourceTableId, TableRuntime> tableCache = new ConcurrentHashMap<SourceTableId, TableRuntime>();

    SyncWriter(SourceAdmin sourceAdmin, DorisAdmin dorisAdmin, Connection dorisConnection, SyncConfig config) {
        this.sourceAdmin = sourceAdmin;
        this.dorisAdmin = dorisAdmin;
        this.dorisConnection = dorisConnection;
        this.config = config;
        this.transformer = new DebeziumRecordTransformer(config.deltaNullStrategy, config.includeChangedFields, config.includeDeltas);
        this.enhancedEmitter = config.outputMode.hasEnhancedJsonOutput() ? new EnhancedJsonBatchEmitter(config) : null;
    }

    void handle(ChangeEvent<String, String> event) {
        CdcEvent rawEvent;
        try {
            rawEvent = CdcEvent.parse(event);
        }
        catch (Exception e) {
            LOGGER.error("[同步写入] CDC 事件解析失败，已跳过。destination={}", event == null ? null : event.destination(), e);
            return;
        }

        if (rawEvent == null) {
            return;
        }
        if (rawEvent.getTableId() == null) {
            LOGGER.warn("[同步写入] 无法解析源表，已跳过。destination={}", rawEvent.getDestination());
            return;
        }

        EnhancedCdcRecord record = transformer.transform(rawEvent, config.tombstoneAsDelete);
        if (record == null) {
            return;
        }

        // 默认 tombstone 仅做日志/增强输出，不参与 JDBC DML。
        if (record.isTombstone() && !config.tombstoneAsDelete) {
            LOGGER.info("[同步写入] 收到 tombstone 事件，按配置跳过 JDBC 写入：table={}，destination={}",
                    record.getTableId(),
                    record.getDestination());
            if (enhancedEmitter != null) {
                enhancedEmitter.append(record);
            }
            return;
        }

        try {
            if (config.outputMode.hasJdbcOutput()) {
                TableRuntime runtime = tableCache.get(record.getTableId());
                if (runtime == null) {
                    runtime = initRuntime(record.getTableId());
                    tableCache.put(record.getTableId(), runtime);
                }
                applyToDoris(runtime, record);
            }

            if (enhancedEmitter != null) {
                enhancedEmitter.append(record);
            }
        }
        catch (Exception e) {
            LOGGER.error("[同步写入] 写入失败：table={}，op={}，deleted={}，tombstone={}",
                    record.getTableId(),
                    record.getOp(),
                    record.isDeleted(),
                    record.isTombstone(),
                    e);
        }
    }

    @Override
    public void close() {
        if (enhancedEmitter != null) {
            enhancedEmitter.close();
        }
    }

    private void applyToDoris(TableRuntime runtime, EnhancedCdcRecord record) throws SQLException {
        if (record.isDeleted()) {
            if (runtime.isLogicalDeleteEnabled()) {
                logicalDelete(runtime, record);
            }
            else {
                physicalDelete(runtime, record);
            }
            return;
        }

        upsert(runtime, record.getData(), 0);
    }

    private TableRuntime initRuntime(SourceTableId tableId) throws SQLException {
        SourceTableMeta sourceMeta = sourceAdmin.loadTableMeta(tableId);
        TargetTable targetTable = config.route(tableId);

        dorisAdmin.ensureTargetTable(targetTable, sourceMeta);

        boolean logicalDeleteEnabled = config.deleteSyncMode == DeleteSyncMode.LOGICAL_DELETE_SIGN;
        String upsertSql = buildUpsertSql(targetTable,
                sourceMeta.getColumns(),
                logicalDeleteEnabled ? config.logicalDeleteColumn : null);
        String deleteSql = buildDeleteSql(targetTable, sourceMeta.getPrimaryKeys());

        LOGGER.info("[同步写入] 已完成表路由初始化：{} -> {}，logicalDelete={}", tableId, targetTable, logicalDeleteEnabled);
        return new TableRuntime(sourceMeta, targetTable, upsertSql, deleteSql, logicalDeleteEnabled);
    }

    private void upsert(TableRuntime runtime, JSONObject row, int logicalDeleteSign) throws SQLException {
        if (row == null) {
            return;
        }

        try (PreparedStatement ps = dorisConnection.prepareStatement(runtime.getUpsertSql())) {
            int i = 1;
            for (SourceColumn column : runtime.getSourceMeta().getColumns()) {
                bind(ps, i++, row.get(column.getName()), column);
            }
            if (runtime.isLogicalDeleteEnabled()) {
                ps.setInt(i, logicalDeleteSign);
            }
            ps.executeUpdate();
        }
    }

    private void logicalDelete(TableRuntime runtime, EnhancedCdcRecord record) throws SQLException {
        JSONObject row = record.getData();
        if (row == null || row.isEmpty()) {
            LOGGER.warn("[同步写入] 逻辑删除缺少 before/after，回退物理删除：table={}", runtime.getTargetTable());
            physicalDelete(runtime, record);
            return;
        }

        if (!containsAllSourceColumns(row, runtime.getSourceMeta().getColumns())) {
            LOGGER.warn("[同步写入] 逻辑删除字段不完整，回退物理删除：table={}", runtime.getTargetTable());
            physicalDelete(runtime, record);
            return;
        }

        upsert(runtime, row, 1);
    }

    private void physicalDelete(TableRuntime runtime, EnhancedCdcRecord record) throws SQLException {
        if (SinkSupport.isBlank(runtime.getDeleteSql())) {
            if (config.skipDeleteWithoutPk) {
                LOGGER.warn("[同步写入] 目标表无主键映射，DELETE 事件已跳过：{}", runtime.getTargetTable());
                return;
            }
            throw new IllegalStateException("缺少主键，无法执行 DELETE 事件：" + runtime.getTargetTable());
        }

        try (PreparedStatement ps = dorisConnection.prepareStatement(runtime.getDeleteSql())) {
            int i = 1;
            for (String pk : runtime.getSourceMeta().getPrimaryKeys()) {
                Object keyNode = record.getKey() != null ? record.getKey().get(pk) : null;
                if (keyNode == null && record.getBefore() != null) {
                    keyNode = record.getBefore().get(pk);
                }
                if (keyNode == null) {
                    throw new IllegalStateException("DELETE 事件缺少主键值：pk=" + pk + "，table=" + runtime.getTargetTable());
                }
                SourceColumn column = runtime.getSourceMeta().getColumnMap().get(pk);
                if (column == null) {
                    ps.setObject(i++, keyNode);
                }
                else {
                    bind(ps, i++, keyNode, column);
                }
            }
            ps.executeUpdate();
        }
    }

    private static String buildUpsertSql(TargetTable target, List<SourceColumn> columns, String logicalDeleteColumn) {
        StringJoiner colJoiner = new StringJoiner(", ");
        StringJoiner valJoiner = new StringJoiner(", ");
        for (SourceColumn column : columns) {
            colJoiner.add(SinkSupport.backtick(column.getName()));
            valJoiner.add("?");
        }
        if (!SinkSupport.isBlank(logicalDeleteColumn)) {
            colJoiner.add(SinkSupport.backtick(logicalDeleteColumn));
            valJoiner.add("?");
        }
        return "INSERT INTO " + target.qualifiedName() + " (" + colJoiner.toString() + ") VALUES (" + valJoiner.toString() + ")";
    }

    private static String buildDeleteSql(TargetTable target, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return "";
        }

        StringJoiner where = new StringJoiner(" AND ");
        for (String pk : primaryKeys) {
            where.add(SinkSupport.backtick(pk) + " = ?");
        }
        return "DELETE FROM " + target.qualifiedName() + " WHERE " + where.toString();
    }

    private static void bind(PreparedStatement ps, int idx, Object value, SourceColumn column) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.NULL);
            return;
        }

        String dorisType = column.getDorisType();
        if (dorisType.startsWith("BIGINT") || dorisType.startsWith("INT") || dorisType.startsWith("SMALLINT") || dorisType.startsWith("TINYINT")) {
            if (value instanceof Number) {
                ps.setLong(idx, ((Number) value).longValue());
            }
            else {
                ps.setLong(idx, Long.parseLong(String.valueOf(value)));
            }
            return;
        }

        if (dorisType.startsWith("DOUBLE") || dorisType.startsWith("FLOAT")) {
            if (value instanceof Number) {
                ps.setDouble(idx, ((Number) value).doubleValue());
            }
            else {
                ps.setDouble(idx, Double.parseDouble(String.valueOf(value)));
            }
            return;
        }

        if (dorisType.startsWith("DECIMAL")) {
            ps.setBigDecimal(idx, new BigDecimal(String.valueOf(value)));
            return;
        }

        if (dorisType.startsWith("BOOLEAN")) {
            if (value instanceof Boolean) {
                ps.setBoolean(idx, (Boolean) value);
            }
            else {
                ps.setBoolean(idx, Boolean.parseBoolean(String.valueOf(value)));
            }
            return;
        }

        if (value instanceof JSONObject || value instanceof JSONArray || value instanceof Map || value instanceof List) {
            ps.setString(idx, JSON.toJSONString(value));
            return;
        }

        ps.setString(idx, String.valueOf(value));
    }

    private static boolean containsAllSourceColumns(JSONObject row, List<SourceColumn> sourceColumns) {
        for (SourceColumn column : sourceColumns) {
            if (!row.containsKey(column.getName())) {
                return false;
            }
        }
        return true;
    }
}
