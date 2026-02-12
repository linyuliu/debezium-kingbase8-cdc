package io.debezium.connector.kingbasees.sink;

/**
 * 单表运行时上下文：目标表、结构、预编译 SQL。
 */
final class TableRuntime {

    private final SourceTableMeta sourceMeta;
    private final TargetTable targetTable;
    private final String upsertSql;
    private final String deleteSql;
    private final boolean logicalDeleteEnabled;

    TableRuntime(SourceTableMeta sourceMeta,
                 TargetTable targetTable,
                 String upsertSql,
                 String deleteSql,
                 boolean logicalDeleteEnabled) {
        this.sourceMeta = sourceMeta;
        this.targetTable = targetTable;
        this.upsertSql = upsertSql;
        this.deleteSql = deleteSql;
        this.logicalDeleteEnabled = logicalDeleteEnabled;
    }

    SourceTableMeta getSourceMeta() {
        return sourceMeta;
    }

    TargetTable getTargetTable() {
        return targetTable;
    }

    String getUpsertSql() {
        return upsertSql;
    }

    String getDeleteSql() {
        return deleteSql;
    }

    boolean isLogicalDeleteEnabled() {
        return logicalDeleteEnabled;
    }
}
