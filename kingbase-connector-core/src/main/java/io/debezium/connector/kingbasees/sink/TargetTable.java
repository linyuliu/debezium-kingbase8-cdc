package io.debezium.connector.kingbasees.sink;

/**
 * 目标端 Doris 表标识。
 */
final class TargetTable {

    private final String database;
    private final String table;

    TargetTable(String database, String table) {
        this.database = database;
        this.table = table;
    }

    String getDatabase() {
        return database;
    }

    String getTable() {
        return table;
    }

    String qualifiedName() {
        return SinkSupport.backtick(database) + "." + SinkSupport.backtick(table);
    }

    @Override
    public String toString() {
        return database + "." + table;
    }
}
