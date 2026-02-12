package io.debezium.connector.kingbasees.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个源表结构信息：列定义、主键信息、列索引。
 */
final class SourceTableMeta {

    private final SourceTableId id;
    private final List<SourceColumn> columns;
    private final List<String> primaryKeys;
    private final Map<String, SourceColumn> columnMap;

    SourceTableMeta(SourceTableId id, List<SourceColumn> columns, List<String> primaryKeys) {
        this.id = id;
        this.columns = columns;
        this.primaryKeys = primaryKeys;

        Map<String, SourceColumn> map = new HashMap<String, SourceColumn>();
        for (SourceColumn column : columns) {
            map.put(column.getName(), column);
        }
        this.columnMap = map;
    }

    SourceTableId getId() {
        return id;
    }

    List<SourceColumn> getColumns() {
        return columns;
    }

    List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    Map<String, SourceColumn> getColumnMap() {
        return columnMap;
    }
}
