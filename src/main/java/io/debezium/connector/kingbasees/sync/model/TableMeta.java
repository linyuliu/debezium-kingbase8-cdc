package io.debezium.connector.kingbasees.sync.model;

import lombok.Value;

import java.util.List;

@Value
public class TableMeta {
    TableId tableId;
    List<ColumnMeta> columns;
    List<String> primaryKeys;
}

