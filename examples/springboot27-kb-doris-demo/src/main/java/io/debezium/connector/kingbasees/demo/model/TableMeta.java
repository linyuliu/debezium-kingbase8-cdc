package io.debezium.connector.kingbasees.demo.model;

import lombok.Value;

import java.util.List;

@Value
public class TableMeta {
    TableId tableId;
    List<ColumnMeta> columns;
    List<String> primaryKeys;
}

