package io.debezium.connector.kingbasees.demo.model;

import lombok.Value;

@Value
public class ColumnMeta {
    String name;
    String dorisType;
    boolean nullable;
}

