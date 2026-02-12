package io.debezium.connector.kingbasees.sink;

/**
 * 源表列元数据及其 Doris 映射类型。
 */
final class SourceColumn {

    private final String name;
    private final String sourceDataType;
    private final String sourceUdtName;
    private final String dorisType;
    private final boolean nullable;

    SourceColumn(String name, String sourceDataType, String sourceUdtName, String dorisType, boolean nullable) {
        this.name = name;
        this.sourceDataType = sourceDataType;
        this.sourceUdtName = sourceUdtName;
        this.dorisType = dorisType;
        this.nullable = nullable;
    }

    String getName() {
        return name;
    }

    String getSourceDataType() {
        return sourceDataType;
    }

    String getSourceUdtName() {
        return sourceUdtName;
    }

    String getDorisType() {
        return dorisType;
    }

    boolean isNullable() {
        return nullable;
    }
}
