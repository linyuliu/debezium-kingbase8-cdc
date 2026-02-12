package io.debezium.connector.kingbasees.console.model;

import lombok.Data;

/**
 * 数据源连接测试结果。
 */
@Data
public class DataSourceTestResult {

    private boolean ok;
    private String message;
    private Long latencyMs;
    private String jdbcUrl;
}
