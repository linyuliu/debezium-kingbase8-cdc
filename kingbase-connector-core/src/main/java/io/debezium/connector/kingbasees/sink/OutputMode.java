package io.debezium.connector.kingbasees.sink;

/**
 * 同步输出模式：
 * 1) JDBC_DML：直接执行 Doris JDBC DML（当前默认）
 * 2) ENHANCED_JSON_BATCH：输出增强 JSON 批次（便于对接 HTTP/Webhook）
 * 3) JDBC_DML_AND_ENHANCED_JSON_BATCH：两者同时启用
 */
enum OutputMode {
    JDBC_DML("jdbc_dml"),
    ENHANCED_JSON_BATCH("enhanced_json_batch"),
    JDBC_DML_AND_ENHANCED_JSON_BATCH("jdbc_dml_and_enhanced_json_batch");

    private final String code;

    OutputMode(String code) {
        this.code = code;
    }

    boolean hasJdbcOutput() {
        return this == JDBC_DML || this == JDBC_DML_AND_ENHANCED_JSON_BATCH;
    }

    boolean hasEnhancedJsonOutput() {
        return this == ENHANCED_JSON_BATCH || this == JDBC_DML_AND_ENHANCED_JSON_BATCH;
    }

    static OutputMode fromCode(String text) {
        String value = SinkSupport.lower(text);
        if ("enhanced_json_batch".equals(value) || "enhanced_json".equals(value) || "json".equals(value)) {
            return ENHANCED_JSON_BATCH;
        }
        if ("jdbc_dml_and_enhanced_json_batch".equals(value) || "both".equals(value) || "jdbc_and_json".equals(value)) {
            return JDBC_DML_AND_ENHANCED_JSON_BATCH;
        }
        return JDBC_DML;
    }

    String getCode() {
        return code;
    }
}
