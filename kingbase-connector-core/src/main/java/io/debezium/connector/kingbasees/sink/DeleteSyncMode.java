package io.debezium.connector.kingbasees.sink;

/**
 * 删除同步策略：
 * 1) PHYSICAL_DELETE：物理删除（DELETE SQL）
 * 2) LOGICAL_DELETE_SIGN：逻辑删除（写入删除标记列）
 */
enum DeleteSyncMode {
    PHYSICAL_DELETE("physical_delete"),
    LOGICAL_DELETE_SIGN("logical_delete_sign");

    private final String code;

    DeleteSyncMode(String code) {
        this.code = code;
    }

    static DeleteSyncMode fromCode(String text) {
        String value = SinkSupport.lower(text);
        if ("logical_delete_sign".equals(value) || "logical_delete".equals(value) || "logical".equals(value)) {
            return LOGICAL_DELETE_SIGN;
        }
        return PHYSICAL_DELETE;
    }

    String getCode() {
        return code;
    }
}
