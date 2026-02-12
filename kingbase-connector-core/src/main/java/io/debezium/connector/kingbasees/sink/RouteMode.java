package io.debezium.connector.kingbasees.sink;

/**
 * 源表到 Doris 的路由模式。
 */
enum RouteMode {
    SCHEMA_TABLE,
    SCHEMA_AS_DB
}
