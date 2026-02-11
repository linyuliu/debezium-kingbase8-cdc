# Kingbase -> Doris（Spring Boot 主工程方案）

本文档对应主工程内的 Spring Boot 启动入口：

- `src/main/java/io/debezium/connector/kingbasees/sync/SyncApplication.java`

核心实现类：

- 启动编排：`src/main/java/io/debezium/connector/kingbasees/sync/SyncBootstrapRunner.java`
- 源端元数据与 slot：`src/main/java/io/debezium/connector/kingbasees/sync/service/SourceMetadataService.java`
- Doris 自动建表：`src/main/java/io/debezium/connector/kingbasees/sync/service/DorisSchemaService.java`
- Kafka Connect 提交：`src/main/java/io/debezium/connector/kingbasees/sync/service/KafkaConnectService.java`

## 1. 你要准备的组件

1. Kingbase（源库）
2. Kafka（消息队列）
3. Kafka Connect（运行 Source/Sink Connector）
4. Doris（目标库）

## 2. 连接器插件

Connect Worker 必须加载：

1. `io.debezium.connector.kingbasees.PostgresConnector`
2. `org.apache.doris.kafka.connector.DorisSinkConnector`

说明：Java 工程里不需要额外引入 Doris Connector 的 Java 依赖包，因为它运行在 Kafka Connect Worker 里，不在当前进程类路径里。

## 3. 配置文件

统一在：

- `src/main/resources/application.yml`

配置前缀是 `sync.*`，关键项如下：

1. 源端
- `sync.source.include-tables` 或 `sync.source.include-schemas`
- `sync.source.snapshot-mode`
- `sync.source.init-slot`
- `sync.source.recreate-slot`
- `sync.source.apply-replica-identity-full`

2. 目标端
- `sync.doris.auto-create-tables`
- `sync.doris.drop-tables-before-sync`
- `sync.doris.route-mode` (`schema_table` / `schema_as_db`)

3. Connect
- `sync.kafka-connect.connect-url`
- `sync.source.source-connector-name`
- `sync.kafka-connect.sink-connector-name`

## 4. 启动方式

```bash
mvn -q -DskipTests compile
mvn -q -DskipTests \
  -Dspring-boot.run.main-class=io.debezium.connector.kingbasees.sync.SyncApplication \
  spring-boot:run
```

## 5. 启动阶段（日志更友好）

程序会按 5 个阶段打印：

1. 解析源表范围
2. 初始化 slot + 可选 `REPLICA IDENTITY FULL`
3. Doris 自动建库建表/补列（可先删表）
4. 可选重建 Connect 任务
5. 提交 Source/Sink 配置并输出状态

## 6. 常用重跑策略

如果数据量不大，故障后希望全量重灌 Doris：

1. `sync.doris.drop-tables-before-sync=true`
2. `sync.source.snapshot-mode=initial`
3. 重新启动应用

## 7. 关键说明

1. 这套方案已尽量减少手工建表：先读源库元数据，再自动建 Doris 表。  
2. Sink 使用 Doris 官方 Debezium ingestion：
- `converter.mode=debezium_ingestion`
- `debezium.schema.evolution=basic`
- `enable.delete=true`

