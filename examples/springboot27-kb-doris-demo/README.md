# Spring Boot 2.7.18 演示：Kingbase Debezium -> Doris（低手工）

这个演示工程做的是“一键启动同步链路”：

1. 扫描 Kingbase 源表元数据（多 schema、多表）  
2. 自动建 Doris 表（可选先删表，支持故障后重跑）  
3. 自动提交/更新 Kafka Connect 两个任务：  
   - Source: `io.debezium.connector.kingbasees.PostgresConnector`
   - Sink: `org.apache.doris.kafka.connector.DorisSinkConnector`

说明：
- `Doris Kafka Connector` 本身支持 Debezium ingestion 和基础 schema 演进（新增列），
- 但**初始建表**通常还是要你自己做；本演示把这一步自动化了。

## 代码结构（重构后）

- `SyncBootstrapRunner`：只负责 5 个阶段编排和友好日志输出  
- `service/SourceMetadataService`：源端表发现、slot 初始化、`REPLICA IDENTITY FULL`、元数据提取  
- `service/DorisSchemaService`：目标表路由、自动建表、自动补列  
- `service/KafkaConnectService`：Connect REST 调用、source/sink 配置组装和提交  
- `model/*`：表和列的模型对象  
- `util/DemoUtil`：CSV 解析、命名清洗、类型映射等公共工具

## 前置条件

1. 已有 Kafka + Kafka Connect 集群，并安装：
   - 你这个仓库打出来的 Kingbase Debezium connector 插件
   - Doris Kafka Connector 插件
2. Kingbase 已开启逻辑复制，`decoderbufs` 插件可用
3. Doris FE 可用（HTTP 8030、Query 9030）

## 配置

修改 `src/main/resources/application.yml`：

- `demo.source.*`：Kingbase 连接、slot、snapshot、过滤表
- `demo.doris.*`：Doris 路由和自动建表参数
- `demo.kafka-connect.*`：Connect REST 地址、sink 参数

## 运行

```bash
cd examples/springboot27-kb-doris-demo
mvn -q -DskipTests spring-boot:run
```

启动后会执行：

1. 初始化 slot（`init-slot=true`，可选重建）  
2. `REPLICA IDENTITY FULL`（可关）  
3. Doris 按配置 drop/create table  
4. PUT Source Connector 配置  
5. PUT Sink Connector 配置（`converter.mode=debezium_ingestion`，`debezium.schema.evolution=basic`）

## 关键行为

1. 多表多 schema  
- `demo.source.include-tables=form.t_order,public.user_info`
- 或 `demo.source.include-schemas=form,public`

2. 批量重跑  
- 保持 `demo.doris.drop-tables-before-sync=true`
- `demo.source.snapshot-mode=initial`
- 重启这个 demo 即可重灌 Doris

3. 路由策略  
- `schema_table`：`cdc.form__t_order`
- `schema_as_db`：`cdc_form.t_order`

## 常见问题

1. Connector 任务创建成功但没数据  
- 检查 Connect worker 是否加载了两个 connector 插件
- 检查 `table.include.list` 是否匹配实际源表
- 检查 Kingbase slot 和逻辑复制配置

2. 删除事件没生效  
- 需主键，且 sink `enable.delete=true`
- 源表建议保留 `REPLICA IDENTITY FULL`
