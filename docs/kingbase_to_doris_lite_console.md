# Kingbase -> Doris 轻量控制台方案（无 Kafka/Flink）

## 目标

适用于内网、日千万级以内、希望部署简单且可恢复的场景：

1. 单服务（SpringBoot 2.7.18 + JDK8）
2. 嵌入式 Debezium 引擎（复用 `KingbaseToDorisSyncApp`）
3. Vue3 前端 + SpringBoot API 后端
4. 控制台配置持久化到嵌入式 SQLite（自动迁移旧 JSON）
3. 无 Kafka、无 Kafka Connect、无 Flink
4. 支持手动触发、定时触发、断点续跑、全量后 CDC、强制全量后 CDC
5. 工程只保留这一条主线（已移除 Kafka Connect 编排代码）

## 运行入口

1. Web 控制台主类：
- `kingbase-lite-console/src/main/java/io/debezium/connector/kingbasees/console/LiteConsoleApplication.java`

2. 同步引擎主类：
- `kingbase-connector-core/src/main/java/io/debezium/connector/kingbasees/sink/KingbaseToDorisSyncApp.java`

3. 前端工程：
- `kingbase-lite-ui/`

4. 页面地址：
- `http://127.0.0.1:8080/`

5. 启动脚本：
- `scripts/run-lite-console.sh`

## 功能说明

### 1) 数据源管理

支持 Kingbase / Doris 数据源增删改查：

1. 参数校验（host/port/用户名等）
2. 连接测试（`SELECT 1`）
3. Slot 检测（Kingbase）

### 2) 同步任务管理

每个任务绑定 1 个源库 + 1 个目标库，并支持：

1. includeTables 或 includeSchemas
2. slot/plugin/serverName/serverId
3. Doris 路由方式（`schema_table` 或 `schema_as_db`）
4. 自动建库建表、自动补列

### 3) 运行模式

1. `RESUME_CDC`：断点续跑（不清 offset/history）
2. `FULL_THEN_CDC`：清 checkpoint，先全量快照再 CDC
3. `FORCE_FULL_THEN_CDC`：强制全量后 CDC，可选重建 slot
4. `CDC_ONLY`：仅增量

### 4) 定时补偿

任务支持 cron，定时模式可单独设置：

1. 定时增量（`RESUME_CDC`）
2. 定时全量补偿（`FULL_THEN_CDC`）
3. 定时强制重灌（`FORCE_FULL_THEN_CDC`）

## DDL 处理建议

当前这版以“稳”为优先：

1. 依赖 Debezium + 目标端自动补列实现常见新增列兼容
2. 复杂 DDL（改类型/删列）建议通过定时全量补偿兜底
3. 对关键表启用 `REPLICA IDENTITY FULL`，减少 delete/update 键缺失问题

## 数据可靠性建议

1. 开启唯一键模型（有主键表）
2. `offset/history` 文件放本地稳定目录
3. 不要频繁重建 slot；强制重灌时再选配
4. 补偿策略：每日低峰 `FULL_THEN_CDC`（按业务窗口）

## 快速启动

```bash
mvn -q -pl kingbase-lite-console -am -DskipTests package
java -jar kingbase-lite-console/target/kingbase-lite-console-1.0.0.jar
```

如需跳过前端构建：

```bash
mvn -q -pl kingbase-lite-console -am -DskipTests -Dskip.frontend=true package
```

如需自定义数据目录：

```bash
java -Dlite.console.data-dir=./.lite-console \
  -jar kingbase-lite-console/target/kingbase-lite-console-1.0.0.jar
```
