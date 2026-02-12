# Debezium Kingbase 多模块工程

这是一个多模块 Maven 工程，核心目标是“简单可运维”的 Kingbase -> Doris 同步：

1. 平时走 CDC
2. 出问题可手动恢复
3. 可定时做“全量 + CDC”补偿
4. 不依赖 Kafka / Kafka Connect / Flink

## 模块结构

```text
debezium-kingbase-parent (root)
├── kingbase-connector-core   # Debezium Connector 内核 + Embedded 同步引擎
├── kingbase-lite-console     # SpringBoot 控制台后端（API + 调度）
└── kingbase-lite-ui          # Vue3 前端工程（由 Maven 在构建期自动打包）
```

## 关键入口

1. 引擎：`kingbase-connector-core/src/main/java/io/debezium/connector/kingbasees/sink/KingbaseToDorisSyncApp.java`
2. 控制台：`kingbase-lite-console/src/main/java/io/debezium/connector/kingbasees/console/LiteConsoleApplication.java`
3. 页面源码：`kingbase-lite-ui/src/App.vue`
4. 页面产物：Maven 构建时自动写入 `kingbase-lite-console/src/main/resources/static/`

## 控制台能力

1. 数据源管理：增删改查、连通性测试、slot 检测
2. 任务配置：源目标、过滤规则、路由与处理策略
3. 触发模式：
- `RESUME_CDC`
- `FULL_THEN_CDC`
- `FORCE_FULL_THEN_CDC`
- `CDC_ONLY`
4. 定时补偿：cron 调度
5. 5 步向导：源目标 -> 功能配置 -> 表过滤 -> 数据处理 -> 创建确认
6. 元数据选项：后端统一输出中文枚举选项（避免前端硬编码）
7. 存储方式：嵌入式 SQLite（自动迁移旧 JSON 文件）

## 快速开始

```bash
mvn -q -pl kingbase-lite-console -am -DskipTests package
java -jar kingbase-lite-console/target/kingbase-lite-console-1.0.0.jar
```

启动后访问：`http://127.0.0.1:8080/`

## 构建命令

1. 全量打包（包含 UI + Java，跳过测试）：`mvn -q -pl kingbase-lite-console -am -DskipTests package`
2. 全量测试：`mvn -q test`
3. 跳过前端构建：`mvn -q -pl kingbase-lite-console -am -DskipTests -Dskip.frontend=true package`

## 文档

1. 轻量控制台方案：`docs/kingbase_to_doris_lite_console.md`
2. Kingbase CDC 配置：`docs/kingbase_cdc_setup.md`
3. Rich Types 测试步骤：`docs/kingbase_cdc_rich_types_steps.md`
